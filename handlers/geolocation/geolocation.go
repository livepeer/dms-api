package geolocation

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/json"

	"github.com/golang/glog"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/go-api-client"
)

const (
	streamSourceRetries               = 20
	streamSourceRetryInterval         = 1 * time.Second
	streamSourceMaxWrongRegionRetries = 3
	lockPullLeaseTimeout              = 1 * time.Minute
)

var errPullWrongRegion = errors.New("failed to pull stream, wrong region")
var errLockPull = errors.New("failed to lock pull")
var errRateLimit = errors.New("pull stream rate limit exceeded")
var errNoStreamSourceForActiveStream = errors.New("failed to get stream source for active stream")

type streamPullRateLimit struct {
	timeout time.Duration
	pulls   map[string]time.Time
	mu      sync.Mutex
}

func newStreamPullRateLimit(timeout time.Duration) *streamPullRateLimit {
	return &streamPullRateLimit{
		timeout: timeout,
		pulls:   make(map[string]time.Time),
	}
}

func (l *streamPullRateLimit) shouldLimit(playbackID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	lastMarked, ok := l.pulls[playbackID]
	return ok && time.Since(lastMarked) < l.timeout
}

func (l *streamPullRateLimit) mark(playbackID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pulls[playbackID] = time.Now()
}

type GeolocationHandlersCollection struct {
	Balancer            balancer.Balancer
	Config              config.Cli
	Lapi                *api.Client
	LapiCached          *mistapiconnector.ApiClientCached
	streamPullRateLimit *streamPullRateLimit
	serfMembersEndpoint string
}

func NewGeolocationHandlersCollection(balancer balancer.Balancer, config config.Cli, lapi *api.Client, serfMembersEndpoint string) *GeolocationHandlersCollection {
	return &GeolocationHandlersCollection{
		Balancer:            balancer,
		Config:              config,
		Lapi:                lapi,
		LapiCached:          mistapiconnector.NewApiClientCached(lapi),
		streamPullRateLimit: newStreamPullRateLimit(streamSourceRetryInterval),
		serfMembersEndpoint: serfMembersEndpoint,
	}
}

// this package handles geolocation for playback and origin discovery for node replication

// Redirect an incoming user to: CDN (only for /hls), closest node (geolocate)
// or another service (like mist HLS) on the current host for playback.
func (c *GeolocationHandlersCollection) RedirectHandler() httprouter.Handle {

	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		host := r.Host
		pathType, prefix, playbackID, pathTmpl := parsePlaybackID(r.URL.Path)
		redirectPrefixes := c.Config.RedirectPrefixes
		isStudioReq := false

		// `X-Latitude` and `X-Longitude` headers are populated by nginx/geoip when requests come from viewers. The `lat`
		// and `lon` queries can override these and are used by the `studio API` to trigger stream pulls from a desired loc.
		query := r.URL.Query()
		lat, lon := query.Get("lat"), query.Get("lon")
		glog.V(3).Infof("x-latitude=%s x-longitude=%s query_lat=%s query_lon=%s", r.Header.Get("X-Latitude"), r.Header.Get("X-Longitude"), lat, lon)
		if !isValidGPSCoord(lat, lon) {
			lat = r.Header.Get("X-Latitude")
			lon = r.Header.Get("X-Longitude")

			if !isValidGPSCoord(lat, lon) {
				lat, lon = "", ""
				glog.Warningf("invalid coordinates from=%s lat=%s lon=%s", r.URL.String(), lat, lon)
			}
		} else {
			// if lat/lon values were passed in as query params and are valid, then this is
			// a request from Studio API (e.g request to trigger a stream pull from desired location)
			isStudioReq = true
		}

		if c.Config.CdnRedirectPrefix != nil && (pathType == "hls" || pathType == "webrtc") {
			cdnPercentage, toBeRedirected := c.Config.CdnRedirectPlaybackPct[playbackID]
			if toBeRedirected && cdnPercentage > rand.Float64()*100 {
				if pathType == "webrtc" {
					// For webRTC streams on the `CdnRedirectPlaybackIDs` list we return `406`
					// so the player can fallback to a new HLS request. For webRTC streams not
					// on the CdnRedirectPlaybackIDs list we do regular geolocation.
					w.WriteHeader(http.StatusNotAcceptable) // 406 error
					metrics.Metrics.CDNRedirectWebRTC406.WithLabelValues(playbackID).Inc()
					glog.V(6).Infof("%s not supported for CDN-redirected %s", r.URL.Path, playbackID)
					return
				}

				bestNode, fullPlaybackID, err := c.Balancer.GetBestNode(context.Background(), redirectPrefixes, playbackID, lat, lon, prefix, isStudioReq)
				if err != nil {
					glog.Errorf("failed to find either origin or fallback server for playbackID=%s err=%s", playbackID, err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}

				newURL, _ := url.Parse(r.URL.String())
				newURL.Scheme = protocol(r)
				if c.Config.CdnRedirectPrefixCatalystSubdomain {
					newURL.Host = bestNode + "." + c.Config.CdnRedirectPrefix.Host
				} else {
					newURL.Host = c.Config.CdnRedirectPrefix.Host
				}
				newURL.Path, _ = url.JoinPath(c.Config.CdnRedirectPrefix.Path, fmt.Sprintf(pathTmpl, fullPlaybackID))
				http.Redirect(w, r, newURL.String(), http.StatusTemporaryRedirect)
				metrics.Metrics.CDNRedirectCount.WithLabelValues(playbackID).Inc()
				glog.V(6).Infof("CDN redirect host=%s from=%s to=%s", host, r.URL, newURL)
				return
			}
		}

		nodeHost := c.Config.NodeHost

		if nodeHost != "" && nodeHost != host {
			newURL, err := url.Parse(r.URL.String())
			if err != nil {
				glog.Errorf("failed to parse incoming url for redirect url=%s err=%s", r.URL.String(), err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			newURL.Scheme = protocol(r)
			newURL.Host = nodeHost
			http.Redirect(w, r, newURL.String(), http.StatusTemporaryRedirect)
			jsonRedirectInfo, _ := json.Marshal(map[string]interface{}{
				"redirect-type": "closest-node",
				"host":          host,
				"node-host":     nodeHost,
				"playbackID":    playbackID,
				"from":          r.URL.String(),
				"to":            newURL.String(),
				"lat":           lat,
				"lon":           lon,
			})
			glog.Infof(string(jsonRedirectInfo))
			return
		}

		if pathType == "" {
			glog.Warningf("Can not parse playbackID from path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		bestNode, fullPlaybackID, err := c.Balancer.GetBestNode(context.Background(), redirectPrefixes, playbackID, lat, lon, prefix, isStudioReq)

		if err != nil {
			glog.Errorf("failed to find either origin or fallback server for playbackID=%s err=%s", playbackID, err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}

		rPath := fmt.Sprintf(pathTmpl, fullPlaybackID)
		rURL := fmt.Sprintf("%s://%s%s?%s", protocol(r), bestNode, rPath, r.URL.RawQuery)
		rURL, err = c.resolveNodeURL(rURL)
		if err != nil {
			glog.Errorf("failed to resolve node URL playbackID=%s err=%s", playbackID, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var redirectType = "playback"
		if isStudioReq {
			redirectType = "ingest"
		}
		jsonRedirectInfo, _ := json.Marshal(map[string]interface{}{
			"redirectType":     redirectType,
			"playbackID":       playbackID,
			"from":             r.URL.String(),
			"dnsChosenRegion":  c.Config.OwnRegion,
			"mistChosenRegion": bestNode,
			"lat":              lat,
			"lon":              lon,
		})
		glog.Infof(string(jsonRedirectInfo))
		http.Redirect(w, r, rURL, http.StatusTemporaryRedirect)
	}
}

// Given a dtsc:// or https:// url, resolve the proper address of the node via serf tags
func (c *GeolocationHandlersCollection) resolveNodeURL(streamURL string) (string, error) {
	u, err := url.Parse(streamURL)
	if err != nil {
		return "", err
	}
	nodeName := u.Host
	protocol := u.Scheme

	member, err := c.clusterMember(map[string]string{}, "alive", nodeName)
	if err != nil {
		return "", err
	}
	addr, has := member.Tags[protocol]
	if !has {
		glog.V(7).Infof("no tag found, not tag resolving protocol=%s nodeName=%s", protocol, nodeName)
		return streamURL, nil
	}
	u2, err := url.Parse(addr)
	if err != nil {
		err = fmt.Errorf("node has unparsable tag!! nodeName=%s protocol=%s tag=%s", nodeName, protocol, addr)
		glog.Error(err)
		return "", err
	}
	u2.Path = filepath.Join(u2.Path, u.Path)
	u2.RawQuery = u.RawQuery
	return u2.String(), nil
}

func (c *GeolocationHandlersCollection) clusterMember(filter map[string]string, status, name string) (cluster.Member, error) {
	members, err := c.membersFiltered(filter, "", name)
	if err != nil {
		return cluster.Member{}, err
	}
	if len(members) < 1 {
		return cluster.Member{}, fmt.Errorf("could not find serf member name=%s", name)
	}
	if len(members) > 1 {
		glog.Errorf("found multiple serf members with the same name! this shouldn't happen! name=%s count=%d", name, len(members))
	}
	if members[0].Status != status {
		return cluster.Member{}, fmt.Errorf("found serf member name=%s but status=%s (wanted %s)", name, members[0].Status, status)
	}

	return members[0], nil
}

// RedirectConstPathHandler redirects const path into the self catalyst node if it was not yet redirected.
func (c *GeolocationHandlersCollection) RedirectConstPathHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		if r.Host != c.Config.NodeName {
			rURL := fmt.Sprintf("%s://%s%s", protocol(r), c.Config.NodeName, r.URL.Path)
			glog.V(6).Infof("generated redirect url=%s", rURL)
			http.Redirect(w, r, rURL, http.StatusTemporaryRedirect)
		}
	}
}

// respond to a STREAM_SOURCE request from Mist
func (c *GeolocationHandlersCollection) HandleStreamSource(ctx context.Context, payload *misttriggers.StreamSourcePayload) (string, error) {
	lat := c.Config.NodeLatitude
	lon := c.Config.NodeLongitude
	// if VOD source is detected, return empty response to use input URL as configured
	if strings.HasPrefix(payload.StreamName, "catalyst_vod_") || strings.HasPrefix(payload.StreamName, "tr_src_") {
		return "", nil
	}

	latStr := fmt.Sprintf("%f", lat)
	lonStr := fmt.Sprintf("%f", lon)
	var errMist error
	for i := 0; i < streamSourceRetries; i++ {
		var dtscURL string
		dtscURL, errMist = c.Balancer.MistUtilLoadSource(context.Background(), payload.StreamName, latStr, lonStr)
		if errMist == nil {
			return c.resolveReplicatedStream(dtscURL, payload.StreamName)
		}

		playbackID := playbackIdFor(payload.StreamName)
		pullURL, err := c.getStreamPull(playbackID, i)
		if err == nil || errors.Is(err, api.ErrNotExists) {
			if pullURL == "" {
				// not a stream pull, stream is not active or does not exist, usual situation when a viewer tries to play inactive stream
				glog.V(6).Infof("unable to find STREAM_SOURCE: playbackID=%s, mistErr=%v", playbackID, errMist)
				return "push://", nil
			} else {
				// start stream pull
				glog.Infof("replying to Mist STREAM_SOURCE with stream pull request=%s response=%s", payload.StreamName, pullURL)
				return pullURL, nil
			}
		}
		if errors.Is(err, errNoStreamSourceForActiveStream) {
			// stream is active, but STREAM_SOURCE cannot be found
			glog.Errorf("error querying mist for active stream STREAM_SOURCE: playbackID=%s, mistErr=%v", playbackID, errMist)
			return "push://", nil
		} else if !errors.Is(err, errLockPull) && !errors.Is(err, errPullWrongRegion) && !errors.Is(err, errRateLimit) {
			// stream pull failed for unknown reason
			glog.Errorf("getStreamPull failed for %s: %s", payload.StreamName, err)
			return "push://", nil
		}
		// stream pull failed, because it should be pulled from another region or it the pull was already started by another node
		glog.Warningf("another node is currently pulling the stream, waiting %v and retrying, err=%v", streamSourceRetryInterval, err)
		time.Sleep(streamSourceRetryInterval)
	}
	glog.Errorf("error querying mist for STREAM_SOURCE for stream pull request: mistErr=%v", errMist)
	return "push://", nil
}

func playbackIdFor(streamName string) string {
	res := streamName
	parts := strings.Split(res, "+")
	if len(parts) == 2 {
		res = parts[1] // take the playbackID after the prefix e.g. 'video+'
	}
	return res
}

func (c *GeolocationHandlersCollection) resolveReplicatedStream(dtscURL string, streamName string) (string, error) {
	outURL, err := c.resolveNodeURL(dtscURL)
	if err != nil {
		glog.Errorf("error finding STREAM_SOURCE: %s", err)
		return "push://", nil
	}
	glog.V(7).Infof("replying to Mist STREAM_SOURCE request=%s response=%s", streamName, outURL)
	return outURL, nil
}

func (c *GeolocationHandlersCollection) getStreamPull(playbackID string, retryCount int) (string, error) {
	if c.Lapi == nil {
		return "", nil
	}

	// To prevent overloading the LAPI, we limit the rate at which we query for stream pulls
	if c.streamPullRateLimit.shouldLimit(playbackID) {
		return "", errRateLimit
	}

	stream, err := c.LapiCached.GetStreamByPlaybackID(playbackID)
	if err != nil {
		return "", fmt.Errorf("failed to get stream to check stream pull: %w", err)
	}

	if stream.Suspended {
		return "", fmt.Errorf("stream is suspended")
	}

	if stream.Deleted {
		return "", fmt.Errorf("stream is deleted")
	}

	if stream.Pull == nil {
		if stream.IsActive {
			return "", errNoStreamSourceForActiveStream
		}
		return "", nil
	}

	// For stream pull, we limit the rate to prevent overloading the LAPI
	c.streamPullRateLimit.mark(playbackID)

	if stream.PullRegion != "" && c.Config.OwnRegion != stream.PullRegion {
		if retryCount < streamSourceMaxWrongRegionRetries {
			if retryCount == 0 {
				glog.Infof("Stream pull requested in the incorrect region, sending playback request to the correct region, playbackID=%s, region=%s", playbackID, stream.PullRegion)
				c.sendPlaybackRequestAsync(playbackID, stream.PullRegion)
			}
			return "", errPullWrongRegion
		} else {
			glog.Infof("Stream pull requested in the incorrect region, retries exceeded, playbackID=%s, region=%s", playbackID, stream.PullRegion)
		}
	}

	glog.Infof("LockPull for stream %v", playbackID)
	if err := c.Lapi.LockPull(stream.ID, lockPullLeaseTimeout, c.Config.NodeName); err != nil {
		return "", errLockPull
	}

	if len(stream.Pull.Headers) == 0 {
		return stream.Pull.Source, nil
	}

	params := []string{"readtimeout=180"}
	for k, v := range stream.Pull.Headers {
		param := "addheader=" + url.QueryEscape(k+" "+v)
		params = append(params, param)
	}
	finalPullURL := stream.Pull.Source + "?" + strings.Join(params, "&")
	return finalPullURL, nil
}

func (c *GeolocationHandlersCollection) sendPlaybackRequestAsync(playbackID string, region string) {
	members, err := c.membersFiltered(map[string]string{"region": region}, "", "")
	if err != nil || len(members) == 0 {
		glog.Errorf("Error fetching member list: %v", err)
		return
	}
	m := members[rand.Intn(len(members))]

	go func() {
		url := fmt.Sprintf("%s/hls/%s+%s/index.m3u8", m.Tags["https"], c.Config.MistBaseStreamName, playbackID)
		resp, err := http.Get(url)
		if err != nil {
			glog.Errorf("Error making a playback request url=%s, err=%v", url, err)
			return
		}
		resp.Body.Close()
	}()
}

func (c *GeolocationHandlersCollection) membersFiltered(filter map[string]string, status, name string) ([]cluster.Member, error) {
	resp, err := http.Get(c.serfMembersEndpoint)
	if err != nil {
		return []cluster.Member{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return []cluster.Member{}, fmt.Errorf("failed to get members: %s", resp.Status)
	}
	var members []cluster.Member
	if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
		return []cluster.Member{}, err
	}

	res, err := cluster.FilterMembers(members, filter, status, name)
	return res, err
}

func parsePlus(plusString string) (string, string) {
	slice := strings.Split(plusString, "+")
	prefix := ""
	playbackID := ""
	if len(slice) > 2 {
		return "", ""
	}
	if len(slice) == 2 {
		prefix = slice[0]
		playbackID = slice[1]
	} else {
		playbackID = slice[0]
	}
	return prefix, playbackID
}

var regexpHLSPath = regexp.MustCompile(`^/hls/([\w+-]+)/(.*index.m3u8.*)$`)

// Incoming requests might come with some prefix attached to the
// playback ID. We try to drop that here by splitting at `+` and
// picking the last piece. For eg.
// incoming path = '/hls/video+4712oox4msvs9qsf/index.m3u8'
// playbackID = '4712oox4msvs9qsf'
func parsePlaybackIDHLS(path string) (string, string, string, string) {
	m := regexpHLSPath.FindStringSubmatch(path)
	if len(m) < 3 {
		return "", "", "", ""
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", ""
	}
	pathTmpl := "/hls/%s/" + m[2]
	return "hls", prefix, playbackID, pathTmpl
}

var regexpJSONPath = regexp.MustCompile(`^/json_([\w+-]+).js$`)

func parsePlaybackIDJS(path string) (string, string, string, string) {
	m := regexpJSONPath.FindStringSubmatch(path)
	if len(m) < 2 {
		return "", "", "", ""
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", ""
	}
	return "json", prefix, playbackID, "/json_%s.js"
}

var regexpWebRTCPath = regexp.MustCompile(`^/webrtc/([\w+-]+)$`)

func parsePlaybackIDWebRTC(path string) (string, string, string, string) {
	m := regexpWebRTCPath.FindStringSubmatch(path)
	if len(m) < 2 {
		return "", "", "", ""
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", ""
	}
	return "webrtc", prefix, playbackID, "/webrtc/%s"
}

var regexpFLVPath = regexp.MustCompile(`^/flv/([\w+-]+)$`)

func parsePlaybackIDFLV(path string) (string, string, string, string) {
	m := regexpFLVPath.FindStringSubmatch(path)
	if len(m) < 2 {
		return "", "", "", ""
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", ""
	}
	return "flv", prefix, playbackID, "/%s.flv"
}

func parsePlaybackID(path string) (string, string, string, string) {
	parsers := []func(string) (string, string, string, string){parsePlaybackIDHLS, parsePlaybackIDJS, parsePlaybackIDWebRTC, parsePlaybackIDFLV}
	for _, parser := range parsers {
		pathType, prefix, playbackID, suffix := parser(path)
		if pathType != "" {
			return pathType, prefix, playbackID, suffix
		}
	}
	return "", "", "", ""
}

func protocol(r *http.Request) string {
	if r.Header.Get("X-Forwarded-Proto") == "https" {
		return "https"
	}
	return "http"
}

func isValidGPSCoord(lat, lon string) bool {
	if lat == "" || lon == "" {
		return false
	}

	latF, errLat := strconv.ParseFloat(lat, 64)
	lonF, errLon := strconv.ParseFloat(lon, 64)
	if errLat != nil || errLon != nil {
		return false
	}
	return latF >= -90 && latF <= 90 && lonF >= -180 && lonF <= 180
}

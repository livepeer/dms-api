package geolocation

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/metrics"
	mockbalancer "github.com/livepeer/catalyst-api/mocks/balancer"
	mockcluster "github.com/livepeer/catalyst-api/mocks/cluster"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	closestNodeAddr = "someurl.com"
	playbackID      = "abc_XYZ-123"
)

var fakeSerfMember = cluster.Member{
	Name: "fake-serf-member",
	Tags: map[string]string{
		"http":  fmt.Sprintf("http://%s", closestNodeAddr),
		"https": fmt.Sprintf("https://%s", closestNodeAddr),
		"dtsc":  fmt.Sprintf("dtsc://%s", closestNodeAddr),
	},
}

var prefixes = [...]string{"video", "videorec", "stream", "playback", "vod"}

func randomPrefix() string {
	return prefixes[rand.Intn(len(prefixes))]
}

func randomPlaybackID(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[rand.Intn(length)]
	}
	return string(res)
}

func TestPlaybackIDParserWithPrefix(t *testing.T) {
	for i := 0; i < rand.Int()%16+1; i++ {
		id := randomPlaybackID(rand.Int()%24 + 1)
		path := fmt.Sprintf("/hls/%s+%s/index.m3u8", randomPrefix(), id)
		pathType, _, playbackID, _ := parsePlaybackID(path)
		if pathType == "" {
			t.Fail()
		}
		require.Equal(t, id, playbackID)
		require.Equal(t, pathType, "hls")
	}
}

func TestPlaybackIDParserWithSegment(t *testing.T) {
	for i := 0; i < rand.Int()%16+1; i++ {
		id := randomPlaybackID(rand.Int()%24 + 1)
		seg := "2_1"
		path := fmt.Sprintf("/hls/%s+%s/%s/index.m3u8", randomPrefix(), id, seg)
		pathType, _, playbackID, suffix := parsePlaybackID(path)
		if pathType == "" {
			t.Fail()
		}
		require.Equal(t, id, playbackID)
		require.Equal(t, fmt.Sprintf("/hls/%%s/%s/index.m3u8", seg), suffix)
	}
}

func TestPlaybackIDParserWithoutPrefix(t *testing.T) {
	for i := 0; i < rand.Int()%16+1; i++ {
		id := randomPlaybackID(rand.Int()%24 + 1)
		path := fmt.Sprintf("/hls/%s/index.m3u8", id)
		pathType, _, playbackID, _ := parsePlaybackID(path)
		if pathType == "" {
			t.Fail()
		}
		require.Equal(t, id, playbackID)
	}
}

func getHLSURLs(proto, host string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/hls/%s+%s/index.m3u8", proto, host, prefix, playbackID))
	}
	return urls
}

func getJSURLs(proto, host string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/json_%s+%s.js", proto, host, prefix, playbackID))
	}
	return urls
}

func getWebRTCURLs(proto, host string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/webrtc/%s+%s", proto, host, prefix, playbackID))
	}
	return urls
}

func getHLSURLsWithSeg(proto, host, seg, query string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/hls/%s+%s/%s/index.m3u8?%s", proto, host, prefix, playbackID, seg, query))
	}
	return urls
}

func mockHandlers(t *testing.T) *GeolocationHandlersCollection {
	ctrl := gomock.NewController(t)
	mb := mockbalancer.NewMockBalancer(ctrl)
	mc := mockcluster.NewMockCluster(ctrl)
	mb.EXPECT().
		GetBestNode(context.Background(), prefixes[:], playbackID, "", "", "").
		AnyTimes().
		Return(closestNodeAddr, fmt.Sprintf("%s+%s", prefixes[0], playbackID), nil)

	mc.EXPECT().
		Member(map[string]string{}, "alive", closestNodeAddr).
		AnyTimes().
		Return(fakeSerfMember, nil)

	mc.EXPECT().
		ResolveNodeURL(gomock.Any()).DoAndReturn(func(streamURL string) (string, error) {
		return cluster.ResolveNodeURL(mc, streamURL)
	}).
		AnyTimes()
	coll := GeolocationHandlersCollection{
		Balancer: mb,
		Cluster:  mc,
		Config: config.Cli{
			RedirectPrefixes: prefixes[:],
		},
	}
	return &coll
}

func TestRedirectHandler404(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/hls/%s/index.m3u8", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr)...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("https", closestNodeAddr)...)
}

func TestRedirectHandlerHLS_Correct(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/hls/%s/index.m3u8", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr)...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("https", closestNodeAddr)...)
}

func TestRedirectHandlerHLSVOD_Correct(t *testing.T) {
	n := mockHandlers(t)

	n.Balancer.(*mockbalancer.MockBalancer).EXPECT().
		GetBestNode(context.Background(), prefixes[:], playbackID, "", "", "vod").
		AnyTimes().
		Return(closestNodeAddr, fmt.Sprintf("%s+%s", "vod", playbackID), nil)

	pathHLS := fmt.Sprintf("/hls/vod+%s/index.m3u8", playbackID)

	requireReq(t, pathHLS).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/hls/vod+%s/index.m3u8", closestNodeAddr, playbackID))

	requireReq(t, pathHLS).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("https://%s/hls/vod+%s/index.m3u8", closestNodeAddr, playbackID))

	pathJS := fmt.Sprintf("/json_vod+%s.js", playbackID)

	requireReq(t, pathJS).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/json_vod+%s.js", closestNodeAddr, playbackID))

	requireReq(t, pathJS).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("https://%s/json_vod+%s.js", closestNodeAddr, playbackID))
}

func TestRedirectHandlerHLS_SegmentInPath(t *testing.T) {
	n := mockHandlers(t)

	seg := "4_1"
	getParams := "mTrack=0&iMsn=4&sessId=1274784345"
	path := fmt.Sprintf("/hls/%s/%s/index.m3u8?%s", playbackID, seg, getParams)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLsWithSeg("http", closestNodeAddr, seg, getParams)...)
}

func TestRedirectHandlerHLS_InvalidPath(t *testing.T) {
	n := mockHandlers(t)

	requireReq(t, "/hls").result(n).hasStatus(http.StatusNotFound)
	requireReq(t, "/hls").result(n).hasStatus(http.StatusNotFound)
	requireReq(t, "/hls/").result(n).hasStatus(http.StatusNotFound)
	requireReq(t, "/hls/12345").result(n).hasStatus(http.StatusNotFound)
	requireReq(t, "/hls/12345/somepath").result(n).hasStatus(http.StatusNotFound)
}

func TestRedirectHandlerJS_Correct(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/json_%s.js", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getJSURLs("http", closestNodeAddr)...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getJSURLs("https", closestNodeAddr)...)
}

func TestRedirectHandlerWebRTC_Correct(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/webrtc/%s", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getWebRTCURLs("http", closestNodeAddr)...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getWebRTCURLs("https", closestNodeAddr)...)
}

func TestNodeHostRedirect(t *testing.T) {
	n := mockHandlers(t)
	n.Config.NodeHost = "right-host"

	// Success case; get past the redirect handler and 404
	requireReq(t, "http://right-host/any/path").
		withHeader("Host", "right-host").
		result(n).
		hasStatus(http.StatusNotFound)

	requireReq(t, "http://wrong-host/any/path").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "http://right-host/any/path")

	requireReq(t, "http://wrong-host/any/path?foo=bar").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "http://right-host/any/path?foo=bar")

	requireReq(t, "http://wrong-host/any/path").
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "https://right-host/any/path")
}

func TestNodeHostPortRedirect(t *testing.T) {
	n := mockHandlers(t)
	n.Config.NodeHost = "right-host:20443"

	requireReq(t, "http://wrong-host/any/path").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "http://right-host:20443/any/path")

	requireReq(t, "http://wrong-host:1234/any/path").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "http://right-host:20443/any/path")

	requireReq(t, "http://wrong-host:7777/any/path").
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "https://right-host:20443/any/path")

	n.Config.NodeHost = "right-host"
	requireReq(t, "http://wrong-host:7777/any/path").
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "https://right-host/any/path")
}

func TestCdnRedirect(t *testing.T) {
	n := mockHandlers(t)
	CdnRedirectedPlaybackId := "def_ZXY-999"
	n.Config.NodeHost = "someurl.com"
	n.Config.CdnRedirectPrefix, _ = url.Parse("https://external-cdn.com/mist")
	n.Config.CdnRedirectPlaybackIDs = []string{CdnRedirectedPlaybackId}

	// to be redirected to the closest node
	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", playbackID)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/hls/%s/index.m3u8", closestNodeAddr, playbackID))

	// playbackID is configured to be redirected to CDN but the path is /json_video... so redirect it to the closest node
	requireReq(t, fmt.Sprintf("/json_video+%s.js", CdnRedirectedPlaybackId)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/json_video+%s.js", closestNodeAddr, CdnRedirectedPlaybackId))

	// playbackID is configured to be redirected to CDN but it's /webrtc
	require.Equal(t, testutil.CollectAndCount(metrics.Metrics.CDNRedirectWebRTC406), 0)
	requireReq(t, fmt.Sprintf("/webrtc/%s", CdnRedirectedPlaybackId)).
		result(n).
		hasStatus(http.StatusNotAcceptable)
	require.Equal(t, testutil.CollectAndCount(metrics.Metrics.CDNRedirectWebRTC406), 1)
	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectWebRTC406.WithLabelValues("unknown")), float64(0))
	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectWebRTC406.WithLabelValues(CdnRedirectedPlaybackId)), float64(1))

	// this playbackID is configured to be redirected to CDN
	require.Equal(t, testutil.CollectAndCount(metrics.Metrics.CDNRedirectCount), 0)

	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", CdnRedirectedPlaybackId)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://external-cdn.com/mist/hls/%s/index.m3u8", CdnRedirectedPlaybackId))

	require.Equal(t, testutil.CollectAndCount(metrics.Metrics.CDNRedirectCount), 1)
	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectCount.WithLabelValues("unknown")), float64(0))
	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectCount.WithLabelValues(CdnRedirectedPlaybackId)), float64(1))

	// don't redirect if `CdnRedirectPrefix` is not configured
	n.Config.CdnRedirectPrefix = nil
	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", CdnRedirectedPlaybackId)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/hls/%s/index.m3u8", closestNodeAddr, CdnRedirectedPlaybackId))

}

type httpReq struct {
	*testing.T
	*http.Request
}

type httpCheck struct {
	*testing.T
	*httptest.ResponseRecorder
}

func requireReq(t *testing.T, path string) httpReq {
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		t.Fatal(err)
	}

	return httpReq{t, req}
}

func (hr httpReq) withHeader(key, value string) httpReq {
	hr.Header.Set(key, value)
	return hr
}

func (hr httpReq) result(geo *GeolocationHandlersCollection) httpCheck {
	rr := httptest.NewRecorder()
	geo.RedirectHandler()(rr, hr.Request, httprouter.Params{})
	return httpCheck{hr.T, rr}
}

func (hc httpCheck) hasStatus(code int) httpCheck {
	require.Equal(hc, code, hc.Code)
	return hc
}

func (hc httpCheck) hasHeader(key string, values ...string) httpCheck {
	header := hc.Header().Get(key)
	require.Contains(hc, values, header)
	return hc
}

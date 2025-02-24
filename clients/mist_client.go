package clients

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/patrickmn/go-cache"
)

//go:generate mockgen -source=./mist_client.go -destination=../mocks/clients/mist_client.go

const (
	stateCacheKey          = "stateCacheKey"
	defaultCacheExpiration = time.Second
	cacheCleanupInterval   = 10 * time.Minute
)

type MistAPIClient interface {
	AddStream(streamName, sourceUrl string) error
	PushAutoAdd(streamName, targetURL string) error
	PushAutoRemove(streamParams []interface{}) error
	PushStop(id int64) error
	InvalidateSessions(streamName string) error
	DeleteStream(streamName string) error
	NukeStream(streamName string) error
	StopSessions(streamName string) error
	AddTrigger(streamName []string, triggerName, triggerCallback string, sync bool) error
	DeleteTrigger(streamName []string, triggerName string) error
	GetStreamInfo(streamName string) (MistStreamInfo, error)
	GetState() (MistState, error)
}

type MistClient struct {
	ApiUrl     string
	Username   string
	Password   string
	HttpReqUrl string
	configMu   sync.Mutex
	cache      *cache.Cache
	httpClient *http.Client
}

const MistClientTimeout = 1 * time.Minute

func NewMistAPIClient(user, password, host string, port int, timeout time.Duration) MistAPIClient {
	if timeout == 0 {
		timeout = MistClientTimeout
	}
	mist := &MistClient{
		ApiUrl:     fmt.Sprintf("http://%s:%d", host, port),
		Username:   user,
		Password:   password,
		cache:      cache.New(defaultCacheExpiration, cacheCleanupInterval),
		httpClient: newRetryableClient(&http.Client{Timeout: timeout}),
	}
	return mist
}

type MistStreamInfoTrack struct {
	Codec   string `json:"codec,omitempty"`
	Firstms int64  `json:"firstms,omitempty"`
	Idx     int    `json:"idx,omitempty"`
	Init    string `json:"init,omitempty"`
	Lastms  int64  `json:"lastms,omitempty"`
	Maxbps  int    `json:"maxbps,omitempty"`
	Trackid int    `json:"trackid,omitempty"`
	Type    string `json:"type,omitempty"`
	Bps     int    `json:"bps,omitempty"`

	// Audio Only Fields
	Channels int `json:"channels,omitempty"`
	Rate     int `json:"rate,omitempty"`
	Size     int `json:"size,omitempty"`

	// Video Only Fields
	Bframes int `json:"bframes,omitempty"`
	Fpks    int `json:"fpks,omitempty"`
	Height  int `json:"height,omitempty"`
	Width   int `json:"width,omitempty"`
}

type MistStreamInfoSource struct {
	Hrn          string `json:"hrn,omitempty"`
	Priority     int    `json:"priority,omitempty"`
	Relurl       string `json:"relurl,omitempty"`
	SimulTracks  int    `json:"simul_tracks,omitempty"`
	TotalMatches int    `json:"total_matches,omitempty"`
	Type         string `json:"type,omitempty"`
	URL          string `json:"url,omitempty"`
	PlayerURL    string `json:"player_url,omitempty"`
}

type MistStreamInfoMetadata struct {
	Bframes int                            `json:"bframes,omitempty"`
	Tracks  map[string]MistStreamInfoTrack `json:"tracks,omitempty"`
	Version int                            `json:"version,omitempty"`
	Vod     int                            `json:"vod,omitempty"`
}

type MistStreamInfo struct {
	Height int                    `json:"height,omitempty"`
	Meta   MistStreamInfoMetadata `json:"meta,omitempty"`
	Selver int                    `json:"selver,omitempty"`
	Source []MistStreamInfoSource `json:"source,omitempty"`
	Type   string                 `json:"type,omitempty"`
	Width  int                    `json:"width,omitempty"`
	Error  string                 `json:"error,omitempty"`
}

type MistState struct {
	ActiveStreams map[string]*ActiveStream    `json:"active_streams"`
	StreamsStats  map[string]*MistStreamStats `json:"stats_streams"`
	PushList      []*MistPush                 `json:"push_list"`
	PushAutoList  []*MistPushAuto             `json:"push_auto_list"`
}

type AuthorizationResponse struct {
	Authorize struct {
		Status    string `json:"status"`
		Challenge string `json:"challenge"`
	} `json:"authorize"`
}

func (ms MistState) IsIngestStream(stream string) bool {
	if ms.ActiveStreams == nil {
		return false
	}
	as, ok := ms.ActiveStreams[stream]
	// Mist returns:
	//  - "push://" for ingest streams
	//  - "push://INTERNAL_ONLY:dtsc://*" for playback streams
	//  - "push://INTERNAL_ONLY:<url>" for ingest pull-source streams
	return ok && !(strings.HasPrefix(as.Source, "push://INTERNAL_ONLY:dtsc://"))
}

type MistPush struct {
	ID           int64
	Stream       string
	OriginalURL  string
	EffectiveURL string
	Stats        *MistPushStats
}

func (p *MistPush) UnmarshalJSON(data []byte) error {
	// this field is undocumented and shows up as null everytime.
	var unknown json.RawMessage
	if err := unmarshalJSONArray(data, &p.ID, &p.Stream, &p.OriginalURL, &p.EffectiveURL, &unknown, &p.Stats); err != nil {
		return err
	}
	return nil
}

type MistPushAuto struct {
	Stream       string
	Target       string
	StreamParams []interface{}
}

func (p *MistPushAuto) UnmarshalJSON(data []byte) error {
	var parsed []interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if len(parsed) < 2 {
		return errors.New("invalid Mist auto_push_list entry, less than 2 params, expected at least [stream, target, ...]")
	}
	stream, ok := parsed[0].(string)
	if !ok {
		return fmt.Errorf("invalid Mist auto_push_list entry, expected first param as 'stream', but got %s", parsed[0])
	}
	target, ok := parsed[1].(string)
	if !ok {
		return fmt.Errorf("invalid Mist auto_push_list entry, expected second param as 'target', but got %s", parsed[1])
	}

	p.Stream = stream
	p.Target = target
	p.StreamParams = parsed

	return nil
}

type ActiveStream struct {
	Source string
}

func (p *ActiveStream) UnmarshalJSON(data []byte) error {
	if err := unmarshalJSONArray(data, &p.Source); err != nil {
		return err
	}
	return nil
}

type MistStreamStats struct {
	Clients     int
	MediaTimeMs int64
}

func (s *MistStreamStats) UnmarshalJSON(data []byte) error {
	if err := unmarshalJSONArray(data, &s.Clients, &s.MediaTimeMs); err != nil {
		return err
	}
	return nil
}

// unmarshalJSONArray parses data (which is a JSON array) into the provided values.
// If data JSON array is longer than values, then the excessive items are ignored (Mist returned irrelevant data)
// If values are longer than data, then the excessive values are left unfilled (Mist may return different array length
// depending on its status, e.g., push_list may not include the last element (stats) if the push has never succeeded.
func unmarshalJSONArray(data []byte, values ...interface{}) error {
	var valuesData []json.RawMessage
	if err := json.Unmarshal(data, &valuesData); err != nil {
		return err
	}
	min := len(values)
	if len(valuesData) < min {
		min = len(valuesData)
	}
	for i := 0; i < min; i++ {
		if err := json.Unmarshal(valuesData[i], values[i]); err != nil {
			return err
		}
	}
	return nil
}

type MistPushStats struct {
	ActiveSeconds int64 `json:"active_seconds"`
	Bytes         int64 `json:"bytes"`
	MediaTime     int64 `json:"mediatime"`
	Tracks        []int `json:"tracks"`
}

func (mc *MistClient) AddStream(streamName, sourceUrl string) error {
	c := commandAddStream(streamName, sourceUrl)
	return wrapErr(validateAddStream(mc.sendCommand(c)), streamName)
}

func (mc *MistClient) PushAutoAdd(streamName, targetURL string) error {
	c := commandPushAutoAdd(streamName, targetURL)
	return wrapErr(validatePushAutoAdd(mc.sendCommand(c)), streamName)
}

func (mc *MistClient) PushAutoRemove(streamParams []interface{}) error {
	if len(streamParams) == 0 {
		return errors.New("streamParams cannot be empty")
	}
	streamName, ok := streamParams[0].(string)
	if !ok {
		return errors.New("first param in streamParams must be the stream name")
	}
	c := commandPushAutoRemove(streamParams)
	return wrapErr(validatePushAutoRemove(mc.sendCommand(c)), streamName)
}

func (mc *MistClient) PushStop(id int64) error {
	c := commandPushStop(id)
	if err := validatePushAutoRemove(mc.sendCommand(c)); err != nil {
		return err
	}
	return nil
}

func (mc *MistClient) InvalidateSessions(streamName string) error {
	c := commandInvalidateSessions(streamName)
	return wrapErr(validateInvalidateSessions(mc.sendCommand(c)), streamName)
}

func (mc *MistClient) DeleteStream(streamName string) error {
	// Need to send both 'deletestream' and 'nuke_stream' in order to remove stream with all configuration and processes
	deleteErr := wrapErr(validateDeleteStream(mc.sendCommand(commandDeleteStream(streamName))), streamName)
	nukeErr := wrapErr(validateNukeStream(mc.sendCommand(commandNukeStream(streamName))), streamName)
	if deleteErr != nil || nukeErr != nil {
		return fmt.Errorf("deleting stream failed, 'deletestream' command err: %v, 'nuke_stream' command err: %v", deleteErr, nukeErr)
	}
	return nil
}

func (mc *MistClient) NukeStream(streamName string) error {
	c := commandNukeStream(streamName)
	if err := validateNukeStream(mc.sendCommand(c)); err != nil {
		return err
	}
	return nil
}

func (mc *MistClient) StopSessions(streamName string) error {
	c := commandStopSessions(streamName)
	if err := validateAuth(mc.sendCommand(c)); err != nil {
		return err
	}
	return nil
}

// AddTrigger adds a trigger `triggerName` for the stream `streamName`.
// Note that Mist API supports only overriding the whole trigger configuration, therefore this function needs to:
// 1. Acquire a lock
// 2. Get current triggers
// 3. Add a new trigger (or update the existing one)
// 4. Override the triggers
// 5. Release the lock
func (mc *MistClient) AddTrigger(streamNames []string, triggerName, triggerCallback string, sync bool) error {
	mc.configMu.Lock()
	defer mc.configMu.Unlock()

	triggers, err := mc.getCurrentTriggers()
	if err != nil {
		return err
	}
	c := commandAddTrigger(streamNames, triggerName, triggerCallback, triggers, sync)
	resp, err := mc.sendCommand(c)
	return validateAddTrigger(streamNames, triggerName, resp, err, sync)
}

// DeleteTrigger deletes triggers with the name `triggerName` for the stream `streamName`.
// Note that Mist API supports only overriding the whole trigger configuration, therefore this function needs to:
// 1. Acquire a lock
// 2. Get current triggers
// 3. Add a new trigger (or update the existing one)
// 4. Override the triggers
// 5. Release the lock
func (mc *MistClient) DeleteTrigger(streamNames []string, triggerName string) error {
	mc.configMu.Lock()
	defer mc.configMu.Unlock()

	triggers, err := mc.getCurrentTriggers()
	if err != nil {
		return err
	}
	c := commandDeleteTrigger(streamNames, triggerName, triggers)
	resp, err := mc.sendCommand(c)
	return validateDeleteTrigger(streamNames, triggerName, resp, err)
}

func (mc *MistClient) getCurrentTriggers() (Triggers, error) {
	c := commandGetTriggers()
	resp, err := mc.sendCommand(c)
	if err := validateAuth(resp, err); err != nil {
		return nil, err
	}

	cc := MistConfig{}
	if err := json.Unmarshal([]byte(resp), &cc); err != nil {
		return nil, err
	}

	if cc.Config.Triggers == nil {
		return Triggers{}, nil
	}

	return cc.Config.Triggers, nil
}

func (mc *MistClient) sendCommand(command interface{}) (string, error) {
	resp, err := mc.sendCommandToMist(command)
	if authErr := validateAuth(resp, err); authErr != nil {
		glog.Infof("Request to Mist not authorized, authorizing and retrying command: %v", command)
		if authErr := mc.authorize(resp); authErr != nil {
			glog.Warningf("Failed to authorize Mist request: %v", authErr)
			return resp, err
		}
		return mc.sendCommandToMist(command)
	}
	return resp, err
}

// authorize authorizes the communication with Mist Server by sending the authorization command.
// Mist doc: https://docs.mistserver.org/docs/mistserver/integration/api/authentication
func (mc *MistClient) authorize(unauthResp string) error {
	r := AuthorizationResponse{}
	if err := json.Unmarshal([]byte(unauthResp), &r); err != nil {
		return err
	}
	passwordMd5, err := computeMD5Hash(mc.Password)
	if err != nil {
		return err
	}
	password, err := computeMD5Hash(passwordMd5 + r.Authorize.Challenge)
	if err != nil {
		return err
	}
	c := commandAuthorize(mc.Username, password)
	return validateAuth(mc.sendCommandToMist(c))
}

func (mc *MistClient) sendCommandToMist(command interface{}) (string, error) {
	c, err := commandToString(command)
	if err != nil {
		return "", err
	}
	payload := payloadFor(c)
	req, err := http.NewRequest(http.MethodPost, mc.ApiUrl, bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	resp, err := metrics.MonitorRequest(metrics.Metrics.MistClient, mc.httpClient, req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), err
}

func commandToString(command interface{}) (string, error) {
	res, err := json.Marshal(command)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

func payloadFor(command string) string {
	return fmt.Sprintf("command=%s", url.QueryEscape(command))
}

func (mc *MistClient) sendHttpRequest(streamName string) (string, error) {
	jsonStreamInfoUrl := mc.HttpReqUrl + "/json_" + streamName + ".js"

	req, err := http.NewRequest(http.MethodGet, jsonStreamInfoUrl, nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := metrics.MonitorRequest(metrics.Metrics.MistClient, mc.httpClient, req)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("got HTTP Status %d from Mist StreamInfo API", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), err
}

func (mc *MistClient) GetStreamInfo(streamName string) (MistStreamInfo, error) {
	resp, err := mc.sendHttpRequest(streamName)
	if err != nil {
		return MistStreamInfo{}, fmt.Errorf("error making GetStreamInfo HTTP request for %q: %s", streamName, err)
	}

	var msi MistStreamInfo
	if err := json.Unmarshal([]byte(resp), &msi); err != nil {
		return MistStreamInfo{}, fmt.Errorf("error unmarshalling MistStreamInfo JSON for %q: %s\nResponse Body: %s", streamName, err, resp)
	}

	if msi.Error != "" {
		return msi, fmt.Errorf("%s", msi.Error)
	}

	return msi, nil
}

func (mc *MistClient) GetState() (MistState, error) {
	cachedState, found := mc.cache.Get(stateCacheKey)
	if found {
		glog.V(6).Info("returning mist GetState from cache")
		return *cachedState.(*MistState), nil
	}

	c := commandState()
	resp, err := mc.sendCommand(c)
	if err := validateAuth(resp, err); err != nil {
		return MistState{}, err
	}

	stats := MistState{}
	if err := json.Unmarshal([]byte(resp), &stats); err != nil {
		return MistState{}, err
	}

	mc.cache.SetDefault(stateCacheKey, &stats)

	return stats, nil
}

type authorizeCommand struct {
	Authorize Authorize `json:"authorize"`
}

type Authorize struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func commandAuthorize(username, password string) interface{} {
	return authorizeCommand{
		Authorize: Authorize{
			Username: username,
			Password: password,
		},
	}
}

type addStreamCommand struct {
	Addstream map[string]Stream `json:"addstream"`
}

type Stream struct {
	Source string `json:"source"`
}

func commandAddStream(name, url string) interface{} {
	return addStreamCommand{
		Addstream: map[string]Stream{
			name: {
				Source: url,
			},
		},
	}
}

type invalidateSessionsCommand struct {
	InvalidateSessions string `json:"invalidate_sessions"`
}

func commandInvalidateSessions(name string) interface{} {
	return invalidateSessionsCommand{
		InvalidateSessions: name,
	}
}

type deleteStreamCommand struct {
	Deletestream map[string]interface{} `json:"deletestream"`
}

func commandDeleteStream(name string) deleteStreamCommand {
	return deleteStreamCommand{
		Deletestream: map[string]interface{}{name: nil},
	}
}

type nukeStreamCommand struct {
	Nukestream string `json:"nuke_stream"`
}

func commandNukeStream(name string) nukeStreamCommand {
	return nukeStreamCommand{
		Nukestream: name,
	}
}

type stopSessionsCommand struct {
	StopSessions string `json:"stop_sessions"`
}

func commandStopSessions(name string) stopSessionsCommand {
	return stopSessionsCommand{
		StopSessions: name,
	}
}

type pushAutoAddCommand struct {
	PushAutoAdd PushAutoAdd `json:"push_auto_add"`
}

type PushAutoAdd struct {
	Stream string `json:"stream"`
	Target string `json:"target"`
}

type pushAutoRemoveCommand struct {
	PushAutoRemove [][]interface{} `json:"push_auto_remove"`
}

func commandPushAutoAdd(streamName, target string) pushAutoAddCommand {
	return pushAutoAddCommand{
		PushAutoAdd: PushAutoAdd{
			Stream: streamName,
			Target: target,
		},
	}
}

func commandPushAutoRemove(streamParams []interface{}) pushAutoRemoveCommand {
	return pushAutoRemoveCommand{
		PushAutoRemove: [][]interface{}{streamParams},
	}
}

type pushStopCommand struct {
	PushStop []int64 `json:"push_stop"`
}

func commandPushStop(id int64) pushStopCommand {
	return pushStopCommand{
		PushStop: []int64{id},
	}
}

type MistConfig struct {
	Config Config `json:"config"`
}

type ConfigTrigger struct {
	Handler string   `json:"handler"`
	Streams []string `json:"streams"`
	Sync    bool     `json:"sync"`
}

type Triggers map[string][]ConfigTrigger

type Config struct {
	Triggers map[string][]ConfigTrigger `json:"triggers,omitempty"`
}

func commandAddTrigger(streamNames []string, triggerName, handlerUrl string, currentTriggers Triggers, sync bool) MistConfig {
	newTrigger := &ConfigTrigger{
		Handler: handlerUrl,
		Streams: streamNames,
		Sync:    sync,
	}
	return commandUpdateTrigger(streamNames, triggerName, currentTriggers, newTrigger, sync)
}

func commandDeleteTrigger(streamNames []string, triggerName string, currentTriggers Triggers) MistConfig {
	return commandUpdateTrigger(streamNames, triggerName, currentTriggers, nil, false)
}

func commandUpdateTrigger(streamNames []string, triggerName string, currentTriggers Triggers, replaceTrigger *ConfigTrigger, sync bool) MistConfig {
	triggersMap := currentTriggers

	triggers := triggersMap[triggerName]
	triggers = deleteAllTriggersFor(triggers, streamNames)
	if replaceTrigger != nil {
		triggers = append(triggers, *replaceTrigger)
	}

	triggersMap[triggerName] = triggers
	return MistConfig{Config{Triggers: triggersMap}}
}

func deleteAllTriggersFor(triggers []ConfigTrigger, streamNames []string) []ConfigTrigger {
	var res []ConfigTrigger
	for _, t := range triggers {
		if !sameStringSlice(streamNames, t.Streams) {
			res = append(res, t)
		}
	}
	return res
}

// set equality; returns true if they contain the same strings disregarding order
func sameStringSlice(s1, s2 []string) bool {
	s1 = append([]string{}, s1...)
	s2 = append([]string{}, s2...)
	sort.Strings(s1)
	sort.Strings(s2)
	return reflect.DeepEqual(s1, s2)
}

func commandGetTriggers() MistConfig {
	// send an empty config struct returns the current Mist configuration
	return MistConfig{}
}

type stateCommand struct {
	ActiveStreams []string `json:"active_streams"`
	StatsStreams  []string `json:"stats_streams"`
	PushList      bool     `json:"push_list"`
	PushAutoList  bool     `json:"push_auto_list"`
}

func commandState() stateCommand {
	return stateCommand{
		ActiveStreams: []string{"source"},
		StatsStreams:  []string{"clients", "lastms"},
		PushList:      true,
		PushAutoList:  true,
	}
}

func validateAddStream(resp string, err error) error {
	if err != validateAuth(resp, err) {
		return err
	}

	r := struct {
		Streams map[string]interface{} `json:"streams"`
	}{}
	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		return err
	}
	if len(r.Streams) == 0 {
		return errors.New("adding stream failed")
	}
	return nil
}

func validateInvalidateSessions(resp string, err error) error {
	// nothing other than auth to validate, Mist always returns the same response
	return validateAuth(resp, err)
}

func validatePushAutoAdd(resp string, err error) error {
	// nothing other than auth to validate, Mist always returns the same response
	return validateAuth(resp, err)
}

func validatePushAutoRemove(resp string, err error) error {
	// nothing other than auth to validate, Mist always returns the same response
	return validateAuth(resp, err)
}

func validateDeleteStream(resp string, err error) error {
	// nothing other than auth to validate, Mist always returns the same response
	return validateAuth(resp, err)
}

func validateNukeStream(resp string, err error) error {
	// nothing other than auth to validate, Mist always returns the same response
	return validateAuth(resp, err)
}

func validateAddTrigger(streamNames []string, triggerName, resp string, err error, sync bool) error {
	if err != validateAuth(resp, err) {
		return err
	}

	r := MistConfig{}
	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		return err
	}

	if r.Config.Triggers == nil {
		return errors.New("adding trigger failed, nil triggers value in response")
	}
	ts, ok := r.Config.Triggers[triggerName]
	if !ok {
		return fmt.Errorf("adding trigger failed, no trigger '%s' in response", triggerName)
	}
	for _, t := range ts {
		if sameStringSlice(t.Streams, streamNames) && t.Sync == sync {
			return nil
		}
	}
	return fmt.Errorf("adding trigger failed, no stream '%v' found in trigger '%s'", streamNames, triggerName)
}

func validateDeleteTrigger(streamNames []string, triggerName, resp string, err error) error {
	if err := validateAuth(resp, err); err != nil {
		return err
	}

	r := MistConfig{}
	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		return err
	}

	if r.Config.Triggers == nil {
		return nil
	}
	ts, ok := r.Config.Triggers[triggerName]
	if !ok {
		return nil
	}
	for _, t := range ts {
		if sameStringSlice(t.Streams, streamNames) {
			return fmt.Errorf("deleting trigger failed, stream '%v' found in trigger '%s'", streamNames, triggerName)
		}
	}
	return nil
}

func validateAuth(resp string, err error) error {
	if err != nil {
		return err
	}
	r := AuthorizationResponse{}

	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		return err
	}
	if r.Authorize.Status != "OK" {
		return errors.New("authorization to Mist API failed")
	}
	return nil
}

func computeMD5Hash(input string) (string, error) {
	hasher := md5.New()
	_, err := io.WriteString(hasher, input)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func wrapErr(err error, streamName string) error {
	if err != nil {
		return fmt.Errorf("error in processing stream '%s': %w", streamName, err)
	}
	return nil
}

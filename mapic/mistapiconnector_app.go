//nolint:all
package mistapiconnector

//go:generate mockgen -source=./mistapiconnector_app.go -destination=../mocks/mistapiconnector/mistapiconnector_app.go

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/mapic/metrics"
	"github.com/livepeer/catalyst-api/mapic/model"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
)

const streamPlaybackPrefix = "playback_"
const waitForPushError = 7 * time.Second
const waitForPushErrorIncreased = 2 * time.Minute
const keepStreamAfterEnd = 15 * time.Second

const ownExchangeName = "lp_mist_api_connector"
const webhooksExchangeName = "webhook_default_exchange"
const eventMultistreamConnected = "multistream.connected"
const eventMultistreamError = "multistream.error"
const eventMultistreamDisconnected = "multistream.disconnected"
const eventMultistreamErrorTolerance = 2

type (
	// IMac creates new Mist API Connector application
	IMac interface {
		Start(ctx context.Context) error
		MetricsHandler() http.Handler
		MistMetricsHandler() http.Handler
		RefreshStreamIfNeeded(playbackID string)
		NukeStream(playbackID string)
		InvalidateAllSessions(playbackID string)
		StopSessions(playbackID string)
		IStreamCache
	}

	IStreamCache interface {
		GetCachedStream(playbackID string) *api.Stream
	}

	pushStatus struct {
		target              *api.MultistreamTarget
		profile             string
		lastEvent           string
		lastEventAt         time.Time
		lastEventErrorCount int
		metrics             *data.MultistreamMetrics
		mu                  sync.Mutex
	}

	streamInfo struct {
		id        string
		isLazy    bool
		stream    *api.Stream
		startedAt time.Time

		mu               sync.Mutex
		done             chan struct{}
		stopped          bool
		pushStatus       map[string]*pushStatus
		lastSeenBumpedAt time.Time
	}

	// MacOptions configuration object
	MacOptions struct {
		NodeID, MistHost string
		LivepeerAPI      *api.Client
		BalancerHost     string
		CheckBandwidth   bool
		RoutePrefix, PlaybackDomain, MistURL,
		SendAudio, BaseStreamName string
		AMQPUrl, OwnRegion        string
		MistStreamSource          string
		MistHardcodedBroadcasters string
		NoMistScrapeMetrics       bool
	}

	mac struct {
		ctx                       context.Context
		cancel                    context.CancelFunc
		lapi                      *api.Client
		lapiCached                *ApiClientCached
		balancerHost              string
		mu                        sync.RWMutex
		mistHot                   string
		checkBandwidth            bool
		baseStreamName            string
		streamInfo                map[string]*streamInfo
		producer                  event.AMQPProducer
		nodeID                    string
		ownRegion                 string
		mistStreamSource          string
		mistHardcodedBroadcasters string
		config                    *config.Cli
		broker                    misttriggers.TriggerBroker
		mist                      clients.MistAPIClient
		streamUpdated             chan struct{}
		metricsCollector          *metricsCollector
		streamMetricsRe           *regexp.Regexp
	}
)

func (mc *mac) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// todo: you're not supposed to store these...
	mc.ctx = ctx
	mc.cancel = cancel

	mc.broker.OnStreamBuffer(mc.handleStreamBuffer)
	mc.broker.OnPushRewrite(mc.handlePushRewrite)
	mc.broker.OnLiveTrackList(mc.handleLiveTrackList)
	mc.broker.OnPushOutStart(mc.handlePushOutStart)
	mc.broker.OnPushEnd(mc.handlePushEnd)

	lapi, _ := api.NewAPIClientGeolocated(api.ClientOptions{
		Server:      mc.config.APIServer,
		AccessToken: mc.config.APIToken,
	})
	mc.lapi = lapi
	mc.lapiCached = NewApiClientCached(lapi)

	if mc.balancerHost != "" && !strings.Contains(mc.balancerHost, ":") {
		mc.balancerHost = mc.balancerHost + ":8042" // must set default port for Mist's Load Balancer
	}
	var producer event.AMQPProducer
	if mc.config.AMQPURL != "" {
		pu, err := url.Parse(mc.config.AMQPURL)
		if err != nil {
			return fmt.Errorf("error parsing AMQP url err=%w", err)
		}

		glog.Infof("Creating AMQP producer with url=%s", pu.Redacted())
		setup := func(c event.AMQPChanSetup) error {
			if err := c.ExchangeDeclarePassive(webhooksExchangeName, "topic", true, false, false, false, nil); err != nil {
				glog.Warningf("mist-api-connector: Webhooks exchange does not exist. exchange=%s err=%v", webhooksExchangeName, err)
			}
			return c.ExchangeDeclare(ownExchangeName, "topic", true, false, false, false, nil)
		}
		producer, err = event.NewAMQPProducer(mc.config.AMQPURL, event.NewAMQPConnectFunc(setup))
		if err != nil {
			return err
		}
		mc.producer = producer
	} else {
		glog.Infof("AMQP url is empty!")
	}
	if producer != nil && mc.config.MistScrapeMetrics {
		mc.metricsCollector = createMetricsCollector(mc.nodeID, mc.ownRegion, mc.mist, lapi, producer, ownExchangeName, mc)
	}

	mc.streamUpdated = make(chan struct{}, 1)
	go func() {
		mc.reconcileLoop(ctx)
	}()

	<-ctx.Done()
	return nil
}

func (mc *mac) MetricsHandler() http.Handler {
	return metrics.Exporter
}

func (mc *mac) RefreshStreamIfNeeded(playbackID string) {
	if !mc.streamExists(playbackID) {
		// Ignore streams that aren't already in memory. This is to avoid a surge of
		// requests to the API on any event. For streams not synced to mapic memory,
		// it will be reconciled on the next loop anyway (30s) and get fixed soon.
		return
	}
	si, err := mc.refreshStream(playbackID)
	if err != nil {
		glog.Errorf("Error refreshing stream playbackID=%s err=%q", playbackID, err)
		return
	}

	// trigger an immediate stream reconcile to already nuke it if needed
	mc.reconcileSingleStream(si)
}

func (mc *mac) NukeStream(playbackID string) {
	mc.nukeAllStreamNames(playbackID)
}

func (mc *mac) StopSessions(playbackID string) {
	mistState, err := mc.mist.GetState()
	if err != nil {
		glog.Errorf("error stopping sessions, mist GetState failed playbackId=%s err=%q", playbackID, err)
		return
	}

	streamNames := []string{
		"video+" + playbackID,
	}

	for _, streamName := range streamNames {
		if !mistState.IsIngestStream(streamName) {
			// only call stop sessions if we are the ingest node for this stream
			continue
		}
		glog.V(7).Infof("calling mist StopSessions playbackId=%s streamName=%s", playbackID, streamName)
		err := mc.mist.StopSessions(streamName)
		if err != nil {
			glog.Errorf("error stopping sessions playbackId=%s streamName=%s err=%q", playbackID, streamName, err)
		}
	}
}

func (mc *mac) InvalidateAllSessions(playbackID string) {
	mc.invalidateAllSessions(playbackID)
}

func (mc *mac) handleStreamBuffer(ctx context.Context, payload *misttriggers.StreamBufferPayload) error {
	var isActive bool
	if payload.IsEmpty() {
		isActive = false
	} else if payload.IsFull() {
		isActive = true
	} else {
		// Ignore all other STREAM_BUFFER states for setting stream /setactive
		return nil
	}
	playbackID := payload.StreamName
	if mc.baseStreamName != "" && strings.Contains(playbackID, "+") {
		playbackID = strings.Split(playbackID, "+")[1]
	}
	if info, ok := mc.getStreamInfoLogged(playbackID); ok {
		glog.Infof("Setting stream's manifestID=%s playbackID=%s active status to %v", info.id, playbackID, isActive)
		ok, err := mc.lapi.SetActive(info.id, isActive, info.startedAt)
		if !ok || err != nil {
			glog.Errorf("Error calling setactive for stream's manifestID=%s playbackID=%s err=%v", info.id, playbackID, err)
		}
		mc.emitStreamStateEvent(info.stream, data.StreamState{Active: isActive})
		if isActive {
			metrics.StartStream()
		} else {
			info.mu.Lock()
			info.stopped = true
			info.mu.Unlock()
			mc.removeInfoDelayed(playbackID, info.done)
			metrics.StopStream(true)
		}
	}

	return nil
}

func (mc *mac) handlePushRewrite(ctx context.Context, payload *misttriggers.PushRewritePayload) (string, error) {
	streamKey := payload.StreamName
	var responseName string
	if payload.URL.Scheme == "rtmp" {
		pp := strings.Split(payload.URL.Path, "/")
		if len(pp) != 3 {
			glog.Errorf("Push rewrite URL wrongly formatted - should be in format rtmp://mist.host/live/streamKey payload=%s", payload)
			return "", nil
		}
	}
	glog.V(model.VVERBOSE).Infof("Requested stream key is '%s'", streamKey)
	// ask API
	stream, err := mc.lapi.GetStreamByKey(streamKey)
	if errors.Is(err, api.ErrNotExists) {
		glog.Errorf("Stream not found for push rewrite streamKey=%s err=%v", streamKey, err)
		return "", nil
	} else if err != nil || stream == nil {
		return "", fmt.Errorf("Error getting stream info from Livepeer API streamKey=%s err=%v", streamKey, err)
	}
	glog.V(model.VERBOSE).Infof("For stream %s got info %+v", streamKey, stream)

	if stream.PlaybackID != "" {
		mc.mu.Lock()
		if info, ok := mc.streamInfo[stream.PlaybackID]; ok {
			info.mu.Lock()
			glog.Infof("Stream playbackID=%s stopped=%v already in map, removing its info", stream.PlaybackID, info.stopped)
			info.mu.Unlock()
			mc.removeInfoLocked(stream.PlaybackID)
		}
		info := &streamInfo{
			id:         stream.ID,
			stream:     stream,
			done:       make(chan struct{}),
			pushStatus: make(map[string]*pushStatus),
			startedAt:  time.Now(),
		}
		mc.streamInfo[stream.PlaybackID] = info
		mc.mu.Unlock()
		streamKey = stream.PlaybackID
		// streamKey = strings.ReplaceAll(streamKey, "-", "")
		if mc.balancerHost != "" {
			streamKey = streamPlaybackPrefix + streamKey
		}
		if mc.baseStreamName == "" {
			responseName = streamKey
		} else {
			responseName = mc.wildcardPlaybackID(stream)
		}
	} else {
		glog.Errorf("Shouldn't happen streamID=%s", stream.ID)
	}
	if stream.Deleted || stream.Suspended {
		// Do not allow to start deleted or suspended streams
		return "", nil
	}
	glog.Infof("Responded with '%s'", responseName)
	return responseName, nil
}

func (mc *mac) handleLiveTrackList(ctx context.Context, payload *misttriggers.LiveTrackListPayload) error {
	go func() {
		videoTracksNum := payload.CountVideoTracks()
		playbackID := mistStreamName2playbackID(payload.StreamName)
		glog.Infof("for video %s got %d video tracks", playbackID, videoTracksNum)
		mc.refreshStream(playbackID)
	}()
	return nil
}

func (mc *mac) streamExists(playbackID string) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	_, streamExists := mc.streamInfo[playbackID]
	return streamExists
}

func (mc *mac) refreshStream(playbackID string) (*streamInfo, error) {
	si, err := mc.refreshStreamInfo(playbackID)
	if err != nil {
		glog.Errorf("Error refreshing stream info for playbackID=%s", playbackID)
		return nil, err
	}

	select {
	case mc.streamUpdated <- struct{}{}:
		// trigger reconcile loop
	default:
		// do not block if reconcile multistream already triggered
	}

	return si, nil
}

func (mc *mac) handlePushOutStart(ctx context.Context, payload *misttriggers.PushOutStartPayload) (string, error) {
	go func() {
		playbackID := mistStreamName2playbackID(payload.StreamName)
		if info, ok := mc.getStreamInfoLogged(playbackID); ok {
			info.mu.Lock()
			defer info.mu.Unlock()
			if pushInfo, ok := info.pushStatus[payload.URL]; ok {
				go mc.waitPush(info, pushInfo)
			} else {
				glog.Errorf("For stream playbackID=%s got unknown RTMP push %s", playbackID, payload.URL)
			}
		}

	}()
	return payload.URL, nil
}

// waits for RTMP push error
func (mc *mac) waitPush(info *streamInfo, pushInfo *pushStatus) {
	waitForPushEvent := waitForPushError
	var waitForPushEventWasIncreased bool
	pushInfo.mu.Lock()
	if pushInfo.lastEventErrorCount > eventMultistreamErrorTolerance {
		// Most probably there is an issue with the multistream target; increase the wait time between PUSH_OUT_START
		// and PUSH_END to avoid flickering between 'connected' and 'error' events
		waitForPushEvent = waitForPushErrorIncreased
		waitForPushEventWasIncreased = true
	}
	pushInfo.mu.Unlock()

	select {
	case <-info.done:
		return
	case <-time.After(waitForPushEvent):
		info.mu.Lock()
		defer info.mu.Unlock()
		if info.stopped {
			return
		}
		pushInfo.mu.Lock()
		defer pushInfo.mu.Unlock()

		// push disabled in the meantime
		pushDisabled := pushInfo.target.Disabled
		// no need to send it again
		eventAlreadySent := pushInfo.lastEvent == eventMultistreamConnected
		// there was another event after PUSH_OUT, no need to send connected event
		staleWait := time.Now().Add(-waitForPushEvent).Before(pushInfo.lastEventAt)
		if pushDisabled || eventAlreadySent || staleWait {
			return
		}

		pushInfo.lastEvent = eventMultistreamConnected
		pushInfo.lastEventAt = time.Now()
		if waitForPushEventWasIncreased {
			// Reset error count, because most probably the multistream target started to work
			pushInfo.lastEventErrorCount = 0
		}
		mc.emitWebhookEventAsync(info.stream, pushInfo, eventMultistreamConnected)
	}
}

func (mc *mac) emitMultistreamDisconnectedEvent(stream *streamInfo, status *pushStatus) {
	status.mu.Lock()
	defer status.mu.Unlock()
	status.lastEvent = eventMultistreamDisconnected
	status.lastEventAt = time.Now()
	mc.emitWebhookEventAsync(stream.stream, status, eventMultistreamDisconnected)

}

func (mc *mac) emitStreamStateEvent(stream *api.Stream, state data.StreamState) {
	streamID := stream.ParentID
	if streamID == "" {
		streamID = stream.ID
	}
	stateEvt := data.NewStreamStateEvent(mc.nodeID, mc.ownRegion, stream.UserID, streamID, state)
	mc.emitAmqpEvent(ownExchangeName, "stream.state."+streamID, stateEvt)
}

func (mc *mac) emitWebhookEventAsync(stream *api.Stream, pushInfo *pushStatus, eventKey string) {
	go func() {
		streamID, sessionID := stream.ParentID, stream.ID
		if streamID == "" {
			streamID = sessionID
		}
		payload := data.MultistreamWebhookPayload{
			Target: pushToMultistreamTargetInfo(pushInfo),
		}
		hookEvt, err := data.NewWebhookEvent(streamID, eventKey, stream.UserID, sessionID, payload)

		if err != nil {
			glog.Errorf("Error creating webhook event err=%v", err)
			return
		}
		mc.emitAmqpEvent(webhooksExchangeName, "events."+eventKey, hookEvt)
	}()
}

func (mc *mac) emitAmqpEvent(exchange, key string, evt data.Event) {
	if mc.producer == nil {
		return
	}
	glog.Infof("Publishing amqp message to exchange=%s key=%s msg=%+v", exchange, key, evt)

	ctx, cancel := context.WithTimeout(mc.ctx, 3*time.Second)
	defer cancel()
	err := mc.producer.Publish(ctx, event.AMQPMessage{
		Exchange:   exchange,
		Key:        key,
		Body:       evt,
		Persistent: true,
	})
	if err != nil {
		glog.Errorf("Error publishing amqp message to exchange=%s key=%s, err=%v", exchange, key, err)
	}
}

func (mc *mac) handlePushEnd(ctx context.Context, payload *misttriggers.PushEndPayload) error {
	go func() {
		playbackID := mistStreamName2playbackID(payload.StreamName)
		if info, ok := mc.getStreamInfoLogged(playbackID); ok {
			info.mu.Lock()
			defer info.mu.Unlock()
			if info.stopped {
				// stream stopped, no need to send any events
				return
			}
			if pushInfo, ok := info.pushStatus[payload.Destination]; ok {
				pushInfo.mu.Lock()
				defer pushInfo.mu.Unlock()
				pushInfo.lastEventAt = time.Now()
				switch pushInfo.lastEvent {
				case eventMultistreamDisconnected:
					// push was disconnected, reset the lastEvent state
					pushInfo.lastEvent = ""
					pushInfo.lastEventErrorCount = 0
				default:
					pushInfo.lastEvent = eventMultistreamError
					if pushInfo.lastEventErrorCount <= eventMultistreamErrorTolerance {
						pushInfo.lastEventErrorCount++
					}
					if pushInfo.lastEventErrorCount == eventMultistreamErrorTolerance {
						// Trigger error event only after a few errors
						mc.emitWebhookEventAsync(info.stream, pushInfo, eventMultistreamError)
					}
				}
			} else {
				glog.Errorf("For stream playbackID=%s got unknown RTMP push %s", playbackID, payload.StreamName)
			}
		}
	}()
	return nil
}

func (mc *mac) removeInfoDelayed(playbackID string, done chan struct{}) {
	go func() {
		select {
		case <-done:
			return
		case <-time.After(keepStreamAfterEnd):
			mc.removeInfo(playbackID)
		}
	}()
}

func (mc *mac) removeInfo(playbackID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.removeInfoLocked(playbackID)
}

// must be called inside mc.mu.Lock
func (mc *mac) removeInfoLocked(playbackID string) {
	if info, ok := mc.streamInfo[playbackID]; ok {
		close(info.done)
		delete(mc.streamInfo, playbackID)
	}
}

func (mc *mac) wildcardPlaybackID(stream *api.Stream) string {
	return mc.baseStreamName + "+" + stream.PlaybackID
}

// reconcileLoop calls reconcileStream, reconcileMultistream and processStats
// periodically or when streamUpdated is triggered on demand (from serf event).
func (mc *mac) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-mc.streamUpdated:
		}
		mistState, err := mc.mist.GetState()
		if err != nil {
			glog.Errorf("error executing query on Mist, cannot reconcile err=%v", err)
			continue
		}
		mc.reconcileStreams(mistState)
		mc.reconcileMultistream(mistState)
		mc.processStats(mistState)
	}
}

func (mc *mac) reconcileStreams(mistState clients.MistState) {
	for streamName, _ := range mistState.ActiveStreams {
		if !mistState.IsIngestStream(streamName) {
			continue
		}

		si, err := mc.getStreamInfo(streamName)
		if err != nil {
			glog.Errorf("error getting stream info streamName=%s err=%v", streamName, err)
			continue
		}

		mc.reconcileSingleStream(si)
	}
}

func (mc *mac) reconcileSingleStream(si *streamInfo) {
	shouldNuke := si.stream.Deleted || si.stream.Suspended
	if !shouldNuke {
		// the only thing we do here is nuke
		return
	}

	// make sure we nuke any possible stream names on mist to account for any inconsistencies
	mc.nukeAllStreamNames(si.stream.PlaybackID)
}

func (mc *mac) nukeAllStreamNames(playbackID string) {
	streamNames := []string{
		mc.wildcardPlaybackID(&api.Stream{PlaybackID: playbackID}),               // not recorded
		mc.wildcardPlaybackID(&api.Stream{PlaybackID: playbackID, Record: true}), // recorded
	}

	for _, streamName := range streamNames {
		err := mc.mist.NukeStream(streamName)
		if err != nil {
			glog.Errorf("error nuking stream playbackId=%s streamName=%s err=%q", playbackID, streamName, err)
		}
	}
}

func (mc *mac) invalidateAllSessions(playbackID string) {
	streamNames := []string{
		"video+" + playbackID,
	}

	for _, streamName := range streamNames {
		err := mc.mist.InvalidateSessions(streamName)
		if err != nil {
			glog.Errorf("error invalidating sessions playbackId=%s streamName=%s err=%q", playbackID, streamName, err)
		}
	}
}

// reconcileMultistream makes sure that Mist contains the multistream pushes exactly as specified in streamInfo cache.
// There may be multiple reasons why Mist is not in sync with streamInfo cache:
// - streamInfo cache has changed (multistream target was turned on/off or multistream target was added/removed)
// - Mist removed its push for some reason
// Note that we use Mist AUTO_PUSH (which in turn makes sure that the PUSH is always available).
// Note also that we only create AUTO_PUSH for active streams which are ingest (not playback).
func (mc *mac) reconcileMultistream(mistState clients.MistState) {
	glog.Warningf("### Reconciling Multistreams, mistState=%v", mistState)
	type key struct {
		stream string
		target string
	}
	toKey := func(stream, target string) key {
		return key{stream: stream, target: target}
	}
	isMultistream := func(k key) bool {
		acceptedTargetPrefixes := []string{"rtmp:", "rtmps:", "srt:"}
		if strings.HasPrefix(k.stream, "video+") || strings.HasPrefix(k.stream, "videorec+") {
			for _, acceptedTargetPrefix := range acceptedTargetPrefixes {
				if strings.HasPrefix(strings.ToLower(k.target), acceptedTargetPrefix) {
					return true
				}
			}
		}
		return false
	}

	// Get the existing PUSH_AUTO from Mist
	var filteredMistPushAutoList []*clients.MistPushAuto
	mistMap := map[key]bool{}
	for _, e := range mistState.PushAutoList {
		k := toKey(e.Stream, e.Target)
		if isMultistream(k) {
			filteredMistPushAutoList = append(filteredMistPushAutoList, e)
			mistMap[toKey(e.Stream, e.Target)] = true
		}
	}
	glog.Warningf("### mistMap=%v", mistMap)
	glog.Warningf("### filteredMistPushAutoList=%v", filteredMistPushAutoList)

	// Get the existing PUSH from Mist
	var filteredMistPushList []*clients.MistPush
	for _, e := range mistState.PushList {
		k := toKey(e.Stream, e.OriginalURL)
		if isMultistream(k) {
			filteredMistPushList = append(filteredMistPushList, e)
		}
	}
	glog.Warningf("### mistMap=%v", mistMap)

	// Get the expected multistreams from cached streamInfo
	type pushInfo struct {
		status  *pushStatus
		stream  *streamInfo
		enabled bool
	}
	cachedMap := map[key]*pushInfo{}
	mc.mu.Lock()
	for _, si := range mc.streamInfo {
		for target, v := range si.pushStatus {
			if v.target != nil {
				stream := mc.wildcardPlaybackID(si.stream)
				if isIngestStream(stream, si, mistState) {
					cachedMap[toKey(stream, target)] = &pushInfo{status: v, stream: si, enabled: !v.target.Disabled}
				}
			}
		}
	}
	mc.mu.Unlock()
	glog.Warningf("### cachedMap=%v", cachedMap)

	// Remove AUTO_PUSH that exists in Mist, but is not in streamInfo cache
	for _, e := range filteredMistPushAutoList {
		pi, exist := cachedMap[toKey(e.Stream, e.Target)]
		if !exist || !pi.enabled {
			if pi != nil {
				mc.emitMultistreamDisconnectedEvent(pi.stream, pi.status)
			}
			glog.Infof("removing AUTO_PUSH for stream=%s target=%s", e.Stream, e.Target)
			if err := mc.mist.PushAutoRemove(e.StreamParams); err != nil {
				glog.Errorf("cannot remove AUTO_PUSH for stream=%s target=%s err=%v", e.Stream, e.Target, err)
			}
		}
	}

	// Remove PUSH that exists in Mist, but is not in streamInfo cache
	// Deleted AUTO_PUSH does not automatically delete the related PUSH
	for _, e := range filteredMistPushList {
		pi, exist := cachedMap[toKey(e.Stream, e.OriginalURL)]
		if !exist || !pi.enabled {
			glog.Infof("stopping PUSH for stream=%s target=%s id=%d", e.Stream, e.OriginalURL, e.ID)
			if err := mc.mist.PushStop(e.ID); err != nil {
				glog.Errorf("cannot stop PUSH for stream=%s target=%s id=%d err=%v", e.Stream, e.OriginalURL, e.ID, err)
			}
		}
	}

	// Add AUTO_PUSH that exists streamInfo cache, but not in Mist
	for k, v := range cachedMap {
		glog.Warningf("### k=%v v=%v", k, v)
		glog.Warningf("### v.enabled=%v mistMap[toKey(k.stream, k.target)]=%v", v.enabled, mistMap[toKey(k.stream, k.target)])
		if v.enabled && !mistMap[toKey(k.stream, k.target)] {
			glog.Infof("adding AUTO_PUSH for stream=%s target=%s", k.stream, k.target)
			if err := mc.mist.PushAutoAdd(k.stream, k.target); err != nil {
				glog.Errorf("cannot add AUTO_PUSH for stream=%s target=%s err=%v", k.stream, k.target, err)
			}
		}
	}
}

func (mc *mac) processStats(mistState clients.MistState) {
	if mc.metricsCollector != nil {
		mc.metricsCollector.collectMetricsLogged(mc.ctx, 60*time.Second, mistState)
	}
}

func (mc *mac) getPushUrl(stream *api.Stream, targetRef *api.MultistreamTargetRef) (*api.MultistreamTarget, string, error) {
	target, err := mc.lapi.GetMultistreamTarget(targetRef.ID)
	if err != nil {
		return nil, "", fmt.Errorf("error fetching multistream target %s: %w", targetRef.ID, err)
	}
	// Find the actual parameters of the profile we're using
	var videoSelector string
	// Not actually the source. But the highest quality.
	if targetRef.Profile == "source" {
		videoSelector = "maxbps"
	} else {
		var prof *api.Profile
		for _, p := range stream.Profiles {
			if p.Name == targetRef.Profile {
				prof = &p
				break
			}
		}
		if prof == nil {
			return nil, "", fmt.Errorf("profile not found: %s", targetRef.Profile)
		}
		videoSelector = fmt.Sprintf("~%dx%d", prof.Width, prof.Height)
	}
	audioSelector := "maxbps"
	if targetRef.VideoOnly {
		audioSelector = "silent"
	}
	// Inject ?video=~widthxheight to send the correct rendition. Notice that we don't care if there is already a
	// query-string in the URL since Mist will always strip from the last `?` in the push URL for its configs.
	return target, fmt.Sprintf("%s?video=%s&audio=%s", target.URL, videoSelector, audioSelector), nil
}

func (mc *mac) getStreamInfoLogged(playbackID string) (*streamInfo, bool) {
	info, err := mc.getStreamInfo(playbackID)
	if err != nil {
		glog.Errorf("Error getting stream info playbackID=%q err=%q", playbackID, err)
		return nil, false
	}
	return info, true
}

func (mc *mac) getStreamInfo(playbackID string) (*streamInfo, error) {
	playbackID = mistStreamName2playbackID(playbackID)

	mc.mu.RLock()
	info := mc.streamInfo[playbackID]
	mc.mu.RUnlock()

	if info == nil {
		return mc.refreshStreamInfo(playbackID)
	}
	return info, nil
}

func (mc *mac) refreshStreamInfo(playbackID string) (*streamInfo, error) {
	glog.Infof("Refreshing stream info for playbackID=%s", playbackID)

	stream, err := mc.lapiCached.GetStreamByPlaybackID(playbackID)
	if err != nil {
		return nil, fmt.Errorf("error getting stream by playback ID %s: %w", playbackID, err)
	}

	newPushes := make(map[string]*pushStatus)
	for _, ref := range stream.Multistream.Targets {
		target, pushURL, err := mc.getPushUrl(stream, &ref)
		if err != nil {
			glog.Errorf("error fetching multistream target, err=%v", err)
			continue
		}
		newPushes[pushURL] = &pushStatus{
			target:  target,
			profile: ref.Profile,
			metrics: &data.MultistreamMetrics{},
		}
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	info, exists := mc.streamInfo[playbackID]
	if !exists {
		info = &streamInfo{
			id:         stream.ID,
			stream:     stream,
			isLazy:     true, // flag it as a lazy stream info to avoid sending metrics
			done:       make(chan struct{}),
			pushStatus: newPushes,
		}
		mc.streamInfo[playbackID] = info
	} else {
		info.id = stream.ID
		info.stream = stream
	}
	info.mu.Lock()
	defer info.mu.Unlock()

	for newPushURL, newPush := range newPushes {
		if push, exists := info.pushStatus[newPushURL]; exists {
			push.mu.Lock()
			push.target = newPush.target
			push.profile = newPush.profile
			push.mu.Unlock()
		} else {
			info.pushStatus[newPushURL] = newPush
		}
	}
	for oldPushURL := range info.pushStatus {
		if _, exists := newPushes[oldPushURL]; !exists {
			delete(info.pushStatus, oldPushURL)
		}
	}

	glog.Infof("Refreshed stream info for playbackID=%s id=%s numPushes=%d", playbackID, stream.ID, len(info.pushStatus))

	return info, nil
}

func (mc *mac) GetCachedStream(playbackID string) *api.Stream {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if si, ok := mc.streamInfo[playbackID]; ok {
		si.mu.Lock()
		defer si.mu.Unlock()
		return si.stream
	}
	return nil
}

func mistStreamName2playbackID(msn string) string {
	if strings.Contains(msn, "+") {
		return strings.Split(msn, "+")[1]
	}
	return msn
}

func pushToMultistreamTargetInfo(pushInfo *pushStatus) data.MultistreamTargetInfo {
	pushInfo.mu.Lock()
	defer pushInfo.mu.Unlock()
	return data.MultistreamTargetInfo{
		ID:      pushInfo.target.ID,
		Name:    pushInfo.target.Name,
		Profile: pushInfo.profile,
	}
}

// isIngestStream checks if the given stream is ingest (push) as opposed to the playback (pull) stream.
// We do need to check it in both streamInfo and MistState, because:
//   - streamInfo: isLazy is set to false in the PUSH_REWRITE trigger, which is present only for ingest streams; checking
//     this condition only is not good enough, because catalyst-api might be restarted and have isLazy set to true for
//     the ingest stream
//   - MistState: active_streams have source "push://" for ingest streams; checking this condition only is not good
//     enough, because a freshly started stream may not be yet visible in Mist (though it's already started).
func isIngestStream(stream string, si *streamInfo, mistState clients.MistState) bool {
	return (si != nil && !si.isLazy) || mistState.IsIngestStream(stream)
}

package pipeline

import (
	"crypto/rsa"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	_ "github.com/lib/pq"
	"github.com/livepeer/catalyst-api/c2pa"
	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/crypto"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/catalyst-api/video"
)

// Strategy indicates how the pipelines should be coordinated. Mainly changes
// which pipelines to execute, in what order, and which ones go in background.
// Background pipelines are only logged and are not reported back to the client.
type Strategy string

const (
	// Only execute the external pipeline
	StrategyExternalDominance Strategy = "external"
	// Only execute the FFMPEG / Livepeer pipeline
	StrategyCatalystFfmpegDominance Strategy = "catalyst_ffmpeg"
	// Execute the FFMPEG pipeline first and fallback to the external transcoding
	// provider on errors.
	StrategyFallbackExternal Strategy = "fallback_external"
	// Execute the external transcoder pipeline in foreground and FFMPEG / Livepeer in background.
	StrategyBackgroundFFMpeg Strategy = "background_ffmpeg"
	// Only mp4s of maxMP4OutDuration will have MP4s generated for each rendition
	maxMP4OutDuration          = 2 * time.Minute
	maxRecordingMP4Duration    = 12 * time.Hour
	maxRecordingThumbsDuration = maxRecordingMP4Duration
)

func (s Strategy) IsValid() bool {
	switch s {
	case StrategyExternalDominance, StrategyCatalystFfmpegDominance, StrategyFallbackExternal, StrategyBackgroundFFMpeg:
		return true
	default:
		return false
	}
}

// UploadJobPayload is the required payload to start an upload job.
type UploadJobPayload struct {
	SourceFile            string
	CallbackURL           string
	HlsTargetURL          *url.URL
	Mp4TargetURL          *url.URL
	FragMp4TargetURL      *url.URL
	ClipTargetURL         *url.URL
	ThumbnailsTargetURL   *url.URL
	Mp4OnlyShort          bool
	AccessToken           string
	TranscodeAPIUrl       string
	HardcodedBroadcasters string
	RequestID             string
	ExternalID            string
	Profiles              []video.EncodedProfile
	PipelineStrategy      Strategy
	TargetSegmentSizeSecs int64
	GenerateMP4           bool
	Encryption            *EncryptionPayload
	InputFileInfo         video.InputVideo
	SourceCopy            bool
	ClipStrategy          video.ClipStrategy
	C2PA                  bool
}

type EncryptionPayload struct {
	EncryptedKey string `json:"encrypted_key"`
}

// UploadJobResult is the object returned by the successful execution of an
// upload job.
type UploadJobResult struct {
	InputVideo video.InputVideo
	Outputs    []video.OutputVideo
}

// JobInfo represents the state of a single upload job.
type JobInfo struct {
	mu sync.Mutex
	UploadJobPayload
	PipelineInfo
	StreamName string
	// this is only set&used internally in the mist pipeline
	SegmentingTargetURL string

	statusClient clients.TranscodeStatusClient

	SourcePlaybackDone time.Time
	DownloadDone       time.Time
	SegmentingDone     time.Time
	TranscodingDone    time.Time

	sourceBytes             int64
	sourceSegments          int
	sourceDurationMs        int64
	sourceCodecVideo        string
	sourceCodecAudio        string
	sourceWidth             int64
	sourceHeight            int64
	sourceFPS               float64
	sourceBitrateVideo      int64
	sourceBitrateAudio      int64
	sourceChannels          int
	sourceSampleRate        int
	sourceSampleBits        int
	sourceVideoStartTimeSec float64
	sourceAudioStartTimeSec float64

	targetSegmentSizeSecs int64
	catalystRegion        string
	numProfiles           int
	inFallbackMode        bool
	SignedSourceURL       string
	LivepeerSupported     bool
	C2PA                  *c2pa.C2PA
}

// PipelineInfo represents the state of an individual pipeline, i.e. ffmpeg or mediaconvert
// These fields have been split out from JobInfo to ensure that they are zeroed out in startOneUploadJob() when a fallback pipeline runs
type PipelineInfo struct {
	startTime          time.Time
	result             chan bool
	handler            Handler
	hasFallback        bool
	transcodedSegments int
	pipeline           string
	state              string
}

func (j *JobInfo) ReportProgress(stage clients.TranscodeStatus, completionRatio float64) {
	tsm := clients.NewTranscodeStatusProgress(j.CallbackURL, j.RequestID, stage, completionRatio)
	// Ignore errors, send the progress next time
	_ = j.statusClient.SendTranscodeStatus(tsm)
}

func ClippingRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(5*time.Second), 10)
}

// Coordinator provides the main interface to handle the pipelines. It should be
// called directly from the API handlers and never blocks on execution, but
// rather schedules routines to do the actual work in background.
type Coordinator struct {
	strategy     Strategy
	statusClient clients.TranscodeStatusClient

	pipeFfmpeg, pipeExternal Handler

	Jobs                 *cache.Cache[*JobInfo]
	MetricsDB            *sql.DB
	InputCopy            clients.InputCopier
	VodDecryptPrivateKey *rsa.PrivateKey
	SourceOutputURL      *url.URL
	C2PA                 *c2pa.C2PA
}

func NewCoordinator(strategy Strategy, sourceOutputURL, extTranscoderURL string, statusClient clients.TranscodeStatusClient, metricsDB *sql.DB, VodDecryptPrivateKey *rsa.PrivateKey, broadcasterURL string, sourcePlaybackHosts map[string]string, c2pa *c2pa.C2PA) (*Coordinator, error) {
	if !strategy.IsValid() {
		return nil, fmt.Errorf("invalid strategy: %s", strategy)
	}

	var extTranscoder clients.TranscodeProvider
	if extTranscoderURL != "" {
		var err error
		extTranscoder, err = clients.ParseTranscodeProviderURL(extTranscoderURL)
		if err != nil {
			return nil, fmt.Errorf("error creating external transcoder: %v", err)
		}
	}
	if strategy != StrategyCatalystFfmpegDominance && extTranscoder == nil {
		return nil, fmt.Errorf("external transcoder is required for strategy: %v", strategy)
	}
	sourceOutput, err := url.Parse(sourceOutputURL)
	if err != nil {
		return nil, fmt.Errorf("cannot create sourceOutputUrl: %w", err)
	}
	broadcaster, err := clients.NewLocalBroadcasterClient(broadcasterURL)
	if err != nil {
		return nil, fmt.Errorf("cannot initalilze local broadcaster: %w", err)
	}

	return &Coordinator{
		strategy:     strategy,
		statusClient: statusClient,
		pipeFfmpeg: &ffmpeg{
			SourceOutputURL:     sourceOutput,
			Broadcaster:         broadcaster,
			probe:               video.Probe{},
			sourcePlaybackHosts: sourcePlaybackHosts,
		},
		pipeExternal:         &external{extTranscoder},
		Jobs:                 cache.New[*JobInfo](),
		MetricsDB:            metricsDB,
		InputCopy:            clients.NewInputCopy(),
		VodDecryptPrivateKey: VodDecryptPrivateKey,
		SourceOutputURL:      sourceOutput,
		C2PA:                 c2pa,
	}, nil
}

func NewStubCoordinator() *Coordinator {
	return NewStubCoordinatorOpts(StrategyCatalystFfmpegDominance, nil, nil, nil)
}

func NewStubCoordinatorOpts(strategy Strategy, statusClient clients.TranscodeStatusClient, pipeFfmpeg, pipeExternal Handler) *Coordinator {
	if strategy == "" {
		strategy = StrategyCatalystFfmpegDominance
	}
	if statusClient == nil {
		statusClient = clients.TranscodeStatusFunc(func(tsm clients.TranscodeStatusMessage) error { return nil })
	}
	if pipeFfmpeg == nil {
		pipeFfmpeg = &ffmpeg{SourceOutputURL: &url.URL{}, probe: video.Probe{}}
	}
	if pipeExternal == nil {
		pipeExternal = &external{}
	}
	return &Coordinator{
		strategy:     strategy,
		statusClient: statusClient,
		pipeFfmpeg:   pipeFfmpeg,
		pipeExternal: pipeExternal,
		Jobs:         cache.New[*JobInfo](),
		InputCopy: &clients.InputCopy{
			Probe: video.Probe{},
		},
		SourceOutputURL: &url.URL{},
	}
}

// Starts a new upload job.
//
// This has the main logic regarding the pipeline strategy. It starts jobs and
// handles processing the response and triggering a fallback if appropriate.
func (c *Coordinator) StartUploadJob(p UploadJobPayload) {
	streamName := config.SegmentingStreamName(p.RequestID)
	log.AddContext(p.RequestID, "stream_name", streamName)
	si := &JobInfo{
		UploadJobPayload: p,
		statusClient:     c.statusClient,
		StreamName:       streamName,

		numProfiles:    len(p.Profiles),
		catalystRegion: os.Getenv("MY_REGION"),
		PipelineInfo: PipelineInfo{
			startTime: time.Now(),
			state:     "segmenting",
		},
	}
	si.ReportProgress(clients.TranscodeStatusPreparing, 0)
	c.Jobs.Store(streamName, si)
	log.Log(si.RequestID, "Wrote to jobs cache")
	metrics.Metrics.JobsInFlight.Set(float64(len(c.Jobs.GetKeys())))

	c.runHandlerAsync(si, func() (*HandlerOutput, error) {
		sourceURL, err := url.Parse(p.SourceFile)
		if err != nil {
			return nil, fmt.Errorf("error parsing source as url: %w", err)
		}

		var decryptor *crypto.DecryptionKeys

		if p.Encryption != nil {
			decryptor = &crypto.DecryptionKeys{
				DecryptKey:   c.VodDecryptPrivateKey,
				EncryptedKey: p.Encryption.EncryptedKey,
			}
		}

		osTransferURL := c.SourceOutputURL.JoinPath(p.RequestID, "transfer", path.Base(sourceURL.Path))
		originalSource := sourceURL

		// Update osTransferURL if needed
		if clients.IsHLSInput(sourceURL) {
			// Interim Fix: Don't try and use the backup for clipping - this causes a big slowness where we
			// hang for a long time inside this method. Root cause needs to be established, but current thinking is that
			// it happens when there's a very large input manifest
			if !p.ClipStrategy.Enabled {
				// Handle falling back to backup bucket for manifest and segments
				sourceURL, err = clients.RecordingBackupCheck(p.RequestID, sourceURL, osTransferURL.JoinPath(".."))
				if err != nil {
					return nil, err
				}
			}

			// Currently we only clip an HLS source (e.g recordings or transcoded asset)
			if p.ClipStrategy.Enabled {
				err := backoff.Retry(func() error {
					log.Log(p.RequestID, "clippity clipping the input", "Playback-ID", p.ClipStrategy.PlaybackID)
					// Use new clipped manifest as the source URL
					clipSourceURL, err := clients.ClipInputManifest(p.RequestID, sourceURL.String(), p.ClipTargetURL.String(), p.ClipStrategy.StartTime, p.ClipStrategy.EndTime)
					if err != nil {
						return fmt.Errorf("clipping failed: %s %w", sourceURL.Redacted(), err)
					}
					sourceURL = clipSourceURL
					return nil
				}, ClippingRetryBackoff())
				if err != nil {
					return nil, err
				}
			}
			// Use the source URL location as the transfer directory to hold the clipped outputs
			osTransferURL = sourceURL
		} else if p.SourceCopy {
			log.Log(p.RequestID, "source copy enabled")
			osTransferURL = p.HlsTargetURL.JoinPath("video")
		}

		inputVideoProbe, signedNewSourceURL, err := c.InputCopy.CopyInputToS3(p.RequestID, sourceURL, osTransferURL, decryptor)
		if err != nil {
			return nil, fmt.Errorf("error copying input to storage: %w", err)
		}

		checkClipResolution(p, &inputVideoProbe, originalSource)

		if p.C2PA {
			si.C2PA = c.C2PA
		}
		si.SourceFile = osTransferURL.String() // OS URL used by ffmpeg pipeline
		log.AddContext(p.RequestID, "new_source_url", si.SourceFile)

		si.SignedSourceURL = signedNewSourceURL // http(s) URL used by mediaconvert
		log.AddContext(p.RequestID, "signed_url", si.SignedSourceURL)

		si.InputFileInfo = inputVideoProbe

		shouldGenerateMP4, reason := ShouldGenerateMP4(sourceURL, p.Mp4TargetURL, p.FragMp4TargetURL, p.Mp4OnlyShort, si.InputFileInfo.Duration)
		log.Log(si.RequestID, "Deciding whether to generate MP4s", "should_generate", shouldGenerateMP4, "duration", si.InputFileInfo.Duration, "reason", reason)
		si.GenerateMP4 = shouldGenerateMP4

		si.DownloadDone = time.Now()

		c.startUploadJob(si)
		return nil, nil
	})
}

func checkClipResolution(p UploadJobPayload, inputVideoProbe *video.InputVideo, originalSource *url.URL) {
	// HACK: sometimes probing the clip manifest results in zero height and width, probe the original manifest instead to get this info
	if !p.ClipStrategy.Enabled {
		return
	}
	if inputVideoProbe == nil {
		log.Log(p.RequestID, "checkClipResolution inputVideoProbe was nil")
		return
	}
	videoTrack, err := inputVideoProbe.GetTrack(video.TrackTypeVideo)
	if err != nil {
		log.LogError(p.RequestID, "checkClipResolution error getting video track", err)
		return
	}
	if videoTrack.Height > 0 && videoTrack.Width > 0 {
		return
	}

	iv, err := video.Probe{IgnoreErrMessages: clients.IgnoreProbeErrs}.ProbeFile(p.RequestID, originalSource.String())
	if err != nil {
		log.LogError(p.RequestID, "checkClipResolution probe error", err)
		return
	}
	newTrack, err := iv.GetTrack(video.TrackTypeVideo)
	if err != nil {
		log.LogError(p.RequestID, "checkClipResolution error getting video track", err)
		return
	}
	videoTrack.VideoTrack = newTrack.VideoTrack
	err = inputVideoProbe.SetTrack(video.TrackTypeVideo, videoTrack)
	if err != nil {
		log.LogError(p.RequestID, "checkClipResolution error setting video track", err)
		return
	}
}

func ShouldGenerateMP4(sourceURL, mp4TargetUrl *url.URL, fragMp4TargetUrl *url.URL, mp4OnlyShort bool, durationSecs float64) (bool, string) {
	// Skip mp4 generation if we weren't able to determine the duration of the input file for any reason
	if durationSecs == 0.0 {
		return false, "duration is missing or zero"
	}
	// We're currently memory-bound for generating MP4s above a certain file size
	// This has been hitting us for long recordings, so do a crude "is it longer than 12 hours?" check and skip the MP4 if it is
	if clients.IsHLSInput(sourceURL) && durationSecs > maxRecordingMP4Duration.Seconds() {
		return false, "recording duration is too long"
	}

	if mp4TargetUrl != nil && (!mp4OnlyShort || durationSecs <= maxMP4OutDuration.Seconds()) {
		return true, ""
	}

	if fragMp4TargetUrl == nil {
		return false, "missing MP4 target URL"
	}

	return true, ""
}

func (c *Coordinator) startUploadJob(p *JobInfo) {
	strategy := c.strategy
	if p.PipelineStrategy.IsValid() {
		strategy = p.PipelineStrategy
	}
	p.LivepeerSupported, strategy = checkLivepeerCompatible(p.RequestID, strategy, p.InputFileInfo)
	log.AddContext(p.RequestID, "strategy", strategy)
	log.Log(p.RequestID, "Starting upload job")

	switch strategy {
	case StrategyExternalDominance:
		c.startOneUploadJob(p, c.pipeExternal, true, false)
	case StrategyCatalystFfmpegDominance:
		c.startOneUploadJob(p, c.pipeFfmpeg, true, false)
	case StrategyBackgroundFFMpeg:
		c.startOneUploadJob(p, c.pipeExternal, true, false)
		c.startOneUploadJob(p, c.pipeFfmpeg, false, false)
	case StrategyFallbackExternal:
		// nolint:errcheck
		go recovered(func() (t bool, e error) {
			success := <-c.startOneUploadJob(p, c.pipeFfmpeg, true, true)
			if !success {
				p.inFallbackMode = true
				log.Log(p.RequestID, "Entering fallback pipeline")
				c.startOneUploadJob(p, c.pipeExternal, true, false)
			}
			return
		})
	}
}

// checkLivepeerCompatible checks if the input codecs are compatible with our Livepeer pipeline and overrides the pipeline strategy
// to external if they are incompatible
func checkLivepeerCompatible(requestID string, strategy Strategy, iv video.InputVideo) (bool, Strategy) {
	if _, err := iv.GetTrack(video.TrackTypeVideo); err != nil {
		log.Log(requestID, "audio-only inputs not supported by Livepeer pipeline")
		return livepeerNotSupported(strategy)
	}

	for _, track := range iv.Tracks {
		// If the video codec is not compatible then override to external pipeline to avoid sending to Livepeer
		// We always covert the audio to AAC before sending for transcoding, so don't need to check this
		if track.Type == video.TrackTypeVideo {
			if strings.ToLower(track.Codec) == "hevc" && strategy != StrategyCatalystFfmpegDominance {
				strategy = StrategyBackgroundFFMpeg
			} else if strings.ToLower(track.Codec) != "h264" {
				log.Log(requestID, "codec not supported by Livepeer pipeline", "trackType", track.Type, "codec", track.Codec)
				return livepeerNotSupported(strategy)
			}
		}
		if track.Type == video.TrackTypeVideo && track.Rotation != 0 {
			log.Log(requestID, "video rotation not supported by Livepeer pipeline", "rotation", track.Rotation)
			return livepeerNotSupported(strategy)
		}
		if !checkDisplayAspectRatio(track, requestID) {
			return livepeerNotSupported(strategy)
		}
	}
	return true, strategy
}

func livepeerNotSupported(strategy Strategy) (bool, Strategy) {
	// Allow "dominance" strategies to pass through as these are used in tests and we might want to manually force them for debugging
	if strategy == StrategyCatalystFfmpegDominance {
		return false, strategy
	}
	return false, StrategyExternalDominance
}

func checkDisplayAspectRatio(track video.InputTrack, requestID string) bool {
	if track.Type != video.TrackTypeVideo || track.DisplayAspectRatio == "" {
		return true
	}
	aspectRatioParts := strings.Split(track.DisplayAspectRatio, ":")
	if len(aspectRatioParts) != 2 {
		return true
	}
	w, err := strconv.ParseFloat(aspectRatioParts[0], 64)
	h, err2 := strconv.ParseFloat(aspectRatioParts[1], 64)
	if err != nil || err2 != nil {
		log.Log(requestID, "failed to parse floats when checking display aspect ratio", "err", err, "err2", err2)
		return true
	}
	if h == 0 || track.Height == 0 {
		log.Log(requestID, "height was zero when checking display aspect ratio", "ratioHeight", h, "trackHeight", track.Height)
		return true
	}
	dar := w / h
	resRatio := float64(track.Width) / float64(track.Height)

	// calculate the difference between the aspect ratio and the real ratio of the resolution, allow up to 20%
	diff := math.Abs(dar - resRatio)
	if (diff / dar) < 0.2 {
		return true
	}
	log.Log(requestID, "display aspect ratio doesn't match resolution, not supported by Livepeer pipeline", "display_aspect_ratio", track.DisplayAspectRatio, "width", track.Width, "height", track.Height)
	return false
}

// Starts a single upload job with specified pipeline Handler. If the job is
// running in background (foreground=false) then:
//   - the job will have a different requestID
//   - no transcode status updates will be reported to the caller, only logged
//   - the output will go to a different location than the real job
//
// The `hasFallback` argument means the caller has a special logic to handle
// failures (today this means re-running the job in another pipeline). If it's
// set to true, error callbacks from this job will not be sent.
func (c *Coordinator) startOneUploadJob(si *JobInfo, handler Handler, foreground, hasFallback bool) <-chan bool {
	if !foreground {
		// TODO only do if output location is livepeer, i.e. don't do for transcode api jobs as we'd be writing unexpected files to customer storage

		// replace si with a copy of itself to avoid interfering with the foreground job
		requestID := fmt.Sprintf("bg_%s", si.RequestID)
		si = &JobInfo{
			UploadJobPayload: si.UploadJobPayload,
			StreamName:       config.SegmentingStreamName(requestID),
			statusClient:     c.statusClient,

			numProfiles:           len(si.UploadJobPayload.Profiles),
			targetSegmentSizeSecs: si.UploadJobPayload.TargetSegmentSizeSecs,
			catalystRegion:        os.Getenv("MY_REGION"),

			DownloadDone: time.Now(),
		}
		si.RequestID = requestID

		if si.HlsTargetURL != nil {
			si.HlsTargetURL = si.HlsTargetURL.JoinPath("..", handler.Name(), path.Base(si.HlsTargetURL.Path))
		}
		// TODO set other target URLs to nil for now

		// this will prevent the callbacks for this job from actually being sent
		si.CallbackURL = ""
	}
	log.AddContext(si.RequestID, "handler", handler.Name())

	var pipeline = handler.Name()
	if pipeline == "external" {
		pipeline = "aws-mediaconvert"
	}

	// Codecs are parsed here primarily to write codec stats for each job
	// Start Time of a/v track is parsed here to detect out-of-sync segments for recordings
	var videoCodec, audioCodec string
	var videoStartTimeSec, audioStartTimeSec float64
	videoTrack, err := si.InputFileInfo.GetTrack(video.TrackTypeVideo)
	if err != nil {
		videoCodec = "n/a"
		videoStartTimeSec = -1
	} else {
		videoCodec = videoTrack.Codec
		videoStartTimeSec = videoTrack.StartTimeSec
	}
	audioTrack, err := si.InputFileInfo.GetTrack(video.TrackTypeAudio)
	if err != nil {
		audioCodec = "n/a"
		audioStartTimeSec = -1
	} else {
		audioCodec = audioTrack.Codec
		audioStartTimeSec = audioTrack.StartTimeSec
	}

	si.PipelineInfo = PipelineInfo{
		startTime:          time.Now(),
		result:             make(chan bool, 1),
		handler:            handler,
		hasFallback:        hasFallback,
		transcodedSegments: 0,
		pipeline:           pipeline,
		state:              "segmenting",
	}

	si.targetSegmentSizeSecs = si.TargetSegmentSizeSecs
	si.sourceBytes = si.InputFileInfo.SizeBytes
	si.sourceDurationMs = int64(math.Round(si.InputFileInfo.Duration) * 1000)
	si.sourceCodecVideo = videoCodec
	si.sourceCodecAudio = audioCodec
	si.sourceWidth = videoTrack.Width
	si.sourceHeight = videoTrack.Height
	si.sourceFPS = videoTrack.FPS
	si.sourceBitrateVideo = videoTrack.Bitrate
	si.sourceBitrateAudio = audioTrack.Bitrate
	si.sourceChannels = audioTrack.Channels
	si.sourceSampleRate = audioTrack.SampleRate
	si.sourceSampleBits = audioTrack.SampleBits
	si.sourceVideoStartTimeSec = videoStartTimeSec
	si.sourceAudioStartTimeSec = audioStartTimeSec

	si.ReportProgress(clients.TranscodeStatusPreparing, 0)
	c.Jobs.Store(si.StreamName, si)
	log.Log(si.RequestID, "Wrote to jobs cache")
	metrics.Metrics.JobsInFlight.Set(float64(len(c.Jobs.GetKeys())))

	c.runHandlerAsync(si, func() (*HandlerOutput, error) {
		return si.handler.HandleStartUploadJob(si)
	})
	return si.result
}

// runHandlerAsync starts a background go-routine to run the handler function
// safely. It locks on the JobInfo object to allow safe mutations inside the
// handler. It also handles panics and errors, turning them into a transcode
// status update with an error result.
func (c *Coordinator) runHandlerAsync(job *JobInfo, handler func() (*HandlerOutput, error)) {
	// nolint:errcheck
	go recovered(func() (t bool, e error) {
		job.mu.Lock()
		defer job.mu.Unlock()

		out, err := recovered(handler)
		if err != nil || (out != nil && !out.Continue) {
			if err != nil {
				log.LogError(job.RequestID, "error running job handler", err)
			}
			c.finishJob(job, out, err)
		}
		// dummy
		return
	})
}

func (c *Coordinator) finishJob(job *JobInfo, out *HandlerOutput, err error) {
	defer close(job.result)
	var tsm clients.TranscodeStatusMessage
	if err != nil {
		callbackURL := job.CallbackURL
		if job.hasFallback {
			// an empty url will skip actually sending the callback. we still want the log tho
			callbackURL = ""
		}
		tsm = clients.NewTranscodeStatusError(callbackURL, job.RequestID, err.Error(), errors.IsUnretriable(err))
		job.state = "failed"
	} else {
		tsm = clients.NewTranscodeStatusCompleted(job.CallbackURL, job.RequestID, out.Result.InputVideo, out.Result.Outputs)
		job.state = "completed"
	}
	err2 := job.statusClient.SendTranscodeStatus(tsm)
	if err2 != nil {
		log.LogError(tsm.RequestID, "failed sending finalize callback, job state set to 'failed'", err2)
		job.state = "failed"
	}

	// Automatically delete jobs after an error or result
	success := err == nil && err2 == nil
	c.Jobs.Remove(job.StreamName)
	log.Log(job.RequestID, "Finished job and deleted from job cache", "success", success)
	metrics.Metrics.JobsInFlight.Set(float64(len(c.Jobs.GetKeys())))

	var labels = []string{
		job.sourceCodecVideo,
		job.sourceCodecAudio,
		job.pipeline,
		job.catalystRegion,
		fmt.Sprint(job.numProfiles),
		job.state,
		config.Version,
		strconv.FormatBool(job.inFallbackMode),
		strconv.FormatBool(job.LivepeerSupported),
		strconv.FormatBool(job.ClipStrategy.Enabled),
		strconv.FormatBool(job.ThumbnailsTargetURL != nil),
	}

	metrics.Metrics.VODPipelineMetrics.Count.
		WithLabelValues(labels...).
		Inc()

	metrics.Metrics.VODPipelineMetrics.Duration.
		WithLabelValues(labels...).
		Observe(time.Since(job.startTime).Seconds())

	metrics.Metrics.VODPipelineMetrics.SourceSegments.
		WithLabelValues(labels...).
		Observe(float64(job.sourceSegments))

	metrics.Metrics.VODPipelineMetrics.SourceBytes.
		WithLabelValues(labels...).
		Observe(float64(job.sourceBytes))

	metrics.Metrics.VODPipelineMetrics.SourceDuration.
		WithLabelValues(labels...).
		Observe(float64(job.sourceDurationMs))

	metrics.Metrics.VODPipelineMetrics.TranscodedSegments.
		WithLabelValues(labels...).
		Add(float64(job.transcodedSegments))

	c.sendDBMetrics(job, out)

	job.result <- success
}

func getProfileCount(out *HandlerOutput) int {
	if out == nil || out.Result == nil || len(out.Result.Outputs) < 1 {
		return 0
	}
	return len(out.Result.Outputs[0].Videos)
}

func (c *Coordinator) sendDBMetrics(job *JobInfo, out *HandlerOutput) {
	if c.MetricsDB == nil {
		return
	}

	// If it's a fallback, we want a unique Request ID so that it doesn't clash with the row that's already been created for the first pipeline
	metricsRequestID := job.RequestID
	if job.inFallbackMode {
		metricsRequestID = "fb_" + metricsRequestID
	}

	targetURL := ""
	if job.HlsTargetURL != nil {
		targetURL = job.HlsTargetURL.Redacted()
	}
	insertDynStmt := `insert into "vod_completed"(
                            "finished_at",
                            "started_at",
                            "request_id",
                            "external_id",
                            "source_codec_video",
                            "source_codec_audio",
                            "pipeline",
                            "catalyst_region",
                            "state",
                            "profiles_count",
                            "job_duration",
                            "source_segment_count",
                            "transcoded_segment_count",
                            "source_bytes_count",
                            "source_duration",
                            "source_url",
                            "target_url",
                            "in_fallback_mode",
                            "source_playback_at",
                            "download_done_at",
                            "segmenting_done_at",
                            "transcoding_done_at",
                            "is_clip",
                            "is_thumbs"
                            ) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)`
	_, err := c.MetricsDB.Exec(
		insertDynStmt,
		time.Now().Unix(),
		job.startTime.Unix(),
		metricsRequestID,
		job.ExternalID,
		job.sourceCodecVideo,
		job.sourceCodecAudio,
		job.pipeline,
		job.catalystRegion,
		job.state,
		getProfileCount(out),
		time.Since(job.startTime).Milliseconds(),
		job.sourceSegments,
		job.transcodedSegments,
		job.sourceBytes,
		job.sourceDurationMs,
		log.RedactURL(job.SourceFile),
		targetURL,
		job.inFallbackMode,
		job.SourcePlaybackDone.Unix(),
		job.DownloadDone.Unix(),
		job.SegmentingDone.Unix(),
		job.TranscodingDone.Unix(),
		job.ClipStrategy.Enabled,
		job.ThumbnailsTargetURL != nil,
	)
	if err != nil {
		log.LogError(job.RequestID, "error writing postgres metrics", err)
		return
	}
}

func recovered[T any](f func() (T, error)) (t T, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.LogNoRequestID("panic in pipeline handler background goroutine, recovering", "err", err, "trace", debug.Stack())
			err = fmt.Errorf("panic in pipeline handler: %v", rec)
		}
	}()
	return f()
}

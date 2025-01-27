package pipeline

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/video"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

var (
	testHandlerResult = &HandlerOutput{
		Result: &UploadJobResult{video.InputVideo{}, []video.OutputVideo{
			{Type: "object_store", Manifest: "manifest", Videos: []video.OutputVideoFile{{}}},
		}},
	}
	testJob = UploadJobPayload{
		RequestID:    "123",
		SourceFile:   "source-file",
		HlsTargetURL: &url.URL{Scheme: "s3+https", Host: "storage.google.com", Path: "/bucket/key", User: url.UserPassword("user", "pass")},
		CallbackURL:  "http://localhost:3000/dummy",
	}
)

func setupTransferDir(t *testing.T, coor *Coordinator) (inputFile *os.File, transferURL *url.URL, cleanup func()) {
	var err error
	inputFile, err = os.CreateTemp(os.TempDir(), "user-input-*")
	require.NoError(t, err)
	movieFile, err := os.Open("../clients/fixtures/mediaconvert_payloads/sample.mp4")
	require.NoError(t, err)
	_, err = io.Copy(inputFile, movieFile)
	require.NoError(t, err)
	_, err = inputFile.WriteString("exampleFileContents")
	require.NoError(t, err)
	require.NoError(t, movieFile.Close())

	// use the random file name as the dir name for the transfer file
	transferDir := path.Join(inputFile.Name()+"-dir", "transfer")
	require.NoError(t, os.MkdirAll(transferDir, 0777))

	cleanup = func() {
		inErr := os.Remove(inputFile.Name())
		dirErr := os.RemoveAll(transferDir)
		require.NoError(t, inErr)
		require.NoError(t, dirErr)
		require.NoError(t, inputFile.Close())
	}

	coor.InputCopy = &clients.InputCopy{
		Probe: video.Probe{},
	}
	transferURL, err = url.Parse(transferDir)
	require.NoError(t, err)
	coor.SourceOutputURL = transferURL
	return
}

func TestCoordinatorDoesNotBlock(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	barrier := make(chan struct{})
	var running atomic.Bool
	blockHandler := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			running.Store(true)
			defer running.Store(false)
			<-barrier
			return nil, errors.New("test error")
		},
	}
	coord := NewStubCoordinatorOpts("", callbackHandler, blockHandler, blockHandler)
	inputFile, _, cleanup := setupTransferDir(t, coord)
	defer cleanup()
	job := testJob
	job.SourceFile = "file://" + inputFile.Name()
	coord.StartUploadJob(job)
	time.Sleep(2 * time.Second)

	require.True(running.Load())
	requireReceive(t, callbacks, 5*time.Second) // discard initial TranscodeStatusPreparing message
	msg := requireReceive(t, callbacks, 5*time.Second)
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)

	close(barrier)
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusError, msg.Status)
	require.Contains(msg.Error, "test error")

	require.Zero(len(callbacks))
}

func TestCoordinatorResistsPanics(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	blockHandler := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			panic("oh no!")
		},
	}
	coord := NewStubCoordinatorOpts("", callbackHandler, blockHandler, blockHandler)

	inputFile, _, cleanup := setupTransferDir(t, coord)
	defer cleanup()
	job := testJob
	job.SourceFile = "file://" + inputFile.Name()
	coord.StartUploadJob(job)

	require.Equal(1, len(callbacks))
	requireReceive(t, callbacks, 1*time.Second) // discard initial TranscodeStatusPreparing message
	require.Equal(clients.TranscodeStatusPreparing, (<-callbacks).Status)

	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusError, msg.Status)
	require.Contains(msg.Error, "oh no")
}

func TestCoordinatorCatalystDominance(t *testing.T) {
	require := require.New(t)

	ffmpeg, calls := recordingHandler(nil)
	external := allFailingHandler(t)
	coord := NewStubCoordinatorOpts(StrategyCatalystFfmpegDominance, nil, ffmpeg, external)

	inputFile, _, cleanup := setupTransferDir(t, coord)
	defer cleanup()
	job := testJob
	job.SourceFile = "file://" + inputFile.Name()
	coord.StartUploadJob(job)

	j := requireReceive(t, calls, 1*time.Second)
	require.Equal("123", j.RequestID)

	time.Sleep(1 * time.Second)
	require.Zero(len(calls))
}

func TestCoordinatorSourceCopy(t *testing.T) {
	require := require.New(t)

	ffmpeg, calls := recordingHandler(nil)
	external := allFailingHandler(t)
	coord := NewStubCoordinatorOpts(StrategyCatalystFfmpegDominance, nil, ffmpeg, external)

	inputFile, _, cleanup := setupTransferDir(t, coord)
	defer cleanup()
	job := testJob
	job.SourceFile = "file://" + inputFile.Name()
	job.SourceCopy = true
	job.HlsTargetURL = coord.SourceOutputURL
	coord.StartUploadJob(job)

	j := requireReceive(t, calls, 1*time.Second)
	require.Equal("123", j.RequestID)

	time.Sleep(1 * time.Second)
	require.Zero(len(calls))
}

func TestCoordinatorFallbackStrategySuccess(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	ffmpeg, ffmpegCalls := recordingHandler(nil)
	external, externalCalls := recordingHandler(nil)

	coord := NewStubCoordinatorOpts(StrategyFallbackExternal, callbackHandler, ffmpeg, external)

	// Start a job that will complete successfully on ffmpeg, which should not
	// trigger the external pipeline
	inputFile, _, cleanup := setupTransferDir(t, coord)
	defer cleanup()
	job := testJob
	job.SourceFile = "file://" + inputFile.Name()
	coord.StartUploadJob(job)

	requireReceive(t, callbacks, 1*time.Second) // discard initial TranscodeStatusPreparing message
	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)

	mistJob := requireReceive(t, ffmpegCalls, 1*time.Second)
	require.Equal("123", mistJob.RequestID)

	// Check successful completion of the ffmpeg event
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusCompleted, msg.Status)

	time.Sleep(1 * time.Second)
	require.Zero(len(ffmpegCalls))
	require.Zero(len(callbacks))
	// nothing should have happened on the external flow
	require.Zero(len(externalCalls))
}

func TestCoordinatorFallbackStrategyFailure(t *testing.T) {
	require := require.New(t)

	callbackHandler, callbacks := callbacksRecorder()
	ffmpeg, ffmpegCalls := recordingHandler(errors.New("ffmpeg error"))
	externalCalls := make(chan *JobInfo, 10)
	external := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			externalCalls <- job
			job.ReportProgress(clients.TranscodeStatusPreparing, 0.2)
			return testHandlerResult, nil
		},
	}

	coord := NewStubCoordinatorOpts(StrategyFallbackExternal, callbackHandler, ffmpeg, external)

	// Start a job which ffmpeg will fail and only then call the external one
	inputFile, _, cleanup := setupTransferDir(t, coord)
	defer cleanup()
	job := testJob
	job.SourceFile = "file://" + inputFile.Name()
	coord.StartUploadJob(job)

	requireReceive(t, callbacks, 1*time.Second) // discard initial TranscodeStatusPreparing message
	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)

	mistJob := requireReceive(t, ffmpegCalls, 1*time.Second)
	require.Equal("123", mistJob.RequestID)

	// External provider pipeline will trigger the initial preparing trigger as well
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)

	meconJob := requireReceive(t, externalCalls, 1*time.Second)
	require.Equal("123", meconJob.RequestID)
	require.Equal(config.SEGMENTING_PREFIX+"123", meconJob.StreamName)

	// Check that the progress reported in the fallback handler is still reported
	msg = requireReceive(t, callbacks, 1*time.Second)
	require.NotZero(msg.URL)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusPreparing, msg.Status)
	require.Equal(clients.OverallCompletionRatio(clients.TranscodeStatusPreparing, 0.2), msg.CompletionRatio)

	msg = requireReceive(t, callbacks, 1*time.Second)
	require.Equal("123", msg.RequestID)
	require.Equal(clients.TranscodeStatusCompleted, msg.Status)

	time.Sleep(1 * time.Second)
	require.Zero(len(ffmpegCalls))
	require.Zero(len(externalCalls))
	require.Zero(len(callbacks))
}

func TestAllowsOverridingStrategyOnRequest(t *testing.T) {
	require := require.New(t)

	ffmpeg, ffmpegCalls := recordingHandler(errors.New("ffmpeg error"))
	external, externalCalls := recordingHandler(nil)

	// create coordinator with strategy catalyst dominance (external should never be called)
	coord := NewStubCoordinatorOpts(StrategyCatalystFfmpegDominance, nil, ffmpeg, external)

	inputFile, _, cleanup := setupTransferDir(t, coord)
	defer cleanup()
	// Override the strategy to background external, which will call the external provider *and* the ffmpeg provider
	p := testJob
	p.PipelineStrategy = StrategyFallbackExternal
	p.SourceFile = "file://" + inputFile.Name()
	coord.StartUploadJob(p)

	// Check that it was really called
	ffmpegJob := requireReceive(t, ffmpegCalls, 500*time.Second)
	require.Equal("123", ffmpegJob.RequestID)
	require.Equal("catalyst_vod_123", ffmpegJob.StreamName)

	// Sanity check that ffmpeg also ran
	externalJob := requireReceive(t, externalCalls, 1*time.Second)
	require.Equal(ffmpegJob.RequestID, externalJob.RequestID)
	require.Equal("catalyst_vod_"+ffmpegJob.RequestID, externalJob.StreamName)
}

func setJobInfoFields(job *JobInfo) {
	job.catalystRegion = "test region"
	job.sourceCodecVideo = "vid codec"
	job.sourceCodecAudio = "audio codec"
	job.numProfiles = 1
	job.sourceSegments = 2
	job.transcodedSegments = 3
	job.sourceBytes = 4
	job.sourceDurationMs = 5
	job.startTime = time.Unix(0, 0)
}

func TestPipelineCollectedMetrics(t *testing.T) {
	require := require.New(t)

	metricsServer := httptest.NewServer(promhttp.Handler())
	defer metricsServer.Close()

	ffmpeg := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			setJobInfoFields(job)
			return testHandlerResult, nil
		},
	}
	external := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			setJobInfoFields(job)
			return testHandlerResult, nil
		},
	}
	callbackHandler, callbacks := callbacksRecorder()

	db, dbMock, err := sqlmock.New()
	require.NoError(err)
	coord := NewStubCoordinatorOpts(StrategyFallbackExternal, callbackHandler, ffmpeg, external)
	coord.MetricsDB = db

	inputFile, transferDir, cleanup := setupTransferDir(t, coord)
	defer cleanup()
	job := testJob
	job.SourceFile = "file://" + inputFile.Name()
	sourceFile := path.Join(transferDir.String(), "123/transfer/"+filepath.Base(inputFile.Name()))

	dbMock.
		ExpectExec("insert into \"vod_completed\".*").
		WithArgs(sqlmock.AnyArg(), 0, "123", sqlmock.AnyArg(), "vid codec", "audio codec", "stub", "test region", "completed", 1, sqlmock.AnyArg(), 2, 3, 4, 5, sourceFile, "s3+https://user:xxxxx@storage.google.com/bucket/key", false, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), false, false).
		WillReturnResult(sqlmock.NewResult(1, 1))

	coord.StartUploadJob(job)
	requireReceive(t, callbacks, 5*time.Second) // discard initial TranscodeStatusPreparing message
	requireReceive(t, callbacks, 5*time.Second) // discard second TranscodeStatusPreparing message

	res, err := http.Get(metricsServer.URL)
	require.NoError(err)

	b, err := io.ReadAll(res.Body)
	require.NoError(err)

	body := string(b)

	require.Contains(body, "# TYPE vod_count")
	require.Contains(body, "# TYPE vod_duration")
	require.Contains(body, "# TYPE vod_source_segments")
	require.Contains(body, "# TYPE vod_source_bytes")
	require.Contains(body, "# TYPE vod_source_duration")

	// the db metrics function is called in a separate go routine so there is a chance we need to wait here
	err = retry(3, 500*time.Millisecond, func() error {
		return dbMock.ExpectationsWereMet()
	})
	require.NoError(err)
}

func Test_EmptyFile(t *testing.T) {
	callbackHandler, callbacks := callbacksRecorder()
	coord := NewStubCoordinatorOpts("", callbackHandler, nil, nil)
	inputFile, _, cleanup := setupTransferDir(t, coord)
	defer cleanup()

	err := os.Truncate(inputFile.Name(), 0)
	require.NoError(t, err)

	job := testJob
	job.SourceFile = "file://" + inputFile.Name()
	coord.StartUploadJob(job)
	requireReceive(t, callbacks, 1*time.Second) // discard initial TranscodeStatusPreparing message
	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(t, clients.TranscodeStatusError, msg.Status)
	require.Equal(t, "error copying input to storage: failed to copy file(s): zero bytes found for source: "+job.SourceFile, msg.Error)
}

func Test_ProbeErrors(t *testing.T) {
	tests := []struct {
		name        string
		fps         float64
		assetType   string
		size        int64
		probeErr    error
		expectedErr string
	}{
		{
			name:        "valid",
			expectedErr: "",
		},
		{
			name:        "invalid framerate",
			fps:         -1,
			expectedErr: "error copying input to storage: invalid framerate: -1.000000",
		},
		{
			name:        "audio only",
			assetType:   "audio",
			expectedErr: "",
		},
		{
			name:        "filesize greater than max",
			size:        config.MaxInputFileSizeBytes + 1,
			expectedErr: "error copying input to storage: input file 32212254721 bytes was greater than 32212254720 bytes",
		},
		{
			name:        "probe error",
			probeErr:    errors.New("probe failed"),
			expectedErr: "error copying input to storage: error probing MP4 input file from S3: probe failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackHandler, callbacks := callbacksRecorder()
			coord := NewStubCoordinatorOpts("", callbackHandler, nil, nil)
			inputFile, transferDir, cleanup := setupTransferDir(t, coord)
			defer cleanup()
			coord.InputCopy = &clients.InputCopy{
				Probe: stubFFprobe{
					FPS:  tt.fps,
					Type: tt.assetType,
					Size: tt.size,
					Err:  tt.probeErr,
				},
			}
			coord.SourceOutputURL = transferDir

			job := testJob
			job.SourceFile = "file://" + inputFile.Name()
			coord.StartUploadJob(job)
			requireReceive(t, callbacks, 1*time.Second) // discard initial TranscodeStatusPreparing message
			msg := requireReceive(t, callbacks, 1*time.Second)

			require.Equal(t, tt.expectedErr, msg.Error)
		})
	}
}

func Test_InputCopiedToTransferLocation(t *testing.T) {
	require := require.New(t)
	var actualTransferInput string
	callbackHandler, callbacks := callbacksRecorder()
	ffmpeg := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			actualTransferInput = job.SourceFile
			return testHandlerResult, nil
		},
	}
	coord := NewStubCoordinatorOpts(StrategyCatalystFfmpegDominance, callbackHandler, ffmpeg, nil)
	f, transferDir, cleanup := setupTransferDir(t, coord)
	defer cleanup()

	job := testJob
	job.SourceFile = "file://" + f.Name()
	coord.StartUploadJob(job)
	requireReceive(t, callbacks, 1*time.Second) // discard initial TranscodeStatusPreparing message
	requireReceive(t, callbacks, 1*time.Second) // discard second TranscodeStatusPreparing message
	msg := requireReceive(t, callbacks, 1*time.Second)
	require.Equal(clients.TranscodeStatusCompleted, msg.Status)

	// Check that the file was copied to the osTransferBucketURL folder
	transferInput := path.Join(transferDir.String(), "/123/transfer/"+filepath.Base(f.Name()))
	require.Equal(transferInput, actualTransferInput)
	content, err := os.Open(transferInput)
	require.NoError(err)

	hashContent := md5.New()
	_, err = io.Copy(hashContent, content)
	require.NoError(err)

	inputFile, err := os.Open(f.Name())
	require.NoError(err)
	hashInputFile := md5.New()
	_, err = io.Copy(hashInputFile, inputFile)
	require.NoError(err)

	require.Equal(hashInputFile, hashContent)
}

func retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(sleep)
			sleep *= 2
		}
		err = f()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("failed after %d attempts, last error: %s", attempts, err)
}

func allFailingHandler(t *testing.T) Handler {
	return &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			require.Fail(t, "Unexpected handleStartUploadJob")
			panic("unreachable")
		},
	}
}

func callbacksRecorder() (clients.TranscodeStatusClient, <-chan clients.TranscodeStatusMessage) {
	callbacks := make(chan clients.TranscodeStatusMessage, 10)
	handler := func(msg clients.TranscodeStatusMessage) error {
		// background jobs send updates without a callback URL, which are ignored by
		// the callbacks client. Only record the real ones here.
		if msg.URL != "" {
			callbacks <- msg
		}
		return nil
	}
	return clients.TranscodeStatusFunc(handler), callbacks
}

func recordingHandler(err error) (Handler, <-chan *JobInfo) {
	jobs := make(chan *JobInfo, 10)
	handler := &StubHandler{
		handleStartUploadJob: func(job *JobInfo) (*HandlerOutput, error) {
			jobs <- job
			if err != nil {
				return nil, err
			}
			return testHandlerResult, nil
		},
	}
	return handler, jobs
}

func requireReceive[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	select {
	case job := <-ch:
		return job
	case <-time.After(timeout):
		require.Fail(t, "did not receive expected message")
		panic("unreachable")
	}
}

type stubFFprobe struct {
	Bitrate  int64
	Duration float64
	FPS      float64
	Type     string
	Size     int64
	Err      error
}

func (f stubFFprobe) ProbeFile(_, _ string, _ ...string) (video.InputVideo, error) {
	if f.Err != nil {
		return video.InputVideo{}, f.Err
	}
	if f.Type == "" {
		f.Type = "video"
	}
	if f.FPS == 0 {
		f.FPS = 30
	}
	return video.InputVideo{
		Duration:  f.Duration,
		SizeBytes: f.Size,
		Tracks: []video.InputTrack{
			{
				Type:    f.Type,
				Codec:   "h264",
				Bitrate: f.Bitrate,
				VideoTrack: video.VideoTrack{
					Width:  576,
					Height: 1024,
					FPS:    f.FPS,
				},
			},
		},
	}, nil
}

func Test_checkMistCompatible(t *testing.T) {
	type args struct {
		strategy Strategy
		iv       video.InputVideo
	}
	inCompatibleVideoAndAudio := video.InputVideo{
		Tracks: []video.InputTrack{
			{
				Codec: "HEVC",
				Type:  video.TrackTypeVideo,
			},
			{
				Codec: "ac-3",
				Type:  video.TrackTypeAudio,
			},
		},
	}
	inCompatibleVideo := video.InputVideo{
		Tracks: []video.InputTrack{
			{
				Codec: "HEVC",
				Type:  video.TrackTypeVideo,
			},
			{
				Codec: "aac",
				Type:  video.TrackTypeAudio,
			},
		},
	}
	nonAACAudio := video.InputVideo{
		Tracks: []video.InputTrack{
			{
				Codec: "h264",
				Type:  video.TrackTypeVideo,
			},
			{
				Codec: "ac-3",
				Type:  video.TrackTypeAudio,
			},
		},
	}
	compatibleVideoAndAudio := video.InputVideo{
		Tracks: []video.InputTrack{
			{
				Codec: "h264",
				Type:  video.TrackTypeVideo,
			},
			{
				Codec: "aac",
				Type:  video.TrackTypeAudio,
			},
		},
	}
	videoRotation := video.InputVideo{
		Tracks: []video.InputTrack{
			{
				Codec: "h264",
				Type:  video.TrackTypeVideo,
				VideoTrack: video.VideoTrack{
					Rotation: 1,
				},
			},
		},
	}
	tests := []struct {
		name          string
		args          args
		want          Strategy
		wantSupported bool
	}{
		{
			name: "catalyst dominance",
			args: args{
				strategy: StrategyCatalystFfmpegDominance,
				iv:       inCompatibleVideoAndAudio,
			},
			want:          StrategyCatalystFfmpegDominance,
			wantSupported: false,
		},
		{
			name: "catalyst dominance",
			args: args{
				strategy: StrategyCatalystFfmpegDominance,
				iv:       inCompatibleVideo,
			},
			want:          StrategyCatalystFfmpegDominance,
			wantSupported: false,
		},
		{
			name: "incompatible with ffmpeg - StrategyFallbackExternal",
			args: args{
				strategy: StrategyFallbackExternal,
				iv:       inCompatibleVideo,
			},
			want:          StrategyBackgroundFFMpeg,
			wantSupported: true,
		},
		{
			name: "compatible with ffmpeg (non AAC audio) - StrategyFallbackExternal",
			args: args{
				strategy: StrategyFallbackExternal,
				iv:       nonAACAudio,
			},
			want:          StrategyFallbackExternal,
			wantSupported: true,
		},
		{
			name: "compatible with ffmpeg - StrategyFallbackExternal",
			args: args{
				strategy: StrategyFallbackExternal,
				iv:       compatibleVideoAndAudio,
			},
			want:          StrategyFallbackExternal,
			wantSupported: true,
		},
		{
			name: "incompatible with ffmpeg - video rotation",
			args: args{
				strategy: StrategyFallbackExternal,
				iv:       videoRotation,
			},
			want:          StrategyExternalDominance,
			wantSupported: false,
		},
		{
			name: "incompatible with ffmpeg - display aspect ratio",
			args: args{
				strategy: StrategyFallbackExternal,
				iv: video.InputVideo{
					Tracks: []video.InputTrack{
						{
							Codec: "h264",
							Type:  video.TrackTypeVideo,
							VideoTrack: video.VideoTrack{
								Width:              100,
								Height:             100,
								DisplayAspectRatio: "16:9",
							},
						},
					},
				},
			},
			want:          StrategyExternalDominance,
			wantSupported: false,
		},
		{
			name: "incompatible with ffmpeg - display aspect ratio",
			args: args{
				strategy: StrategyFallbackExternal,
				iv: video.InputVideo{
					Tracks: []video.InputTrack{
						{
							Codec: "h264",
							Type:  video.TrackTypeVideo,
							VideoTrack: video.VideoTrack{
								Width:              1920,
								Height:             1080,
								DisplayAspectRatio: "9:16",
							},
						},
					},
				},
			},
			want:          StrategyExternalDominance,
			wantSupported: false,
		},
		{
			name: "compatible with ffmpeg - display aspect ratio only slightly mismatched",
			args: args{
				strategy: StrategyFallbackExternal,
				iv: video.InputVideo{
					Tracks: []video.InputTrack{
						{
							Codec: "h264",
							Type:  video.TrackTypeVideo,
							VideoTrack: video.VideoTrack{
								Width:              16,
								Height:             10,
								DisplayAspectRatio: "16:9",
							},
						},
					},
				},
			},
			want:          StrategyFallbackExternal,
			wantSupported: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			supported, got := checkLivepeerCompatible("requestID", tt.args.strategy, tt.args.iv)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantSupported, supported)
		})
	}
}

func TestMP4Generation(t *testing.T) {
	hlsSourceURL, err := url.Parse("http://not-a-real-domain.lol/hls/manifest.m3u8")
	require.NoError(t, err)

	mp4SourceURL, err := url.Parse("http://not-a-real-domain.lol/mp4/source.mp4")
	require.NoError(t, err)

	mp4TargetURL, err := url.Parse("http://not-a-real-domain.lol/target/target.mp4")
	require.NoError(t, err)

	fragMp4TargetURL, err := url.Parse("http://not-a-real-domain.lol/target/target.m3u8")
	require.NoError(t, err)

	should, _ := ShouldGenerateMP4(mp4SourceURL, nil, nil, true, 60)
	require.False(t, should, "Should NOT generate an MP4 if the MP4 target URL isn't present")

	should, _ = ShouldGenerateMP4(mp4SourceURL, mp4TargetURL, fragMp4TargetURL, true, 60)
	require.True(t, should, "SHOULD generate an MP4 for a short source MP4 input even if 'only short MP4s' mode is enabled")

	should, _ = ShouldGenerateMP4(hlsSourceURL, mp4TargetURL, nil, true, 60)
	require.True(t, should, "SHOULD generate an MP4 for a short source HLS input even if 'only short MP4s' mode is enabled")

	should, _ = ShouldGenerateMP4(mp4SourceURL, mp4TargetURL, nil, true, 60*10)
	require.False(t, should, "Should NOT generate an MP4 for a long source MP4 input if 'only short MP4s' mode is enabled")

	should, _ = ShouldGenerateMP4(hlsSourceURL, mp4TargetURL, nil, true, 60*10)
	require.False(t, should, "Should NOT generate an MP4 for a long source HLS input if 'only short MP4s' mode is enabled")

	should, _ = ShouldGenerateMP4(mp4SourceURL, mp4TargetURL, nil, false, 60*10)
	require.True(t, should, "SHOULD generate an MP4 for a long source MP4 input if 'only short MP4s' mode is disabled")

	should, _ = ShouldGenerateMP4(hlsSourceURL, mp4TargetURL, nil, false, 60*10)
	require.True(t, should, "SHOULD generate an MP4 for a long source HLS input if 'only short MP4s' mode is disabled")

	should, _ = ShouldGenerateMP4(hlsSourceURL, mp4TargetURL, nil, false, 60*60*13)
	require.False(t, should, "SHOULD NOT generate an MP4 for a VERY long source HLS input even if 'only short MP4s' mode is disabled")

	should, _ = ShouldGenerateMP4(hlsSourceURL, nil, fragMp4TargetURL, false, 60*60*1)
	require.True(t, should, "SHOULD generate an MP4 for a fragmented Mp4 regardless of 'only short MP4s' mode")

	should, _ = ShouldGenerateMP4(hlsSourceURL, nil, nil, false, 60*60*13)
	require.False(t, should, "SHOULD NOT generate an MP4 if no valid mp4 or fmp4 URL was provided")

	should, _ = ShouldGenerateMP4(hlsSourceURL, mp4TargetURL, fragMp4TargetURL, false, 0)
	require.False(t, should, "SHOULD NOT generate an MP4 if duration is 0 regardless of a valid mp4/fmp4 URL")
}

package pipeline

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/thumbnails"
	"github.com/livepeer/catalyst-api/transcode"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
)

const LocalSourceFilePattern = "sourcevideo*.mp4"

type ffmpeg struct {
	// The base of where to output source segments to
	SourceOutputURL *url.URL
	// Broadcaster for local transcoding
	Broadcaster         clients.BroadcasterClient
	probe               video.Prober
	sourcePlaybackHosts map[string]string
}

func init() {
	// Clean up any temp source files that might be lying around from jobs that were interrupted
	// during a deploy
	if err := cleanUpLocalTmpFiles(os.TempDir(), LocalSourceFilePattern, 6*time.Hour); err != nil {
		log.LogNoRequestID("cleanUpLocalTmpFiles error: %w", err)
	}
}

func (f *ffmpeg) Name() string {
	return "catalyst_ffmpeg"
}

func (f *ffmpeg) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	log.Log(job.RequestID, "Handling job via FFMPEG/Livepeer pipeline")

	sourceOutputURL := f.SourceOutputURL.JoinPath(job.RequestID)
	segmentingTargetURL := sourceOutputURL.JoinPath(config.SEGMENTING_SUBDIR, config.SEGMENTING_TARGET_MANIFEST)

	job.SegmentingTargetURL = segmentingTargetURL.String()
	log.AddContext(job.RequestID, "segmented_url", job.SegmentingTargetURL)
	job.ReportProgress(clients.TranscodeStatusPreparing, 0.3)

	// Segment only for non-HLS inputs
	var localSourceTmp string
	if job.InputFileInfo.Format != "hls" {
		var err error
		localSourceTmp, err = copyFileToLocalTmpAndSegment(job)
		if err != nil {
			return nil, err
		}
		if job.C2PA == nil {
			os.Remove(localSourceTmp)
		} else {
			// Source file is needed for the C2PA signature,
			// so we can remove the temp source file only after the whole transcoding is completed
			defer os.Remove(localSourceTmp)
		}
	} else {
		job.SegmentingTargetURL = job.SourceFile

		// don't generate thumbs for very long recordings since it involves downloading segments
		if job.InputFileInfo.Duration <= 0 || job.InputFileInfo.Duration > maxRecordingThumbsDuration.Seconds() {
			job.ThumbnailsTargetURL = nil
		}
		go func() {
			if job.ThumbnailsTargetURL == nil {
				return
			}
			err := thumbnails.GenerateThumbsFromManifest(job.RequestID, job.SegmentingTargetURL, job.ThumbnailsTargetURL)
			if err != nil {
				log.LogError(job.RequestID, "generate thumbs failed", err, "in", job.SegmentingTargetURL, "out", job.ThumbnailsTargetURL)
			}
		}()
	}
	job.SegmentingDone = time.Now()
	if job.HlsTargetURL != nil {
		f.sendSourcePlayback(job)
	}
	job.ReportProgress(clients.TranscodeStatusPreparingCompleted, 1)

	// Transcode Beginning
	log.Log(job.RequestID, "Beginning transcoding via FFMPEG/Livepeer pipeline")

	transcodeRequest := transcode.TranscodeSegmentRequest{
		SourceFile:        job.SourceFile,
		CallbackURL:       job.CallbackURL,
		AccessToken:       job.AccessToken,
		TranscodeAPIUrl:   job.TranscodeAPIUrl,
		Profiles:          job.Profiles,
		SourceManifestURL: job.SegmentingTargetURL,
		SourceOutputURL:   sourceOutputURL.String(),
		HlsTargetURL:      toStr(job.HlsTargetURL),
		Mp4TargetUrl:      toStr(job.Mp4TargetURL),
		FragMp4TargetUrl:  toStr(job.FragMp4TargetURL),
		RequestID:         job.RequestID,
		ReportProgress:    job.ReportProgress,
		GenerateMP4:       job.GenerateMP4,
		IsClip:            job.ClipStrategy.Enabled,
		C2PA:              job.C2PA,
		LocalSourceTmp:    localSourceTmp,
	}

	inputInfo := video.InputVideo{
		Format:    job.InputFileInfo.Format,
		Duration:  job.InputFileInfo.Duration,
		SizeBytes: job.sourceBytes,
		Tracks: []video.InputTrack{
			// Video Track
			{
				Type:         "video",
				Codec:        job.sourceCodecVideo,
				Bitrate:      job.sourceBitrateVideo,
				DurationSec:  job.InputFileInfo.Duration,
				StartTimeSec: job.sourceVideoStartTimeSec,
				VideoTrack: video.VideoTrack{
					Width:  job.sourceWidth,
					Height: job.sourceHeight,
					FPS:    job.sourceFPS,
				},
			},
			// Audio Track
			{
				Type:         "audio",
				Codec:        job.sourceCodecAudio,
				Bitrate:      job.sourceBitrateAudio,
				DurationSec:  job.InputFileInfo.Duration,
				StartTimeSec: job.sourceAudioStartTimeSec,
				AudioTrack: video.AudioTrack{
					Channels:   job.sourceChannels,
					SampleRate: job.sourceSampleRate,
					SampleBits: job.sourceSampleBits,
				},
			},
		},
	}

	job.state = "transcoding"

	sourceManifest, err := clients.DownloadRenditionManifest(transcodeRequest.RequestID, transcodeRequest.SourceManifestURL)
	if err != nil {
		return nil, fmt.Errorf("error downloading source manifest %s: %w", log.RedactURL(transcodeRequest.SourceManifestURL), err)
	}

	// Check a few segments from the segmented source input file (non-hls)
	sourceSegments := sourceManifest.GetAllSegments()
	job.sourceSegments = len(sourceSegments)
	err = f.probeSourceSegments(job, sourceSegments)
	if err != nil {
		return nil, err
	}

	// Check a few segments from the recording source input file (hls)
	err = f.probeRecordingSourceSegments(job, &inputInfo, sourceSegments)
	if err != nil {
		log.LogError(job.RequestID, "failed to probe recording source segments before transcoding - continuing with transcode", err)
	}

	outputs, transcodedSegments, err := transcode.RunTranscodeProcess(transcodeRequest, job.StreamName, inputInfo, f.Broadcaster)
	if err != nil {
		log.LogError(job.RequestID, "RunTranscodeProcess returned an error", err)
		return nil, fmt.Errorf("transcoding failed: %w", err)
	}

	// wait for thumbs background process
	if job.ThumbnailsTargetURL != nil {
		err := thumbnails.GenerateThumbsVTT(job.RequestID, job.SegmentingTargetURL, job.ThumbnailsTargetURL)
		if err != nil {
			log.LogError(job.RequestID, "waiting for thumbs failed", err, "out", job.ThumbnailsTargetURL)
		} else {
			log.Log(job.RequestID, "waiting for thumbs succeeded", "out", job.ThumbnailsTargetURL)
		}
	}

	job.TranscodingDone = time.Now()
	job.transcodedSegments = transcodedSegments

	return &HandlerOutput{
		Result: &UploadJobResult{
			InputVideo: inputInfo,
			Outputs:    outputs,
		}}, nil
}

var sourcePlaybackBucketBlocklist = []string{"lp-us-catalyst-vod-pvt-monster", "lp-us-catalyst-vod-pvt-com"}

// 80th percentile of assets uploaded in the past week was 5.9mbps
const maxBitrateSourcePb = 6_000_000

func (f *ffmpeg) sendSourcePlayback(job *JobInfo) {
	for _, track := range job.InputFileInfo.Tracks {
		if track.Bitrate > maxBitrateSourcePb {
			log.Log(job.RequestID, "source playback not available, bitrate too high", "bitrate", track.Bitrate)
			return
		}
	}

	segmentingTargetURL, err := url.Parse(job.SegmentingTargetURL)
	if err != nil {
		log.LogError(job.RequestID, "unable to parse url for source playback", err)
		return
	}
	// remove creds as we are creating playback URLs to be used directly by a front end player
	// currently this will work for our regular buckets except for the ones we're excluding in sourcePlaybackBucketBlocklist
	segmentingTargetURL.User = nil

	sourceURL, err := url.Parse(job.SourceFile)
	if err != nil {
		log.LogError(job.RequestID, "unable to parse source url for source playback", err)
		return
	}

	renditionURL := ""
	for k, v := range f.sourcePlaybackHosts {
		if strings.HasPrefix(segmentingTargetURL.String(), k) {
			renditionURL = strings.Replace(segmentingTargetURL.String(), k, v, 1)
			break
		}
	}
	if clients.IsHLSInput(sourceURL) && renditionURL == "" {
		log.Log(job.RequestID, "no source playback prefix found", "segmentingTargetURL", segmentingTargetURL)
		return
	}

	segmentingPath := strings.Split(segmentingTargetURL.Path, "/")
	if len(segmentingPath) < 3 || segmentingPath[1] == "" {
		log.Log(job.RequestID, "unable to find bucket for source playback", "segmentingTargetURL", segmentingTargetURL)
		return
	}
	// assume bucket is second element in slice (first element should be an empty string as the path has a leading slash)
	segmentingBucket := segmentingPath[1]
	if (job.HlsTargetURL == nil || !strings.Contains(job.HlsTargetURL.String(), "/"+segmentingBucket+"/")) && renditionURL == "" {
		log.Log(job.RequestID, "source playback not available, not a studio job", "segmentingTargetURL", segmentingTargetURL)
		return
	}

	// source playback won't currently work for token gating so we're excluding the private buckets here
	for _, blocked := range sourcePlaybackBucketBlocklist {
		if segmentingBucket == blocked {
			log.Log(job.RequestID, "source playback not available, not main bucket")
			return
		}
	}

	sourceMaster := m3u8.NewMasterPlaylist()
	videoTrack, err := job.InputFileInfo.GetTrack(video.TrackTypeVideo)
	if err != nil {
		log.LogError(job.RequestID, "unable to find a video track for source playback", err)
		return
	}

	if renditionURL == "" {
		renditionURL = "/" + path.Join(segmentingPath[2:]...)
	}
	sourceMaster.Append(renditionURL, &m3u8.MediaPlaylist{}, m3u8.VariantParams{
		Bandwidth:  uint32(videoTrack.Bitrate),
		Resolution: fmt.Sprintf("%dx%d", videoTrack.Width, videoTrack.Height),
		Name:       fmt.Sprintf("%dp", videoTrack.Height),
	})
	err = clients.UploadToOSURLFields(job.HlsTargetURL.String(), "index.m3u8", sourceMaster.Encode(), 10*time.Minute, &drivers.FileProperties{CacheControl: "max-age=60"})
	if err != nil {
		log.LogError(job.RequestID, "failed to write source playback playlist", err)
		return
	}

	sourcePlaylist := job.HlsTargetURL.JoinPath("index.m3u8").String()
	sourceOutput := video.OutputVideo{
		Manifest: sourcePlaylist,
	}
	tsm := clients.NewTranscodeStatusSourcePlayback(job.CallbackURL, job.RequestID, clients.TranscodeStatusPreparingCompleted, 1, &sourceOutput)
	err = job.statusClient.SendTranscodeStatus(tsm)
	if err != nil {
		log.LogError(job.RequestID, "failed to send status message for source playback", err)
		return
	}
	job.SourcePlaybackDone = time.Now()
}

func (f *ffmpeg) probeSourceSegments(job *JobInfo, sourceSegments []*m3u8.MediaSegment) error {
	if job.InputFileInfo.Format == "hls" {
		return nil
	}
	segCount := len(sourceSegments)
	if segCount < 4 {
		for _, segment := range sourceSegments {
			if err := f.probeSourceSegment(job.RequestID, segment, job.SegmentingTargetURL); err != nil {
				return err
			}
		}
		return nil
	}
	segmentsToCheck := []int{0, 1, segCount - 2, segCount - 1}
	for _, i := range segmentsToCheck {
		if err := f.probeSourceSegment(job.RequestID, sourceSegments[i], job.SegmentingTargetURL); err != nil {
			return err
		}
	}
	return nil
}

func (f *ffmpeg) probeSourceSegment(requestID string, seg *m3u8.MediaSegment, sourceManifestURL string) error {
	u, err := clients.ManifestURLToSegmentURL(sourceManifestURL, seg.URI)
	if err != nil {
		return fmt.Errorf("error checking source segments: %w", err)
	}
	probeURL, err := clients.SignURL(u)
	if err != nil {
		return fmt.Errorf("failed to create signed url for %s: %w", u.Redacted(), err)
	}
	if err := backoff.Retry(func() error {
		_, err = f.probe.ProbeFile(requestID, probeURL)
		if err != nil {
			if strings.Contains(err.Error(), "non-existing SPS") {
				log.LogError(requestID, "probeSourceSegment warning", err)
			} else {
				return fmt.Errorf("probe failed for segment %s: %w", u, err)
			}
		}
		return nil
	}, retries(6)); err != nil {
		return err
	}

	// check for audio issues https://linear.app/livepeer/issue/VID-287/audio-missing-after-segmenting
	_, err = f.probe.ProbeFile(requestID, probeURL, "-loglevel", "warning")
	if err != nil && strings.Contains(err.Error(), "no TS found at start of file, duration not set") {
		return fmt.Errorf("probe failed with audio issues for segment %s: %w", u, err)
	}
	return nil
}

func (f *ffmpeg) probeRecordingSourceSegments(job *JobInfo, iv *video.InputVideo, sourceSegments []*m3u8.MediaSegment) error {
	// Only inspect recording segments if the height/width was not determined in the initial probing step
	if job.InputFileInfo.Format != "hls" && (iv.Tracks[0].VideoTrack.Width == 0 || iv.Tracks[0].VideoTrack.Height == 0) {
		return nil
	}
	oldWidth, oldHeight := iv.Tracks[0].VideoTrack.Width, iv.Tracks[0].VideoTrack.Height
	segCount := len(sourceSegments)
	// Check a random segment in the middle
	segmentToCheck := sourceSegments[segCount/2]

	u, err := clients.ManifestURLToSegmentURL(job.SegmentingTargetURL, segmentToCheck.URI)
	if err != nil {
		return fmt.Errorf("error checking recording source segments: %w", err)
	}
	probeURL, err := clients.SignURL(u)
	if err != nil {
		return fmt.Errorf("failed to create signed url for %s: %w", u, err)
	}
	if err := backoff.Retry(func() error {
		recSegmentProbe, err := f.probe.ProbeFile(job.RequestID, probeURL)
		if err != nil {
			return fmt.Errorf("probe failed for recording source segment %s: %w", u, err)
		}
		videoTrack, err := recSegmentProbe.GetTrack(video.TrackTypeVideo)
		hasVideoTrack := err == nil
		if hasVideoTrack {
			iv.Tracks[0].VideoTrack.Width = videoTrack.Width
			iv.Tracks[0].VideoTrack.Height = videoTrack.Height
			log.Log(job.RequestID, "Updated recording track info from", "old-width", oldWidth, "old-height", oldHeight, "new-width", iv.Tracks[0].VideoTrack.Width, "new-height", iv.Tracks[0].VideoTrack.Height)
		}
		return nil
	}, retries(3)); err != nil {
		return err
	}

	return nil
}

func copyFileToLocalTmpAndSegment(job *JobInfo) (string, error) {
	// Create a temporary local file to write to
	localSourceFile, err := os.CreateTemp(os.TempDir(), LocalSourceFilePattern)
	if err != nil {
		return "", fmt.Errorf("failed to create local file for segmenting: %w", err)
	}
	defer localSourceFile.Close()

	// Copy the file locally because of issues with ffmpeg segmenting and remote files
	// We can be aggressive with the timeout because we're copying from cloud storage
	if err := backoff.Retry(func() error {
		timeout, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		_, err = clients.CopyFile(timeout, job.SignedSourceURL, localSourceFile.Name(), "", job.RequestID)
		if err != nil {
			return fmt.Errorf("failed to copy file (%s) locally for segmenting: %s", log.RedactURL(job.SignedSourceURL), err)
		}
		return nil
	}, retries(6)); err != nil {
		return "", err
	}

	// Begin Segmenting
	log.Log(job.RequestID, "Beginning segmenting via FFMPEG/Livepeer pipeline")
	job.ReportProgress(clients.TranscodeStatusPreparing, 0.5)

	// FFMPEG fails when presented with a raw IP + Path type URL, so we prepend "http://" to it
	internalAddress := config.HTTPInternalAddress
	if !strings.HasPrefix(internalAddress, "http") {
		internalAddress = "http://" + internalAddress
	}

	destinationURL := fmt.Sprintf("%s/api/ffmpeg/%s/index.m3u8", internalAddress, job.StreamName)
	if err := video.Segment(localSourceFile.Name(), destinationURL, job.TargetSegmentSizeSecs); err != nil {
		return "", err
	}

	return localSourceFile.Name(), nil
}

func cleanUpLocalTmpFiles(dir string, filenamePattern string, maxAge time.Duration) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode().IsRegular() {
			if match, _ := filepath.Match(filenamePattern, info.Name()); match {
				if time.Since(info.ModTime()) > maxAge {
					err = os.Remove(path)
					if err != nil {
						return fmt.Errorf("error removing file %s: %w", path, err)
					}
					log.LogNoRequestID("Cleaned up file", "path", path, "filename", info.Name(), "age", info.ModTime())
				}
			}
		}
		return nil
	})
}

func toStr(URL *url.URL) string {
	if URL != nil {
		return URL.String()
	}
	return ""
}

func retries(retries uint64) backoff.BackOff {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 1 * time.Second
	backOff.MaxInterval = 30 * time.Second
	backOff.MaxElapsedTime = 0 // don't impose a timeout as part of the retries
	backOff.Reset()
	return backoff.WithMaxRetries(backOff, retries)
}

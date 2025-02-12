package clients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/go-tools/drivers"
)

var maxRetryInterval = 5 * time.Second

func DownloadOSURL(osURL string) (io.ReadCloser, error) {
	fileInfoReader, err := GetOSURL(osURL, "")
	if err != nil {
		return nil, err
	}
	return fileInfoReader.Body, nil
}

func NewTeeReadCloser(readCloser io.ReadCloser, writer io.Writer) io.ReadCloser {
	return &TeeReadCloser{
		reader: io.TeeReader(readCloser, writer),
		closer: readCloser,
	}
}

type TeeReadCloser struct {
	reader io.Reader
	closer io.Closer
}

func (tr *TeeReadCloser) Read(p []byte) (n int, err error) {
	return tr.reader.Read(p)
}

func (tr *TeeReadCloser) Close() error {
	return tr.closer.Close()
}

func newByteReporter(host, bucket string) *byteReporter {
	return &byteReporter{
		host:   host,
		bucket: bucket,
	}
}

type byteReporter struct {
	count  int64
	host   string
	bucket string
}

func (c *byteReporter) Write(p []byte) (int, error) {
	n := len(p)
	c.count += int64(n)
	if c.count%1024*1024 == 0 {
		// report count every 1MB, accept that accuracy may be up to 1MB incorrect
		metrics.Metrics.ObjectStoreClient.BytesTransferred.WithLabelValues(c.host, "read", c.bucket).Add(float64(c.count))
		c.count = 0
	}

	return n, nil
}

func GetOSURL(osURL, byteRange string) (*drivers.FileInfoReader, error) {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return nil, catErrs.Unretriable(fmt.Errorf("failed to parse OS URL %q: %w", log.RedactURL(osURL), err))
	}

	start := time.Now()

	sess := storageDriver.NewSession("")
	info := sess.GetInfo()
	var host, bucket string
	if info != nil && info.S3Info != nil {
		host = info.S3Info.Host
		bucket = info.S3Info.Bucket
	}
	var fileInfoReader *drivers.FileInfoReader
	if byteRange == "" {
		fileInfoReader, err = sess.ReadData(context.Background(), "")
	} else {
		fileInfoReader, err = sess.ReadDataRange(context.Background(), "", byteRange)
	}

	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(host, "read", bucket).Inc()

		if errors.Is(err, drivers.ErrNotExist) {
			return nil, catErrs.NewObjectNotFoundError("not found in OS", err)
		}
		return nil, fmt.Errorf("failed to read from OS URL %q: %w", log.RedactURL(osURL), err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(host, "read", bucket).Observe(duration.Seconds())

	// wrap the ReadCloser in a TeeReadCloser so that we can record the egress bytes
	fileInfoReader.Body = NewTeeReadCloser(fileInfoReader.Body, newByteReporter(host, bucket))

	return fileInfoReader, nil
}

func UploadToOSURL(osURL, filename string, data io.Reader, timeout time.Duration) error {
	return UploadToOSURLFields(osURL, filename, data, timeout, nil)
}

func UploadToOSURLFields(osURL, filename string, data io.Reader, timeout time.Duration, fields *drivers.FileProperties) error {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return fmt.Errorf("failed to parse OS URL %q: %s", log.RedactURL(osURL), err)
	}
	start := time.Now()

	var host, bucket string
	sess := storageDriver.NewSession("")
	info := sess.GetInfo()
	if info != nil && info.S3Info != nil {
		host = info.S3Info.Host
		bucket = info.S3Info.Bucket
	}

	_, err = sess.SaveData(context.Background(), filename, data, fields, timeout)

	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(host, "write", bucket).Inc()
		return fmt.Errorf("failed to write to OS URL %q: %s", log.RedactURL(osURL+"/"+filename), err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(host, "write", bucket).Observe(duration.Seconds())

	return nil
}

func ListOSURL(ctx context.Context, osURL string) (drivers.PageInfo, error) {
	osDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return nil, fmt.Errorf("unexpected error parsing internal driver URL: %w", err)
	}
	os := osDriver.NewSession("")

	page, err := os.ListFiles(ctx, "", "")
	if err != nil {
		return nil, fmt.Errorf("error listing files: %w", err)
	}

	return page, nil
}

func Publish(hlsTarget string, mp4Target string) (string, string, error) {
	var hlsPlaybackBaseURL, hlsRel string
	if hlsTarget != "" {
		hlsPubUrl, err := url.Parse(hlsTarget)
		if err != nil {
			return "", "", err
		}
		hlsRel = hlsPubUrl.Path
		hlsPlaybackBaseURL, err = publishDriverSession(hlsTarget, hlsRel)
		if err != nil {
			return "", "", err
		}
	}

	var mp4PlaybackBaseURL string
	if mp4Target != "" {
		mp4TargetURL, err := url.Parse(mp4Target)
		if err != nil {
			return "", "", err
		}
		mp4Rel := mp4TargetURL.Path
		hlsPubUrlNoPath, _ := url.Parse(hlsTarget)
		hlsPubUrlNoPath.Path = ""
		mp4PubUrlNoPath, _ := url.Parse(mp4TargetURL.String())
		mp4PubUrlNoPath.Path = ""
		if hlsPubUrlNoPath.String() == mp4PubUrlNoPath.String() {
			// Do not publish the second time, just reuse playbackBaseURL from HLS
			mp4PlaybackBaseURL = strings.ReplaceAll(hlsPlaybackBaseURL, hlsRel, mp4Rel)
		} else {
			mp4PlaybackBaseURL, err = publishDriverSession(mp4Target, mp4Rel)
			if err != nil {
				return "", "", err
			}
		}
	}
	return hlsPlaybackBaseURL, mp4PlaybackBaseURL, nil
}

// publishDriverSession tries to publish the given osUrl and returns a publicly accessible video URL.
// If driver supports `Publish()`, e.g. web3.storage, then return the path to the video.
// If driver does not support `Publish()`, e.g. S3, then return the input osUrl, video should be accessible with osUrl.
// In case of any other error, return an empty string and an error.
func publishDriverSession(osUrl string, relPath string) (string, error) {
	osDriver, err := drivers.ParseOSURL(osUrl, true)
	if err != nil {
		return "", err
	}

	var videoUrl string
	err = backoff.Retry(func() error {
		var baseUrl string
		baseUrl, err = osDriver.Publish(context.Background())
		if err == drivers.ErrNotSupported {
			// driver does not support Publish(), video will be accessible with osUrl
			videoUrl = osUrl
			return nil
		} else if err != nil {
			// error while publishing the video
			return err
		}
		videoUrl, err = url.JoinPath(baseUrl, relPath)
		return nil
	}, backoff.WithMaxRetries(newExponentialBackOffExecutor(), 5))

	if err != nil {
		return "", fmt.Errorf("failed to publish video, err: %v", err)
	}

	// driver supports Publish() and returned a video url, return it joined with the relative path
	return videoUrl, nil
}

func newExponentialBackOffExecutor() *backoff.ExponentialBackOff {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 200 * time.Millisecond
	backOff.MaxInterval = maxRetryInterval
	backOff.MaxElapsedTime = 0 // don't impose a timeout as part of the retries
	backOff.Reset()
	return backOff
}

func UploadRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(newExponentialBackOffExecutor(), 5)
}

func SignURL(u *url.URL) (string, error) {
	if u.Scheme == "" || u.Scheme == "file" || u.Scheme == "http" || u.Scheme == "https" { // not an OS url
		return u.String(), nil
	}
	driver, err := drivers.ParseOSURL(u.String(), true)
	if err != nil {
		return "", fmt.Errorf("failed to parse OS url: %w", err)
	}

	sess := driver.NewSession("")
	signedURL, err := sess.Presign("", PresignDuration)
	if err != nil {
		return "", fmt.Errorf("failed to generate signed url: %w", err)
	}
	return signedURL, nil
}

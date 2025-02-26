package metrics

import (
	"github.com/livepeer/catalyst-api/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ClientMetrics struct {
	RetryCount       *prometheus.GaugeVec
	FailureCount     *prometheus.CounterVec
	RequestDuration  *prometheus.HistogramVec
	BytesTransferred *prometheus.CounterVec
}

type VODPipelineMetrics struct {
	Count              *prometheus.CounterVec
	Duration           *prometheus.SummaryVec
	SourceSegments     *prometheus.SummaryVec
	TranscodedSegments *prometheus.CounterVec
	SourceBytes        *prometheus.SummaryVec
	SourceDuration     *prometheus.SummaryVec
}

type AnalyticsMetrics struct {
	AnalyticsLogsPlaytimeMs   *prometheus.SummaryVec
	AnalyticsLogsBufferTimeMs *prometheus.SummaryVec

	LogProcessorWriteErrors prometheus.Counter
	AnalyticsLogsErrors     prometheus.Counter
	KafkaWriteErrors        prometheus.Counter
	KafkaWriteMessages      prometheus.Counter
	KafkaWriteRetries       prometheus.Counter
	KafkaWriteAvgTime       prometheus.Summary
}

type CatalystAPIMetrics struct {
	Version                           *prometheus.CounterVec
	UploadVODRequestCount             prometheus.Counter
	UploadVODRequestDurationSec       *prometheus.SummaryVec
	TranscodeSegmentDurationSec       prometheus.Histogram
	PlaybackRequestDurationSec        *prometheus.SummaryVec
	CDNRedirectCount                  *prometheus.CounterVec
	CDNRedirectWebRTC406              *prometheus.CounterVec
	UserEventBufferSize               prometheus.Gauge
	MemberEventBufferSize             prometheus.Gauge
	SerfEventBufferSize               prometheus.Gauge
	AccessControlRequestCount         *prometheus.CounterVec
	AccessControlRequestDurationSec   *prometheus.SummaryVec
	CatabalancerRequestDurationSec    *prometheus.HistogramVec
	CatabalancerSendMetricDurationSec prometheus.Histogram
	CatabalancerSendDBDurationSec     *prometheus.HistogramVec

	JobsInFlight         prometheus.Gauge
	HTTPRequestsInFlight prometheus.Gauge

	TranscodingStatusUpdate ClientMetrics
	BroadcasterClient       ClientMetrics
	MistClient              ClientMetrics
	ObjectStoreClient       ClientMetrics

	VODPipelineMetrics VODPipelineMetrics

	AnalyticsMetrics AnalyticsMetrics
}

var vodLabels = []string{"source_codec_video", "source_codec_audio", "pipeline", "catalyst_region", "num_profiles", "stage", "version", "is_fallback_mode", "is_livepeer_supported", "is_clip", "is_thumbs"}

func NewMetrics() *CatalystAPIMetrics {
	m := &CatalystAPIMetrics{
		// Fired once on startup to let us check which version of this service we're running
		Version: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "version",
			Help: "Current Git SHA / Tag that's running. Incremented once on app startup.",
		}, []string{"app", "version"}),

		// Node-level Capacity Metrics
		JobsInFlight: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "jobs_in_flight",
			Help: "A count of the jobs in flight",
		}),
		HTTPRequestsInFlight: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "A count of the http requests in flight",
		}),
		UserEventBufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "user_event_buffer_size",
			Help: "A count of the user events currently held in the buffer",
		}),
		MemberEventBufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "member_event_buffer_size",
			Help: "A count of the member events currently held in the buffer",
		}),
		SerfEventBufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "serf_event_buffer_size",
			Help: "A count of the serf events currently held in the buffer",
		}),

		// /api/vod request metrics
		UploadVODRequestCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: "upload_vod_request_count",
			Help: "The total number of requests to /api/vod",
		}),
		UploadVODRequestDurationSec: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "upload_vod_request_duration_seconds",
			Help: "The latency of the requests made to /api/vod in seconds broken up by success and status code",
		}, []string{"success", "status_code", "version"}),
		TranscodeSegmentDurationSec: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "transcode_segment_duration_seconds",
			Help:    "Time taken to transcode a segment",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		PlaybackRequestDurationSec: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "catalyst_playback_request_duration_seconds",
			Help: "The latency of the requests made to /asset/hls in seconds broken up by success and status code",
		}, []string{"success", "status_code", "version"}),
		CDNRedirectCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "cdn_redirect_count",
			Help: "Number of requests redirected to CDN",
		}, []string{"playbackID"}),
		CDNRedirectWebRTC406: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "cdn_redirect_webrtc_406",
			Help: "Number of WebRTC requests rejected with HTTP 406 because of playback should be seved from external CDN",
		}, []string{"playbackID"}),
		AccessControlRequestCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "access_control_request_count",
			Help: "The total number of access control requests",
		}, []string{"allowed", "playbackID"}),
		AccessControlRequestDurationSec: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "access_control_request_duration_seconds",
			Help: "The latency of the access control requests",
		}, []string{"allowed", "playbackID"}),
		CatabalancerRequestDurationSec: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "catabalancer_request_duration",
			Help:    "Time taken for catabalancer load balancing requests",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"success", "request_type", "mist_match", "background"}),
		CatabalancerSendMetricDurationSec: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "catabalancer_send_metric_duration",
			Help:    "Total time taken to compile and send catabalancer node metrics",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		CatabalancerSendDBDurationSec: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "catabalancer_send_db_duration",
			Help:    "Time taken to send catabalancer node metrics to the DB",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"success"}),

		// Clients metrics
		TranscodingStatusUpdate: ClientMetrics{
			RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "transcoding_status_update_retry_count",
				Help: "The number of retried transcoding status updates",
			}, []string{"host"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "transcoding_status_update_failure_count",
				Help: "The total number of failed transcoding status updates",
			}, []string{"host", "status_code"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "transcoding_status_update_duration",
				Help:    "Time taken to send transcoding status updates",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host"}),
		},

		BroadcasterClient: ClientMetrics{
			RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "broadcaster_client_retry_count",
				Help: "The number of retried broadcaster requests",
			}, []string{"host"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "broadcaster_client_failure_count",
				Help: "The total number of failed broadcaster requests",
			}, []string{"host", "status_code"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "broadcaster_client_request_duration",
				Help:    "Time taken to send broadcaster requests",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host"}),
		},

		MistClient: ClientMetrics{
			RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "mist_client_retry_count",
				Help: "The number of retried mist requests",
			}, []string{"host"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "mist_client_failure_count",
				Help: "The total number of failed mist requests",
			}, []string{"host", "status_code"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "mist_client_request_duration",
				Help:    "Time taken to send mist requests",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host"}),
		},

		ObjectStoreClient: ClientMetrics{
			RetryCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "object_store_retry_count",
				Help: "The number of retried object store requests",
			}, []string{"host", "operation", "bucket"}),
			FailureCount: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "object_store_failure_count",
				Help: "The total number of failed object store requests",
			}, []string{"host", "operation", "bucket"}),
			RequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "object_store_request_duration",
				Help:    "Time taken to send object store requests",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			}, []string{"host", "operation", "bucket"}),
			BytesTransferred: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "object_store_bytes_transferred",
				Help: "The total number of bytes transferred from storage",
			}, []string{"host", "operation", "bucket"}),
		},

		VODPipelineMetrics: VODPipelineMetrics{
			Count: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "vod_count",
				Help: "Number of VOD pipeline started",
			}, vodLabels),
			Duration: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "vod_duration",
				Help: "Time taken till the VOD job is completed",
			}, vodLabels),
			SourceSegments: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "vod_source_segments",
				Help: "Number of segments of the source asset",
			}, vodLabels),
			TranscodedSegments: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "vod_transcoded_segments",
				Help: "Number of segments of rendition asset",
			}, vodLabels),
			SourceBytes: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "vod_source_bytes",
				Help: "Size of the source asset",
			}, vodLabels),
			SourceDuration: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "vod_source_duration",
				Help: "Duration of the source asset",
			}, vodLabels),
		},

		AnalyticsMetrics: AnalyticsMetrics{
			AnalyticsLogsPlaytimeMs: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "analytics_logs_playtime_ms",
				Help: "Playtime in milliseconds gathered from Analytics Logs",
			}, []string{"playback_id", "user_id", "project_id", "continent"}),
			AnalyticsLogsBufferTimeMs: promauto.NewSummaryVec(prometheus.SummaryOpts{
				Name: "analytics_logs_buffer_time_ms",
				Help: "Buffer time in milliseconds gathered from Analytics Logs",
			}, []string{"playback_id", "user_id", "project_id", "continent"}),
			LogProcessorWriteErrors: promauto.NewCounter(prometheus.CounterOpts{
				Name: "log_processor_write_errors",
				Help: "Number of log processors errors while writing to Kafka",
			}),
			AnalyticsLogsErrors: promauto.NewCounter(prometheus.CounterOpts{
				Name: "analytics_logs_errors",
				Help: "Number of errors while processing analytics logs",
			}),
			KafkaWriteErrors: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_write_errors",
				Help: "Number of errors while writing to Kafka",
			}),
			KafkaWriteMessages: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_write_messages",
				Help: "Number of messages written to Kafka",
			}),
			KafkaWriteRetries: promauto.NewCounter(prometheus.CounterOpts{
				Name: "kafka_write_retries",
				Help: "Number of retries while writing to Kafka",
			}),
			KafkaWriteAvgTime: promauto.NewSummary(prometheus.SummaryOpts{
				Name: "kafka_write_avg_time",
				Help: "Average time taken to write to Kafka",
			}),
		},
	}

	// Fire a metric a single time to let us track the version of the app we're using
	m.Version.WithLabelValues("catalyst-api", config.Version).Inc()

	return m
}

var Metrics = NewMetrics()

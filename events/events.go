package events

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/livepeer/catalyst-api/balancer/catabalancer"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
)

const streamEventResource = "stream"
const nukeEventResource = "nuke"
const stopSessionsEventResource = "stopSessions"
const nodeUpdateEventResource = "nodeUpdate"

type Event interface{}

type GenericEvent struct {
	Resource string `json:"resource"`
}

type StreamEvent struct {
	Resource   string `json:"resource"`
	PlaybackID string `json:"playback_id"`
}

type NukeEvent struct {
	Resource   string `json:"resource"`
	PlaybackID string `json:"playback_id"`
}

type StopSessionsEvent struct {
	Resource   string `json:"resource"`
	PlaybackID string `json:"playback_id"`
}

func Unmarshal(payload []byte) (Event, error) {
	var generic GenericEvent
	err := json.Unmarshal(payload, &generic)
	if err != nil {
		return nil, err
	}
	switch generic.Resource {
	case streamEventResource:
		event := &StreamEvent{}
		err := json.Unmarshal(payload, event)
		if err != nil {
			return nil, err
		}
		return event, nil
	case nukeEventResource:
		event := &NukeEvent{}
		err := json.Unmarshal(payload, event)
		if err != nil {
			return nil, err
		}
		return event, nil
	case stopSessionsEventResource:
		event := &StopSessionsEvent{}
		err := json.Unmarshal(payload, event)
		if err != nil {
			return nil, err
		}
		return event, nil
	case nodeUpdateEventResource:
		event := &catabalancer.NodeUpdateEvent{}
		err := json.Unmarshal(payload, event)
		if err != nil {
			return nil, err
		}
		return event, nil
	}
	return nil, fmt.Errorf("unable to unmarshal event, unknown resource '%s'", generic.Resource)
}

func StartMetricSending(nodeName string, latitude float64, longitude float64, mist clients.MistAPIClient, nodeStatsDB *sql.DB) {
	ticker := time.NewTicker(catabalancer.UpdateNodeStatsEvery)
	go func() {
		for range ticker.C {
			sysusage, err := catabalancer.GetSystemUsage()
			if err != nil {
				log.LogNoRequestID("catabalancer failed to get sys usage", "err", err)
				continue
			}

			event := catabalancer.NodeUpdateEvent{
				Resource: nodeUpdateEventResource,
				NodeID:   nodeName,
				NodeMetrics: catabalancer.NodeMetrics{
					CPUUsagePercentage:       sysusage.CPUUsagePercentage,
					RAMUsagePercentage:       sysusage.RAMUsagePercentage,
					BandwidthUsagePercentage: sysusage.BWUsagePercentage,
					LoadAvg:                  sysusage.LoadAvg.Load5Min,
					GeoLatitude:              latitude,
					GeoLongitude:             longitude,
					Timestamp:                time.Now(),
				},
			}

			if mist != nil {
				mistState, err := mist.GetState()
				if err != nil {
					log.LogNoRequestID("catabalancer failed to get mist state", "err", err)
					continue
				}

				var nonIngestStreams, ingestStreams []string
				for streamID := range mistState.ActiveStreams {
					if mistState.IsIngestStream(streamID) {
						ingestStreams = append(ingestStreams, streamID)
					} else {
						nonIngestStreams = append(nonIngestStreams, streamID)
					}
				}
				event.SetStreams(nonIngestStreams, ingestStreams)
			}

			payload, err := json.Marshal(event)
			if err != nil {
				log.LogNoRequestID("catabalancer failed to marhsal node update", "err", err)
				continue
			}

			insertStatement := `insert into "node_stats"(
                            "node_id",
                            "stats"
                            ) values($1, $2)
							ON CONFLICT (node_id)
							DO UPDATE SET stats = EXCLUDED.stats;`
			_, err = nodeStatsDB.Exec(
				insertStatement,
				nodeName,
				payload,
			)
			if err != nil {
				log.LogNoRequestID("error writing postgres node stats", "err", err)
				continue
			}
		}
	}()
}

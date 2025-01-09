package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/hashicorp/serf/serf"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/balancer/catabalancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/events"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/xeipuuv/gojsonschema"
	"io"
	"net/http"
	"strings"
)

type EventsHandlersCollection struct {
	cluster cluster.Cluster

	mapic mistapiconnector.IMac
	bal   balancer.Balancer

	eventsEndpoint string
}

type Event struct {
	Resource   string `json:"resource"`
	PlaybackID string `json:"playback_id"`
}

func NewEventsHandlersCollection(cluster cluster.Cluster, mapic mistapiconnector.IMac, bal balancer.Balancer, eventsEndpoint string) *EventsHandlersCollection {
	return &EventsHandlersCollection{
		cluster:        cluster,
		mapic:          mapic,
		bal:            bal,
		eventsEndpoint: eventsEndpoint,
	}
}

// Events is a handler called by Catalyst API which forwards events from Studio API.
// Used to, e.g., refresh a stream or nuke a stream.
// This event is then propagated to all Serf nodes and then forwarded to catalyst-api and handled by ReceiveUserEvent().
func (d *EventsHandlersCollection) Events() httprouter.Handle {
	schema := inputSchemasCompiled["Event"]
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot read payload", err)
			return
		}
		result, err := schema.Validate(gojsonschema.NewBytesLoader(payload))
		if err != nil {
			errors.WriteHTTPBadRequest(w, "Cannot validate payload", err)
			return
		}
		if !result.Valid() {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", fmt.Errorf("%s", result.Errors()))
			return
		}
		var event Event
		if err := json.Unmarshal(payload, &event); err != nil {
			errors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}

		err = d.cluster.BroadcastEvent(serf.UserEvent{
			Name:     fmt.Sprintf("%s-%s", event.Resource, event.PlaybackID),
			Payload:  payload,
			Coalesce: true,
		})

		if err != nil {
			errors.WriteHTTPInternalServerError(w, "Cannot process event", err)
			return
		}
	}
}

// ReceiveUserEvent is a handler to receive Serf events from Catalyst.
// The idea is that:
// 1. Studio API sends an event to Catalyst (received by Events() handler)
// 2. Events() handler propagates the event to all Serf nodes
// 3. Each Serf node sends tne event to its corresponding catalyst-api instance (to the ReceiveUserEvent() handler)
// 4. ReceiveUserEvent() handler processes the event
func (c *EventsHandlersCollection) ReceiveUserEvent() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		userEventPayload, err := io.ReadAll(r.Body)
		if err != nil {
			glog.Errorf("cannot read payload: %s", err)
			return
		}
		e, err := events.Unmarshal(userEventPayload)
		if err != nil {
			glog.Errorf("cannot unmarshal received serf event %v: %s", userEventPayload, err)
			return
		}
		switch event := e.(type) {
		case *events.StreamEvent:
			glog.V(5).Infof("received serf StreamEvent: %v", event.PlaybackID)
			c.mapic.RefreshStreamIfNeeded(event.PlaybackID)
		case *events.NukeEvent:
			glog.V(5).Infof("received serf NukeEvent: %v", event.PlaybackID)
			c.mapic.NukeStream(event.PlaybackID)
			return
		case *events.StopSessionsEvent:
			glog.V(5).Infof("received serf StopSessionsEvent: %v", event.PlaybackID)
			c.mapic.StopSessions(event.PlaybackID)
			return
		case *catabalancer.NodeUpdateEvent:
			if glog.V(5) {
				glog.Infof("received serf NodeUpdateEvent. Node: %s. Length: %d bytes. Ingest Streams: %v. Non-Ingest Streams: %v", event.NodeID, len(userEventPayload), strings.Join(event.GetIngestStreams(), ","), strings.Join(event.GetStreams(), ","))
			}

			c.bal.UpdateNodes(event.NodeID, event.NodeMetrics)
			for _, stream := range event.GetStreams() {
				c.bal.UpdateStreams(event.NodeID, stream, false)
			}
			for _, stream := range event.GetIngestStreams() {
				c.bal.UpdateStreams(event.NodeID, stream, true)
			}
		default:
			glog.Errorf("unsupported serf event: %v", e)
		}
	}
}

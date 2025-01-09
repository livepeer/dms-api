package events

import (
	"encoding/json"
	"testing"

	"github.com/livepeer/catalyst-api/balancer/catabalancer"
	"github.com/stretchr/testify/require"
)

func TestItCanHandleStreamEvents(t *testing.T) {
	payload := []byte(`{"resource": "stream", "playback_id": "abc123"}`)
	e, err := Unmarshal(payload)
	require.NoError(t, err)
	event, ok := e.(*StreamEvent)
	require.True(t, ok)
	require.Equal(t, event.PlaybackID, "abc123")
}

func TestItCanHandleNukeEvents(t *testing.T) {
	payload := []byte(`{"resource": "nuke", "playback_id": "abc123"}`)
	e, err := Unmarshal(payload)
	require.NoError(t, err)
	event, ok := e.(*NukeEvent)
	require.True(t, ok)
	require.Equal(t, event.PlaybackID, "abc123")
}

func TestItFailsUnknownEvents(t *testing.T) {
	payload := []byte(`{"resource": "not-real-thing"}`)
	_, err := Unmarshal(payload)
	require.Error(t, err)
}

func TestItFailsBadJSON(t *testing.T) {
	payload := []byte(`this is not valid JSON text`)
	_, err := Unmarshal(payload)
	require.Error(t, err)
}

func TestItFailsBadShapes(t *testing.T) {
	payloads := [][]byte{
		[]byte(`{"resource": "stream", "playback_id": 5.5}`),
		[]byte(`{"resource": "nuke", "playback_id": 5.5}`),
	}
	for _, payload := range payloads {
		_, err := Unmarshal(payload)
		require.Error(t, err)
	}
}

func TestItCanMarshalAndUnMarshalStreamIDs(t *testing.T) {
	n := catabalancer.NodeUpdateEvent{}
	n.SetStreams([]string{"noningest1", "noningest2"}, []string{"ingest1", "ingest2"})
	jsonBytes, err := json.Marshal(n)
	require.NoError(t, err)

	var n2 catabalancer.NodeUpdateEvent
	require.NoError(t, json.Unmarshal(jsonBytes, &n2))

	require.Equal(t, []string{"noningest1", "noningest2"}, n2.GetStreams())
	require.Equal(t, []string{"ingest1", "ingest2"}, n2.GetIngestStreams())
}

func TestItCanMarshalAndUnMarshalStreamIDsWithNoIngestStreams(t *testing.T) {
	n := catabalancer.NodeUpdateEvent{}
	n.SetStreams([]string{"noningest1", "noningest2"}, []string{})
	jsonBytes, err := json.Marshal(n)
	require.NoError(t, err)

	var n2 catabalancer.NodeUpdateEvent
	require.NoError(t, json.Unmarshal(jsonBytes, &n2))

	require.Equal(t, []string{"noningest1", "noningest2"}, n2.GetStreams())
	require.Equal(t, []string{}, n2.GetIngestStreams())
}

func TestItCanMarshalAndUnMarshalStreamIDsWithNoNonIngestStreams(t *testing.T) {
	n := catabalancer.NodeUpdateEvent{}
	n.SetStreams([]string{}, []string{"ingest1", "ingest2"})
	jsonBytes, err := json.Marshal(n)
	require.NoError(t, err)

	var n2 catabalancer.NodeUpdateEvent
	require.NoError(t, json.Unmarshal(jsonBytes, &n2))

	require.Equal(t, []string{}, n2.GetStreams())
	require.Equal(t, []string{"ingest1", "ingest2"}, n2.GetIngestStreams())
}

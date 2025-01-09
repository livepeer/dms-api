package catabalancer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/livepeer/catalyst-api/cluster"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var CPUOverloadedNode = ScoredNode{
	Node: Node{Name: "cpu_overload"},
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       100,
		RAMUsagePercentage:       0,
		BandwidthUsagePercentage: 0,
	},
}

var RAMOverloadedNode = ScoredNode{
	Node: Node{Name: "mem_overload"},
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       0,
		RAMUsagePercentage:       100,
		BandwidthUsagePercentage: 0,
	},
}

var BandwidthOverloadedNode = ScoredNode{
	Node: Node{Name: "bw_overload"},
	NodeMetrics: NodeMetrics{
		CPUUsagePercentage:       0,
		RAMUsagePercentage:       0,
		BandwidthUsagePercentage: 100,
	},
}

func mockDB(t *testing.T) *sql.DB {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		mock.ExpectQuery("SELECT stats FROM node_stats").
			WillReturnRows(sqlmock.NewRows([]string{"stats"}).AddRow("{}"))

	}
	return db
}

func TestItReturnsItselfWhenNoOtherNodesPresent(t *testing.T) {
	c := NewBalancer("me", time.Second, time.Second, mockDB(t))
	nodeName, prefix, err := c.GetBestNode(context.Background(), nil, "playbackID", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "me", nodeName)
	require.Equal(t, "video+playbackID", prefix)
}

func TestStaleNodes(t *testing.T) {
	c := NewBalancer("me", time.Second, time.Second, mockDB(t))
	err := c.UpdateMembers(context.Background(), []cluster.Member{{Name: "node1"}})
	require.NoError(t, err)

	// node is stale, old timestamp
	c.UpdateNodes("node1", NodeMetrics{})
	c.metricTimeout = -5 * time.Second
	nodeName, prefix, err := c.GetBestNode(context.Background(), nil, "playbackID", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "me", nodeName) // we expect node1 to be ignored
	require.Equal(t, "video+playbackID", prefix)

	// node is fresh, recent timestamp
	c.UpdateNodes("node1", NodeMetrics{})
	c.metricTimeout = 5 * time.Second
	nodeName, prefix, err = c.GetBestNode(context.Background(), nil, "playbackID", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "node1", nodeName) // we expect node1 this time
	require.Equal(t, "video+playbackID", prefix)
}

func TestItReturnsBadNodeIfOnlyAvailable(t *testing.T) {
	selectionNodes := []ScoredNode{
		CPUOverloadedNode,
	}

	n, err := SelectNode(selectionNodes, "some-stream-id", 0, 0)
	require.NoError(t, err)
	require.Equal(t, CPUOverloadedNode.Node, n)
}

func TestItDoesntChooseOverloadedNodes(t *testing.T) {
	expectedNode := ScoredNode{
		Node: Node{Name: "expected"},
		NodeMetrics: NodeMetrics{
			CPUUsagePercentage:       10,
			RAMUsagePercentage:       10,
			BandwidthUsagePercentage: 10,
		},
	}
	selectionNodes := []ScoredNode{
		CPUOverloadedNode,
		RAMOverloadedNode,
		expectedNode,
		BandwidthOverloadedNode,
	}

	n, err := SelectNode(selectionNodes, "some-stream-id", 0, 0)
	require.NoError(t, err)
	require.Equal(t, expectedNode.Node, n)
}

func TestItChoosesRandomlyFromTheBestNodes(t *testing.T) {
	selectionNodes := []ScoredNode{
		{Node: Node{Name: "good-node-1"}},
		{Node: Node{Name: "good-node-2"}},
		{Node: Node{Name: "good-node-3"}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{Node: Node{Name: "good-node-4"}},
		{Node: Node{Name: "good-node-5"}},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "some-stream-id", 0, 0)
		require.NoError(t, err)
		foundNodes[n.Name] = true
	}
	require.Equal(
		t,
		map[string]bool{
			"good-node-1": true,
			"good-node-2": true,
			"good-node-3": true,
			"good-node-4": true,
			"good-node-5": true,
		},
		foundNodes,
	)
}

// If we have geographically local servers that aren't overloaded _and_ already have the stream replicated to them
// then that's the ideal situation and we should prefer that over everything else
func TestItPrefersLocalUnloadedServersWithStreamAlready(t *testing.T) {
	selectionNodes := []ScoredNode{
		{Node: Node{Name: "good-node-1"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}},
		{Node: Node{Name: "good-node-2"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}},
		{Node: Node{Name: "good-node-3"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", Timestamp: time.Now()}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{Node: Node{Name: "good-node-4"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", Timestamp: time.Now()}}},
		{Node: Node{Name: "good-node-5"}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "stream-name-we-want", 0, 0)
		require.NoError(t, err)
		foundNodes[n.Name] = true
	}
	require.Equal(
		t,
		map[string]bool{
			"good-node-1": true,
			"good-node-2": true,
			"good-node-5": true,
		},
		foundNodes,
	)
}

// If the geographically local servers that have the stream already are overloaded, prefer local ones
// that aren't overloaded, even if they haven't had the stream replicated to them yet
func TestItPrefersLocalUnloadedServersWithoutStream(t *testing.T) {
	selectionNodes := []ScoredNode{
		{Node: Node{Name: "local-with-stream-high-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
		{Node: Node{Name: "local-without-stream"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", Timestamp: time.Now()}}},
		{Node: Node{Name: "local-without-stream-2"}, Streams: map[string]Stream{"other-steam-name": {ID: "other-steam-name", Timestamp: time.Now()}}},
		RAMOverloadedNode,
		BandwidthOverloadedNode,
		{Node: Node{Name: "far-with-stream"}, NodeMetrics: NodeMetrics{GeoLatitude: 100, GeoLongitude: 100}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
		{Node: Node{Name: "far-without-stream"}, NodeMetrics: NodeMetrics{GeoLatitude: 100, GeoLongitude: 100}, Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want"}}},
	}

	foundNodes := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, err := SelectNode(selectionNodes, "stream-name-we-want", 0, 0)
		require.NoError(t, err)
		foundNodes[n.Name] = true
	}
	require.Equal(
		t,
		map[string]bool{
			"local-without-stream":   true,
			"local-without-stream-2": true,
		},
		foundNodes,
	)
}

func TestItChoosesLeastBad(t *testing.T) {
	requestLatitude, requestLongitude := 51.7520, 1.2577 // Oxford
	highCPULocal := ScoredNode{Node: Node{Name: "local-high-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90, GeoLatitude: requestLatitude, GeoLongitude: requestLongitude}}
	highCPUWithStream := ScoredNode{Node: Node{Name: "far-high-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90, GeoLatitude: 1.35, GeoLongitude: 103.82},
		Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}} // Sin
	highCPU := ScoredNode{Node: Node{Name: "far-medium-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 90, GeoLatitude: 1.35, GeoLongitude: 103.82}}    // Sin
	mediumCPU := ScoredNode{Node: Node{Name: "far-medium-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82}}  // Sin
	lowCPU := ScoredNode{Node: Node{Name: "far-low-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 40, GeoLatitude: 1.35, GeoLongitude: 103.82}}        // Sin
	lowCPUOkDistance := ScoredNode{Node: Node{Name: "low-cpu"}, NodeMetrics: NodeMetrics{CPUUsagePercentage: 40, GeoLatitude: 41.88, GeoLongitude: -87.63}} // Mdw
	mediumMem := ScoredNode{Node: Node{Name: "far-medium-mem"}, NodeMetrics: NodeMetrics{RAMUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82}}  // Sin
	mediumMemWithStream := ScoredNode{Node: Node{Name: "far-medium-mem-with-stream"}, NodeMetrics: NodeMetrics{RAMUsagePercentage: 80, GeoLatitude: 1.35, GeoLongitude: 103.82},
		Streams: map[string]Stream{"stream-name-we-want": {ID: "stream-name-we-want", Timestamp: time.Now()}}} // Sin

	tests := []struct {
		name  string
		nodes []ScoredNode
		want  []ScoredNode
	}{
		{
			name:  "it sorts on resource utilisation",
			nodes: []ScoredNode{highCPULocal, mediumCPU, lowCPU, highCPU},
			want: []ScoredNode{
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
				scores(mediumCPU, ScoredNode{Score: 1, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "it sorts on mixed resource utilisation",
			nodes: []ScoredNode{highCPULocal, lowCPU, mediumMem},
			want: []ScoredNode{
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
				scores(mediumMem, ScoredNode{Score: 1, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "high CPU node picked last",
			nodes: []ScoredNode{highCPULocal, lowCPU, highCPU},
			want: []ScoredNode{
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
				scores(highCPU, ScoredNode{Score: 0, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "okay distance unloaded node",
			nodes: []ScoredNode{highCPULocal, mediumCPU, lowCPUOkDistance},
			want: []ScoredNode{
				scores(lowCPUOkDistance, ScoredNode{Score: 3, GeoScore: 1, GeoDistance: 6424}),
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(mediumCPU, ScoredNode{Score: 1, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "medium loaded node with the stream",
			nodes: []ScoredNode{highCPULocal, lowCPU, mediumMem, mediumMemWithStream},
			want: []ScoredNode{
				scores(mediumMemWithStream, ScoredNode{Score: 3, GeoScore: 0, GeoDistance: 10749, StreamScore: 2}),
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
			},
		},
		{
			name:  "high loaded node with the stream",
			nodes: []ScoredNode{highCPULocal, lowCPU, highCPUWithStream},
			want: []ScoredNode{
				scores(highCPULocal, ScoredNode{Score: 2, GeoScore: 2, GeoDistance: 0}),
				scores(lowCPU, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749}),
				scores(highCPUWithStream, ScoredNode{Score: 2, GeoScore: 0, GeoDistance: 10749, StreamScore: 2}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectTopNodes(tt.nodes, "stream-name-we-want", requestLatitude, requestLongitude, 3)
			require.Equal(t, tt.want, got)
		})
	}
}

func scores(node1 ScoredNode, node2 ScoredNode) ScoredNode {
	node1.Score = node2.Score
	node1.GeoScore = node2.GeoScore
	node1.GeoDistance = node2.GeoDistance
	node1.StreamScore = node2.StreamScore
	return node1
}

func TestSetMetrics(t *testing.T) {
	// simple check that node metrics make it through to the load balancing algo
	c := NewBalancer("", time.Second, time.Second, mockDB(t))
	err := c.UpdateMembers(context.Background(), []cluster.Member{{Name: "node1"}, {Name: "node2"}})
	require.NoError(t, err)

	c.UpdateNodes("node1", NodeMetrics{CPUUsagePercentage: 90})
	c.UpdateNodes("node2", NodeMetrics{CPUUsagePercentage: 0})

	node, fullPlaybackID, err := c.GetBestNode(context.Background(), nil, "1234", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "node2", node)
	require.Equal(t, "video+1234", fullPlaybackID)
}

func TestUnknownNode(t *testing.T) {
	// check that the node metrics call creates the unknown node
	c := NewBalancer("", time.Second, time.Second, mockDB(t))

	c.UpdateNodes("node1", NodeMetrics{CPUUsagePercentage: 90})
	c.UpdateNodes("bgw-node1", NodeMetrics{CPUUsagePercentage: 10})

	node, _, err := c.GetBestNode(context.Background(), nil, "1234", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, "node1", node)
}

func TestNoIngestStream(t *testing.T) {
	c := NewBalancer("", time.Second, time.Second, mockDB(t))
	// first test no nodes available
	c.UpdateNodes("id", NodeMetrics{})
	c.UpdateStreams("id", "stream", false)
	source, err := c.MistUtilLoadSource(context.Background(), "stream", "", "")
	require.EqualError(t, err, "catabalancer no node found for ingest stream: stream stale: false")
	require.Empty(t, source)

	// test node present but ingest stream not available
	err = c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: map[string]string{
			"dtsc": "dtsc://nodedtsc",
		},
	}})
	require.NoError(t, err)
	source, err = c.MistUtilLoadSource(context.Background(), "stream", "", "")
	require.EqualError(t, err, "catabalancer no node found for ingest stream: stream stale: false")
	require.Empty(t, source)
}

func TestMistUtilLoadSource(t *testing.T) {
	c := NewBalancer("", time.Second, time.Second, mockDB(t))
	err := c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: map[string]string{
			"dtsc": "dtsc://nodedtsc",
		},
	}})
	require.NoError(t, err)

	c.UpdateStreams("node", "stream", false)
	c.UpdateStreams("node", "ingest", true)
	require.Len(t, c.Streams, 1)
	require.Equal(t, "stream", c.Streams["node"]["stream"].ID)

	source, err := c.MistUtilLoadSource(context.Background(), "ingest", "", "")
	require.NoError(t, err)
	require.Equal(t, "dtsc://node", source)

	err = c.UpdateMembers(context.Background(), []cluster.Member{})
	require.NoError(t, err)
	require.Empty(t, c.IngestStreams)
	source, err = c.MistUtilLoadSource(context.Background(), "ingest", "", "")
	require.EqualError(t, err, "catabalancer no node found for ingest stream: ingest stale: false")
	require.Empty(t, source)
}

func TestStreamTimeout(t *testing.T) {
	c := NewBalancer("", time.Second, time.Second, mockDB(t))
	err := c.UpdateMembers(context.Background(), []cluster.Member{{
		Name: "node",
		Tags: map[string]string{
			"dtsc": "dtsc://nodedtsc",
		},
	}})
	require.NoError(t, err)
	c.UpdateNodes("node", NodeMetrics{})

	c.metricTimeout = 5 * time.Second
	c.ingestStreamTimeout = 5 * time.Second
	c.UpdateStreams("node", "video+stream", false)
	c.UpdateStreams("node", "video+ingest", true)

	// Source load balance call should work
	source, err := c.MistUtilLoadSource(context.Background(), "video+ingest", "", "")
	require.NoError(t, err)
	require.Equal(t, "dtsc://node", source)
	// Playback load balance calls should work
	nodes := selectTopNodes(c.createScoredNodes(), "stream", 0, 0, 1)
	require.Equal(t, int64(2), nodes[0].StreamScore)
	nodes = selectTopNodes(c.createScoredNodes(), "ingest", 0, 0, 1)
	require.Equal(t, int64(2), nodes[0].StreamScore)

	// test that a new ingest node will overwrite the previous data
	c.UpdateStreams("node2", "video+ingest", true)
	source, err = c.MistUtilLoadSource(context.Background(), "video+ingest", "", "")
	require.NoError(t, err)
	require.Equal(t, "dtsc://node2", source)

	c.metricTimeout = -5 * time.Second
	c.ingestStreamTimeout = -5 * time.Second
	// Re-run the same load balance calls as above, now no results should be returned due to expiry
	source, err = c.MistUtilLoadSource(context.Background(), "video+ingest", "", "")
	require.EqualError(t, err, "catabalancer no node found for ingest stream: video+ingest stale: true")
	require.Empty(t, source)

	nodes = selectTopNodes(c.createScoredNodes(), "stream", 0, 0, 1)
	require.Empty(t, nodes)
	nodes = selectTopNodes(c.createScoredNodes(), "ingest", 0, 0, 1)
	require.Empty(t, nodes)
}

// needs to be run with go test -race
func TestConcurrentUpdates(t *testing.T) {
	c := NewBalancer("", time.Second, time.Second, mockDB(t))

	err := c.UpdateMembers(context.Background(), []cluster.Member{{Name: "node"}})
	require.NoError(t, err)

	errGroup := errgroup.Group{}
	for i := 0; i < 100; i++ {
		i := i
		errGroup.Go(func() error {
			c.UpdateNodes("node", NodeMetrics{CPUUsagePercentage: float64(i)})
			return nil
		})
		errGroup.Go(func() error {
			c.UpdateStreams("node", strconv.Itoa(i), false)
			return nil
		})
	}
	// simulate some member updates at the same time
	for i := 0; i < 100; i++ {
		err = c.UpdateMembers(context.Background(), []cluster.Member{{Name: "node"}})
		require.NoError(t, err)
	}
	require.NoError(t, errGroup.Wait())
}

func TestSimulate(t *testing.T) {
	// update these values to test the lock contention with higher numbers of nodes etc
	nodeCount := 1
	streamsPerNode := 2
	expectedResponseTime := 100 * time.Millisecond
	loadBalanceCallCount := 5

	updateEvery := 5 * time.Second

	c := NewBalancer("node0", time.Second, time.Second, mockDB(t))
	var nodes []cluster.Member
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, cluster.Member{Name: fmt.Sprintf("node%d", i)})
	}
	err := c.UpdateMembers(context.Background(), nodes)
	require.NoError(t, err)

	for i := 0; i < nodeCount; i++ {
		i := i
		go (func() {
			duration := time.Duration(rand.Int63n(updateEvery.Milliseconds())) * time.Millisecond
			time.Sleep(duration)
			for {
				start := time.Now()
				c.UpdateNodes(fmt.Sprintf("node%d", i), NodeMetrics{CPUUsagePercentage: 10})
				require.LessOrEqual(t, time.Since(start), expectedResponseTime)
				for k := 0; k < streamsPerNode; k++ {
					start := time.Now()
					c.UpdateStreams(fmt.Sprintf("node%d", i), strconv.Itoa(k), false)
					require.LessOrEqual(t, time.Since(start), expectedResponseTime)
				}

				time.Sleep(updateEvery)
			}
		})()
	}

	// TODO add in updatemembers calls and source load balance calls

	for j := 0; j < loadBalanceCallCount; j++ {
		start := time.Now()
		_, _, err = c.GetBestNode(context.Background(), nil, "playbackID", "0", "0", "", false)
		require.NoError(t, err)
		require.LessOrEqual(t, time.Since(start), expectedResponseTime)
		time.Sleep(100 * time.Millisecond)
	}
}

package catabalancer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/patrickmn/go-cache"
)

const (
	stateCacheKey  = "stateCacheKey"
	dbQueryTimeout = 10 * time.Second
)

type CataBalancer struct {
	NodeName string // Node name of this instance

	metricTimeout       time.Duration
	ingestStreamTimeout time.Duration
	nodeStatsDB         *sql.DB
	nodeStatsCache      *cache.Cache
	cacheMutex          sync.Mutex
}

type stats struct {
	Streams       map[string]Streams     // Node name -> Streams
	IngestStreams map[string]Streams     // Node name -> Streams
	NodeMetrics   map[string]NodeMetrics // Node name -> NodeMetrics
}

type Streams map[string]Stream // Stream ID -> Stream

type Node struct {
	Name string
	DTSC string
}

type Stream struct {
	ID         string
	PlaybackID string
	Timestamp  time.Time // the time we received these stream details, old streams can be removed on a timeout
}

// JSON representation is deliberately truncated to keep the message size small
type NodeMetrics struct {
	CPUUsagePercentage       float64   `json:"c,omitempty"`
	RAMUsagePercentage       float64   `json:"r,omitempty"`
	BandwidthUsagePercentage float64   `json:"b,omitempty"`
	LoadAvg                  float64   `json:"l,omitempty"`
	GeoLatitude              float64   `json:"la,omitempty"`
	GeoLongitude             float64   `json:"lo,omitempty"`
	Timestamp                time.Time `json:"t,omitempty"` // the time we received these node metrics
}

// All of the scores are in the range 0-2, where:
// 2 = Good
// 1 = Okay
// 0 = Bad
type ScoredNode struct {
	Score       int64
	GeoScore    int64
	StreamScore int64
	GeoDistance float64
	Node
	Streams       Streams
	IngestStreams Streams
	NodeMetrics
}

func (s ScoredNode) String() string {
	return fmt.Sprintf("(Name:%s Score:%d GeoScore:%d StreamScore:%d GeoDistance:%.2f Lat:%.2f Lon:%.2f CPU:%.2f RAM:%.2f BW:%.2f)",
		s.Name,
		s.Score,
		s.GeoScore,
		s.StreamScore,
		s.GeoDistance,
		s.GeoLatitude,
		s.GeoLongitude,
		s.CPUUsagePercentage,
		s.RAMUsagePercentage,
		s.BandwidthUsagePercentage,
	)
}

// JSON representation is deliberately truncated to keep the message size small
type NodeUpdateEvent struct {
	Resource    string      `json:"resource,omitempty"`
	NodeID      string      `json:"n,omitempty"`
	NodeMetrics NodeMetrics `json:"nm,omitempty"`
	Streams     string      `json:"s,omitempty"`
}

func (n *NodeUpdateEvent) SetStreams(streamIDs []string, ingestStreamIDs []string) {
	n.Streams = strings.Join(streamIDs, "|") + "~" + strings.Join(ingestStreamIDs, "|")
}

func (n *NodeUpdateEvent) GetStreams() []string {
	before, _, _ := strings.Cut(n.Streams, "~")
	if len(before) > 0 {
		return strings.Split(before, "|")
	}
	return []string{}
}

func (n *NodeUpdateEvent) GetIngestStreams() []string {
	_, after, _ := strings.Cut(n.Streams, "~")
	if len(after) > 0 {
		return strings.Split(after, "|")
	}
	return []string{}
}

func NewBalancer(nodeName string, metricTimeout time.Duration, ingestStreamTimeout time.Duration, nodeStatsDB *sql.DB, cacheExpiry time.Duration) *CataBalancer {
	return &CataBalancer{
		NodeName:            nodeName,
		metricTimeout:       metricTimeout,
		ingestStreamTimeout: ingestStreamTimeout,
		nodeStatsDB:         nodeStatsDB,
		nodeStatsCache:      cache.New(cacheExpiry, 10*time.Minute),
	}
}

func (c *CataBalancer) Start(ctx context.Context) error {
	return nil
}

func (c *CataBalancer) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	return nil
}

func (c *CataBalancer) GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string, isStudioReq bool) (string, string, error) {
	s, err := c.refreshNodes(ctx)
	if err != nil {
		return "", "", fmt.Errorf("error refreshing nodes: %w", err)
	}

	latf := 0.0
	if lat != "" {
		latf, err = strconv.ParseFloat(lat, 64)
		if err != nil {
			return "", "", err
		}
	}
	lonf := 0.0
	if lon != "" {
		lonf, err = strconv.ParseFloat(lon, 64)
		if err != nil {
			return "", "", err
		}
	}

	// default to ourself if there are no other nodes
	nodeName := c.NodeName

	scoredNodes := c.createScoredNodes(s)
	if len(scoredNodes) > 0 {
		node, err := SelectNode(scoredNodes, playbackID, latf, lonf)
		if err != nil {
			return "", "", err
		}
		nodeName = node.Name
	} else {
		log.LogNoRequestID("catabalancer no nodes found, choosing myself", "chosenNode", nodeName, "streamID", playbackID, "reqLat", lat, "reqLon", lon)
	}

	prefix := "video"
	if len(redirectPrefixes) > 0 {
		prefix = redirectPrefixes[0]
	}
	return nodeName, fmt.Sprintf("%s+%s", prefix, playbackID), nil
}

func (c *CataBalancer) createScoredNodes(s stats) []ScoredNode {
	var nodesList []ScoredNode
	for nodeName, metrics := range s.NodeMetrics {
		if isStale(metrics.Timestamp, c.metricTimeout) {
			log.LogNoRequestID("catabalancer ignoring node with stale metrics", "nodeName", nodeName, "timestamp", metrics.Timestamp)
			continue
		}
		// make a copy of the streams map so that we can release the nodesLock (UpdateStreams will be making changes in the background)
		streams := make(Streams)
		for streamID, stream := range s.Streams[nodeName] {
			if isStale(stream.Timestamp, c.metricTimeout) {
				log.LogNoRequestID("catabalancer ignoring stale stream info", "nodeName", nodeName, "streamID", streamID, "timestamp", stream.Timestamp)
				continue
			}
			streams[streamID] = stream
		}
		nodesList = append(nodesList, ScoredNode{
			Node:        Node{Name: nodeName},
			Streams:     streams,
			NodeMetrics: s.NodeMetrics[nodeName],
		})
	}
	return nodesList
}

func (n *ScoredNode) HasStream(streamID string) bool {
	_, ok := n.Streams[streamID]
	return ok
}

func (n ScoredNode) GetLoadScore() int {
	if n.CPUUsagePercentage > 85 || n.BandwidthUsagePercentage > 85 || n.RAMUsagePercentage > 85 {
		return 0
	}
	if n.CPUUsagePercentage > 50 || n.BandwidthUsagePercentage > 50 || n.RAMUsagePercentage > 50 {
		return 1
	}
	return 2
}

func SelectNode(nodes []ScoredNode, streamID string, requestLatitude, requestLongitude float64) (Node, error) {
	if len(nodes) == 0 {
		return Node{}, fmt.Errorf("no nodes to select from")
	}

	topNodes := selectTopNodes(nodes, streamID, requestLatitude, requestLongitude, 3)

	if len(topNodes) == 0 {
		return Node{}, fmt.Errorf("selectTopNodes returned no nodes")
	}
	chosen := topNodes[rand.Intn(len(topNodes))].Node
	log.LogNoRequestID("catabalancer found node", "chosenNode", chosen.Name, "topNodes", fmt.Sprintf("%v", topNodes), "streamID", streamID, "reqLat", requestLatitude, "reqLon", requestLongitude)
	return chosen, nil
}

func selectTopNodes(scoredNodes []ScoredNode, streamID string, requestLatitude, requestLongitude float64, numNodes int) []ScoredNode {
	scoredNodes = geoScores(scoredNodes, requestLatitude, requestLongitude)

	// 1. Has Stream and Is Local and Isn't Overloaded
	localHasStreamNotOverloaded := []ScoredNode{}
	for _, node := range scoredNodes {
		if node.GeoScore == 2 && node.HasStream(streamID) && node.GetLoadScore() == 2 {
			node.StreamScore = 2
			localHasStreamNotOverloaded = append(localHasStreamNotOverloaded, node)
		}
	}
	if len(localHasStreamNotOverloaded) > 0 { // TODO: Should this be > 1 or > 2 so that we can ensure there's always some randomness?
		shuffle(localHasStreamNotOverloaded)
		return truncateReturned(localHasStreamNotOverloaded, numNodes)
	}

	// 2. Is Local and Isn't Overloaded
	localNotOverloaded := []ScoredNode{}
	for _, node := range scoredNodes {
		if node.GeoScore == 2 && node.GetLoadScore() == 2 {
			localNotOverloaded = append(localNotOverloaded, node)
		}
	}
	if len(localNotOverloaded) > 0 { // TODO: Should this be > 1 or > 2 so that we can ensure there's always some randomness?
		shuffle(localNotOverloaded)
		return truncateReturned(localNotOverloaded, numNodes)
	}

	// 3. Weighted least-bad option
	for i, node := range scoredNodes {
		node.Score += node.GeoScore
		node.Score += int64(node.GetLoadScore())
		if node.HasStream(streamID) {
			node.StreamScore = 2
			node.Score += 2
		}
		scoredNodes[i] = node
	}

	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].Score > scoredNodes[j].Score
	})
	return truncateReturned(scoredNodes, numNodes)
}

func shuffle(scoredNodes []ScoredNode) {
	rand.Shuffle(len(scoredNodes), func(i, j int) {
		scoredNodes[i], scoredNodes[j] = scoredNodes[j], scoredNodes[i]
	})
}

func truncateReturned(scoredNodes []ScoredNode, numNodes int) []ScoredNode {
	if len(scoredNodes) < numNodes {
		return scoredNodes
	}

	return scoredNodes[:numNodes]
}

func (c *CataBalancer) getCachedStats() (stats, bool) {
	cachedState, found := c.nodeStatsCache.Get(stateCacheKey)
	if found {
		return *cachedState.(*stats), true
	}
	return stats{}, false
}

func (c *CataBalancer) refreshNodes(ctx context.Context) (stats, error) {
	cachedState, found := c.getCachedStats()
	if found {
		return cachedState, nil
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	// check cache again since multiple requests can get an initial cache miss, the first one will populate
	// the cache while the requests waiting behind it (with the cacheMutex) can use the new cached data
	cachedState, found = c.getCachedStats()
	if found {
		return cachedState, nil
	}

	s := stats{
		Streams:       make(map[string]Streams),
		IngestStreams: make(map[string]Streams),
		NodeMetrics:   make(map[string]NodeMetrics),
	}

	if c.nodeStatsDB == nil {
		return s, fmt.Errorf("node stats DB was nil")
	}

	queryContext, cancel := context.WithTimeout(ctx, dbQueryTimeout)
	defer cancel()

	query := "SELECT stats FROM node_stats"
	rows, err := c.nodeStatsDB.QueryContext(queryContext, query)
	if err != nil {
		return s, fmt.Errorf("failed to query node stats: %w", err)
	}
	defer rows.Close()

	// Process the result set
	for rows.Next() {
		var statsBytes []byte
		if err := rows.Scan(&statsBytes); err != nil {
			return s, fmt.Errorf("failed to scan node stats row: %w", err)
		}

		var event NodeUpdateEvent
		err = json.Unmarshal(statsBytes, &event)
		if err != nil {
			return s, fmt.Errorf("failed to unmarshal node update event: %w", err)
		}

		if isStale(event.NodeMetrics.Timestamp, c.metricTimeout) {
			log.LogNoRequestID("catabalancer skipping stale data while refreshing", "nodeID", event.NodeID, "timestamp", event.NodeMetrics.Timestamp)
			continue
		}

		s.NodeMetrics[event.NodeID] = event.NodeMetrics
		s.Streams[event.NodeID] = make(Streams)
		s.IngestStreams[event.NodeID] = make(Streams)

		for _, stream := range event.GetStreams() {
			playbackID := getPlaybackID(stream)
			s.Streams[event.NodeID][playbackID] = Stream{ID: stream, PlaybackID: playbackID, Timestamp: time.Now()}
		}
		for _, stream := range event.GetIngestStreams() {
			playbackID := getPlaybackID(stream)
			s.Streams[event.NodeID][playbackID] = Stream{ID: stream, PlaybackID: playbackID, Timestamp: time.Now()}
			s.IngestStreams[event.NodeID][stream] = Stream{ID: stream, PlaybackID: playbackID, Timestamp: time.Now()}
		}
	}

	// Check for errors after iterating through rows
	if err := rows.Err(); err != nil {
		return s, err
	}

	c.nodeStatsCache.SetDefault(stateCacheKey, &s)
	return s, nil
}

func getPlaybackID(streamID string) string {
	playbackID := streamID
	parts := strings.Split(streamID, "+")
	if len(parts) == 2 {
		playbackID = parts[1] // take the playbackID after the prefix e.g. 'video+'
	}
	return playbackID
}

func (c *CataBalancer) MistUtilLoadSource(ctx context.Context, streamID, lat, lon string) (string, error) {
	s, err := c.refreshNodes(ctx)
	if err != nil {
		return "", fmt.Errorf("error refreshing nodes: %w", err)
	}

	for nodeName := range s.NodeMetrics {
		if stream, ok := s.IngestStreams[nodeName][streamID]; ok {
			if isStale(stream.Timestamp, c.ingestStreamTimeout) {
				return "", fmt.Errorf("catabalancer no node found for ingest stream: %s stale: true", streamID)
			}
			dtsc := "dtsc://" + nodeName
			log.LogNoRequestID("catabalancer MistUtilLoadSource found node", "DTSC", dtsc, "nodeName", nodeName, "stream", streamID)
			return dtsc, nil
		}
	}
	return "", fmt.Errorf("catabalancer no node found for ingest stream: %s stale: false", streamID)
}

var updateNodeStatsEvery = 5 * time.Second

func isStale(timestamp time.Time, stale time.Duration) bool {
	return time.Since(timestamp) >= stale
}

func StartMetricSending(nodeName string, latitude float64, longitude float64, mist clients.MistAPIClient, nodeStatsDB *sql.DB) {
	ticker := time.NewTicker(updateNodeStatsEvery)
	go func() {
		for range ticker.C {
			done := make(chan bool)

			// use a separate goroutine to implement a timeout for the sendMetrics func
			go func() {
				sendMetrics(nodeName, latitude, longitude, mist, nodeStatsDB)
				done <- true
			}()

			select {
			case <-done:
				continue
			case <-time.After(updateNodeStatsEvery):
				log.LogNoRequestID("catabalancer send metrics timed out")
				continue
			}
		}
	}()
}

func sendMetrics(nodeName string, latitude float64, longitude float64, mist clients.MistAPIClient, nodeStatsDB *sql.DB) {
	start := time.Now()
	sysusage, err := GetSystemUsage()
	if err != nil {
		log.LogNoRequestID("catabalancer failed to get sys usage", "err", err)
		return
	}

	event := NodeUpdateEvent{
		Resource: "nodeUpdate",
		NodeID:   nodeName,
		NodeMetrics: NodeMetrics{
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
			return
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
		return
	}
	sendMetricsToDB(nodeStatsDB, nodeName, payload)

	metrics.Metrics.CatabalancerSendMetricDurationSec.Observe(time.Since(start).Seconds())
}

func sendMetricsToDB(nodeStatsDB *sql.DB, nodeName string, payload []byte) {
	start := time.Now()
	queryContext, cancel := context.WithTimeout(context.Background(), updateNodeStatsEvery)
	defer cancel()
	insertStatement := `insert into "node_stats"(
                            "node_id",
                            "stats"
                            ) values($1, $2)
							ON CONFLICT (node_id)
							DO UPDATE SET stats = EXCLUDED.stats;`
	_, err := nodeStatsDB.ExecContext(
		queryContext,
		insertStatement,
		nodeName,
		payload,
	)
	if err != nil {
		log.LogNoRequestID("catabalancer error writing postgres node stats", "err", err)
	}
	metrics.Metrics.CatabalancerSendDBDurationSec.
		WithLabelValues(strconv.FormatBool(err == nil)).
		Observe(time.Since(start).Seconds())
}

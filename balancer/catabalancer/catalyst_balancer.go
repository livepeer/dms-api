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

	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/log"
)

type CataBalancer struct {
	NodeName  string // Node name of this instance
	Nodes     map[string]*Node
	nodesLock sync.RWMutex

	metricTimeout       time.Duration
	ingestStreamTimeout time.Duration
	NodeStatsDB         *sql.DB
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

func NewBalancer(nodeName string, metricTimeout time.Duration, ingestStreamTimeout time.Duration, nodeStatsDB *sql.DB) *CataBalancer {
	return &CataBalancer{
		NodeName:            nodeName,
		Nodes:               make(map[string]*Node),
		metricTimeout:       metricTimeout,
		ingestStreamTimeout: ingestStreamTimeout,
		NodeStatsDB:         nodeStatsDB,
	}
}

func (c *CataBalancer) Start(ctx context.Context) error {
	return nil
}

func (c *CataBalancer) UpdateMembers(ctx context.Context, members []cluster.Member) error {
	log.LogNoRequestID("catabalancer UpdateMembers")
	c.nodesLock.Lock()
	defer c.nodesLock.Unlock()

	latestNodes := make(map[string]*Node)
	for _, member := range members {
		if member.Tags["node"] != "media" { // ignore testing nodes from load balancing
			continue
		}
		latestNodes[member.Name] = &Node{
			Name: member.Name,
			DTSC: member.Tags["dtsc"],
		}
	}

	c.Nodes = latestNodes
	return nil
}

func (c *CataBalancer) GetBestNode(ctx context.Context, redirectPrefixes []string, playbackID, lat, lon, fallbackPrefix string, isStudioReq bool) (string, string, error) {
	s, err := c.RefreshNodes()
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
	c.nodesLock.RLock()
	defer c.nodesLock.RUnlock()
	var nodesList []ScoredNode
	for nodeName, node := range c.Nodes {
		metrics, ok := s.NodeMetrics[nodeName]
		if !ok {
			continue
		}
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
			Node:        *node,
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

func (c *CataBalancer) RefreshNodes() (stats, error) {
	s := stats{
		Streams:       make(map[string]Streams),
		IngestStreams: make(map[string]Streams),
		NodeMetrics:   make(map[string]NodeMetrics),
	}

	log.LogNoRequestID("catabalancer refreshing nodes")
	if c.NodeStatsDB == nil {
		return s, fmt.Errorf("node stats DB was nil")
	}

	query := "SELECT stats FROM node_stats"
	rows, err := c.NodeStatsDB.Query(query)
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
	s, err := c.RefreshNodes()
	if err != nil {
		return "", fmt.Errorf("error refreshing nodes: %w", err)
	}

	c.nodesLock.RLock()
	defer c.nodesLock.RUnlock()
	for nodeName := range c.Nodes {
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

var UpdateNodeStatsEvery = 5 * time.Second

func isStale(timestamp time.Time, stale time.Duration) bool {
	return time.Since(timestamp) >= stale
}

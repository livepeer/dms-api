package config

import (
	"encoding/base64"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

type Cli struct {
	HTTPAddress               string
	HTTPInternalAddress       string
	ClusterAddress            string
	ClusterAdvertiseAddress   string
	MistEnabled               bool
	MistTriggerSetup          bool
	MistHost                  string
	MistUser                  string
	MistPassword              string
	MistPrometheus            string
	Mode                      string
	MistPort                  int
	MistConnectTimeout        time.Duration
	MistStreamSource          string
	MistHardcodedBroadcasters string
	MistScrapeMetrics         bool
	MistBaseStreamName        string
	MistLoadBalancerPort      int
	MistLoadBalancerTemplate  string
	MistCleanup               bool
	LogSysUsage               bool
	AMQPURL                   string
	OwnRegion                 string
	OwnRegionTagAdjust        int
	APIToken                  string
	APIServer                 string
	SourceOutput              string
	PrivateBucketURLs         []*url.URL
	ExternalTranscoder        string
	VodPipelineStrategy       string
	MetricsDBConnectionString string
	NodeStatsConnectionString string
	ImportIPFSGatewayURLs     []*url.URL
	ImportArweaveGatewayURLs  []*url.URL
	NodeName                  string
	BalancerArgs              []string
	NodeHost                  string
	NodeLatitude              float64
	NodeLongitude             float64
	RedirectPrefixes          []string
	Tags                      map[string]string
	RetryJoin                 []string
	EncryptKey                string
	VodDecryptPublicKey       string
	VodDecryptPrivateKey      string
	StorageFallbackURLs       map[string]string
	GateURL                   string
	DataURL                   string
	StreamHealthHookURL       string
	BroadcasterURL            string
	SourcePlaybackHosts       map[string]string
	DefaultQuality            int
	MaxBitrateFactor          float64
	BlockedJWTs               []string
	EnableAnalytics           string
	KafkaBootstrapServers     string
	KafkaUser                 string
	KafkaPassword             string
	AnalyticsKafkaTopic       string
	UserEndKafkaTopic         string
	SerfMembersEndpoint       string
	EventsEndpoint            string
	CatalystApiURL            string

	// mapping playbackId to value between 0.0 to 100.0
	CdnRedirectPlaybackPct             map[string]float64
	CdnRedirectPrefix                  *url.URL
	CdnRedirectPrefixCatalystSubdomain bool

	C2PAPrivateKeyPath string
	C2PACertsPath      string

	CataBalancer                    string
	CataBalancerMetricTimeout       time.Duration
	CataBalancerIngestStreamTimeout time.Duration
	SerfQueueSize                   int
	SerfEventBuffer                 int
	SerfMaxQueueDepth               int

	LBReplaceHostMatch   string
	LBReplaceHostPercent int
	LBReplaceHostList    []string
}

// Return our own URL for callback trigger purposes
func (cli *Cli) OwnInternalURL() string {
	//  No errors because we know it's valid from AddrFlag
	host, port, _ := net.SplitHostPort(cli.HTTPInternalAddress)
	ip := net.ParseIP(host)
	if ip.IsUnspecified() {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, port)
	return fmt.Sprintf("http://%s", addr)
}

// EncryptBytes returns the encryption key configured.
func (cli *Cli) EncryptBytes() ([]byte, error) {
	return base64.StdEncoding.DecodeString(cli.EncryptKey)
}

// Should we enable mapic?
func (cli *Cli) ShouldMapic() bool {
	return cli.APIServer != ""
}

func (cli *Cli) IsClusterMode() bool {
	return cli.Mode == "cluster-only" || cli.Mode == "all"
}

func (cli *Cli) IsApiMode() bool {
	return cli.Mode == "api-only" || cli.Mode == "all"
}

// Should we enable mist-cleanup script to run periodically and delete leaky shm?
func (cli *Cli) ShouldMistCleanup() bool {
	return cli.MistCleanup
}

// Should we enable pod-mon script to run periodically and log system usage stats?
func (cli *Cli) ShouldLogSysUsage() bool {
	return cli.LogSysUsage
}

// Handle some legacy environment variables for zero-downtime catalyst-node migration
func (cli *Cli) ParseLegacyEnv() {
	node := os.Getenv("CATALYST_NODE_NODE")
	if node != "" {
		cli.NodeName = node
		glog.Warning("Detected legacy env CATALYST_NODE_NODE, please migrate to CATALYST_API_NODE")
	}

	bind := os.Getenv("CATALYST_NODE_BIND")
	if bind != "" {
		cli.ClusterAddress = bind
		glog.Warning("Detected legacy env CATALYST_NODE_BIND, please migrate to CATALYST_API_CLUSTER_ADDR")
	}

	advertise := os.Getenv("CATALYST_NODE_ADVERTISE")
	if advertise != "" {
		cli.ClusterAdvertiseAddress = advertise
		glog.Warning("Detected legacy env CATALYST_NODE_ADVERTISE, please migrate to CATALYST_API_CLUSTER_ADVERTISE_ADDR")
	}

	tags := os.Getenv("CATALYST_NODE_SERF_TAGS")
	if tags != "" {
		parsed, err := parseCommaMap(tags)
		if err != nil {
			panic(fmt.Errorf("error parsing CATALYST_NODE_SERF_TAGS: %w", err))
		}
		cli.Tags = parsed
		glog.Warning("Detected legacy env CATALYST_NODE_SERF_TAGS, please migrate to CATALYST_API_TAGS")
	}
}

// still a string, but validates the provided value is some kind of coherent host:port
func AddrFlag(fs *flag.FlagSet, dest *string, name, value, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		_, _, err := net.SplitHostPort(s)
		if err != nil {
			return err
		}
		*dest = s
		return nil
	})
}

func parseURL(s string, dest **url.URL) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if _, err = url.ParseQuery(u.RawQuery); err != nil {
		return err
	}
	*dest = u
	return nil
}

func URLVarFlag(fs *flag.FlagSet, dest **url.URL, name, value, usage string) {
	fs.Func(name, usage, func(s string) error {
		return parseURL(s, dest)
	})
}

func URLSliceVarFlag(fs *flag.FlagSet, dest *[]*url.URL, name, value, usage string) {
	fs.Func(name, usage, func(s string) error {
		return parseURLs(s, dest)
	})
}

func parseURLs(s string, dest *[]*url.URL) error {
	strs := strings.Split(s, ",")
	urls := make([]*url.URL, len(strs))
	for i, str := range strs {
		if err := parseURL(str, &urls[i]); err != nil {
			return err
		}
	}
	*dest = urls
	return nil
}

// handles -foo "value1 value2 value3"
func SpaceSliceFlag(fs *flag.FlagSet, dest *[]string, name string, value []string, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		split := strings.Split(s, " ")
		if len(split) == 1 && split[0] == "" {
			*dest = []string{}
			return nil
		}
		*dest = split
		return nil
	})
}

// handles -foo=value1,value2,value3
func CommaSliceFlag(fs *flag.FlagSet, dest *[]string, name string, value []string, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		split := strings.Split(s, ",")
		if len(split) == 1 && split[0] == "" {
			*dest = []string{}
			return nil
		}
		*dest = split
		return nil
	})
}

// handles -foo=value1:10.3,value2:99.9,value3:0.1,value4:100,value5:0
func CommaWithPctSliceFlag(fs *flag.FlagSet, dest *map[string]float64, name string, value map[string]float64, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		elements := strings.Split(s, ",")
		if len(elements) == 0 || (len(elements) == 1 && elements[0] == "") {
			*dest = map[string]float64{}
			return nil
		}
		for _, redirect := range elements {
			elements := strings.Split(redirect, ":")
			if len(elements) == 2 {
				percentage, err := strconv.ParseFloat(elements[1], 64)
				if err != nil || percentage > 100.0 || percentage < 0.0 {
					return fmt.Errorf("invalid config %s - should be between 0.0 and 100.00", redirect)
				} else {
					(*dest)[elements[0]] = percentage
				}
			} else if len(elements) == 1 {
				glog.Infof("Assuming 100% CDN traffic for %s playbackId", elements[0])
				(*dest)[elements[0]] = 100.0
			} else {
				return fmt.Errorf("unexpected format of %s", redirect)
			}
		}

		return nil
	})
}

func parseCommaMap(s string) (map[string]string, error) {
	output := map[string]string{}
	if s == "" {
		return output, nil
	}
	for _, pair := range strings.Split(s, ",") {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			return map[string]string{}, fmt.Errorf("failed to parse keypairs, -option=k1=v1,k2=v2 format required, got %s", s)
		}
		k, v := kv[0], kv[1]
		output[k] = v
	}
	return output, nil
}

// handles -foo=key1=value1,key2=value2
func CommaMapFlag(fs *flag.FlagSet, dest *map[string]string, name string, value map[string]string, usage string) {
	*dest = value
	fs.Func(name, usage, func(s string) error {
		var err error
		*dest, err = parseCommaMap(s)
		return err
	})
}

type InvertedBool struct {
	Value *bool
}

func (f *InvertedBool) String() string {
	if f.Value != nil {
		return fmt.Sprint(*f.Value)
	}
	return ""
}

func (f *InvertedBool) IsBoolFlag() bool {
	return true
}

func (f *InvertedBool) Set(value string) error {
	if value == "true" {
		*f.Value = false
	} else if value == "false" {
		*f.Value = true
	} else {
		return fmt.Errorf("only true and false values allowed")
	}
	return nil
}

// MistController has trouble giving us `-boolean-flag=false` values, so we use `-no-boolean-flag=true` instead 🤷‍♂️
func InvertedBoolFlag(fs *flag.FlagSet, dest *bool, name string, value bool, usage string) {
	*dest = value
	fs.Var(&InvertedBool{dest}, fmt.Sprintf("no-%s", name), usage)
}

package main

import (
	"bytes"
	"context"
	"crypto/rsa"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/serf/serf"

	"github.com/golang/glog"
	_ "github.com/lib/pq"
	"github.com/livepeer/catalyst-api/api"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/balancer/catabalancer"
	mist_balancer "github.com/livepeer/catalyst-api/balancer/mist"
	"github.com/livepeer/catalyst-api/c2pa"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/crypto"
	"github.com/livepeer/catalyst-api/events"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/middleware"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/catalyst-api/pprof"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
	"github.com/peterbourgon/ff/v3"
	"golang.org/x/sync/errgroup"
)

func main() {
	err := flag.Set("logtostderr", "true")
	if err != nil {
		glog.Fatal(err)
	}
	vFlag := flag.Lookup("v")
	fs := flag.NewFlagSet("catalyst-api", flag.ExitOnError)
	cli := config.Cli{}

	version := fs.Bool("version", false, "print application version")

	fs.StringVar(&cli.Mode, "mode", "all", "Mode to run the application in. Options: all, cluster-only, api-only")

	// listen addresses
	config.AddrFlag(fs, &cli.HTTPAddress, "http-addr", "0.0.0.0:8989", "Address to bind for external-facing Catalyst HTTP handling")
	config.AddrFlag(fs, &cli.HTTPInternalAddress, "http-internal-addr", "127.0.0.1:7979", "Address to bind for internal privileged HTTP commands")
	config.AddrFlag(fs, &cli.ClusterAddress, "cluster-addr", "0.0.0.0:9935", "Address to bind Serf network listeners to. To use an IPv6 address, specify [::1] or [::1]:7946.")
	fs.StringVar(&cli.ClusterAdvertiseAddress, "cluster-advertise-addr", "", "Address to advertise to the other cluster members")

	// catalyst-api parameters
	fs.StringVar(&cli.APIToken, "api-token", "IAmAuthorized", "Auth header value for API access")
	fs.StringVar(&cli.SourceOutput, "source-output", "", "URL for the video source segments used if source_segments is not defined in the upload request")
	config.URLSliceVarFlag(fs, &cli.PrivateBucketURLs, "private-bucket", "", "URL for the private media bucket")
	fs.StringVar(&cli.ExternalTranscoder, "external-transcoder", "", "URL for the external transcoder to be used by the pipeline coordinator. Only 1 implementation today for AWS MediaConvert which should be in the format: mediaconvert://key-id:key-secret@endpoint-host?region=aws-region&role=iam-role&s3_aux_bucket=s3://bucket")
	fs.StringVar(&cli.VodPipelineStrategy, "vod-pipeline-strategy", string(pipeline.StrategyCatalystFfmpegDominance), "Which strategy to use for the VOD pipeline")
	fs.StringVar(&cli.MetricsDBConnectionString, "metrics-db-connection-string", "", "Connection string to use for the metrics Postgres DB. Takes the form: host=X port=X user=X password=X dbname=X")
	fs.StringVar(&cli.NodeStatsConnectionString, "node-stats-connection-string", "", "Connection string to use for the node stats DB. Takes the form: host=X port=X user=X password=X dbname=X")
	config.URLSliceVarFlag(fs, &cli.ImportIPFSGatewayURLs, "import-ipfs-gateway-urls", "https://vod-import-gtw.mypinata.cloud/ipfs/?pinataGatewayToken={{secrets.LP_PINATA_GATEWAY_TOKEN}},https://w3s.link/ipfs/,https://ipfs.io/ipfs/,https://cloudflare-ipfs.com/ipfs/", "Comma delimited ordered list of IPFS gateways (includes /ipfs/ suffix) to import assets from")
	config.URLSliceVarFlag(fs, &cli.ImportArweaveGatewayURLs, "import-arweave-gateway-urls", "https://arweave.net/", "Comma delimited ordered list of arweave gateways")
	fs.BoolVar(&cli.MistCleanup, "run-mist-cleanup", true, "Run mist-cleanup.sh to cleanup shm")
	fs.BoolVar(&cli.LogSysUsage, "run-pod-mon", true, "Run pod-mon script to monitor sys usage")
	fs.StringVar(&cli.BroadcasterURL, "broadcaster-url", config.DefaultBroadcasterURL, "URL of local broadcaster")
	config.InvertedBoolFlag(fs, &cli.MistEnabled, "mist", true, "Disable all Mist integrations. Should only be used for development and CI")
	config.CommaMapFlag(fs, &cli.SourcePlaybackHosts, "source-playback-hosts", map[string]string{}, "Hostname to prefix mappings for source playback URLs")
	fs.UintVar(&video.DefaultQuality, "default-quality", 27, "Default transcoded video quality")
	fs.Float64Var(&video.MaxBitrateFactor, "max-bitrate-factor", 1.2, "Factor to limit the max video bitrate with relation to the source average bitrate")
	fs.StringVar(&cli.C2PAPrivateKeyPath, "c2pa-private-key", "", "Path to the private key used to sign C2PA manifest")
	fs.StringVar(&cli.C2PACertsPath, "c2pa-certs", "", "Path to the certs used to sign C2PA manifest")
	fs.IntVar(&config.MaxInFlightJobs, "max-inflight-jobs", 8, "Maximum number of concurrent VOD jobs to support in catalyst-api")
	fs.IntVar(&config.MaxInFlightClipJobs, "max-inflight-clip-jobs", 20, "Maximum number of concurrent clipping jobs to support in catalyst-api")
	fs.IntVar(&config.TranscodingParallelJobs, "parallel-transcode-jobs", 2, "Number of parallel transcode jobs")
	fs.StringVar(&cli.CataBalancer, "catabalancer", "", "Enable catabalancer load balancer")
	fs.DurationVar(&cli.CataBalancerMetricTimeout, "catabalancer-metric-timeout", 26*time.Second, "Catabalancer timeout for node metrics")
	fs.DurationVar(&cli.CataBalancerIngestStreamTimeout, "catabalancer-ingest-stream-timeout", 20*time.Minute, "Catabalancer timeout for ingest stream metrics")
	config.CommaSliceFlag(fs, &cli.BlockedJWTs, "gate-blocked-jwts", []string{}, "List of blocked JWTs for token gating")

	// mist-api-connector parameters
	fs.IntVar(&cli.MistPort, "mist-port", 4242, "Port to connect to Mist")
	fs.StringVar(&cli.MistHost, "mist-host", "127.0.0.1", "Hostname of the Mist server")
	fs.StringVar(&cli.MistUser, "mist-user", "", "username of MistServer")
	fs.StringVar(&cli.MistPassword, "mist-password", "", "password of MistServer")
	fs.StringVar(&cli.MistPrometheus, "mist-prometheus", "", "Mist path for the prometheus metrics endpoint")
	fs.DurationVar(&cli.MistConnectTimeout, "mist-connect-timeout", 5*time.Minute, "Max time to wait attempting to connect to Mist server")
	fs.StringVar(&cli.MistStreamSource, "mist-stream-source", "push://", "Stream source we should use for created Mist stream")
	fs.StringVar(&cli.MistHardcodedBroadcasters, "mist-hardcoded-broadcasters", "", "Hardcoded broadcasters for use by MistProcLivepeer")
	config.InvertedBoolFlag(fs, &cli.MistScrapeMetrics, "mist-scrape-metrics", true, "Scrape statistics from MistServer and publish to RabbitMQ")
	fs.StringVar(&cli.MistBaseStreamName, "mist-base-stream-name", "video", "Base stream name to be used in wildcard-based routing scheme")
	fs.StringVar(&cli.APIServer, "api-server", "", "Livepeer API server to use")
	fs.StringVar(&cli.AMQPURL, "amqp-url", "", "RabbitMQ url")
	fs.StringVar(&cli.OwnRegion, "own-region", "", "Identifier of the region where the service is running, used for mapping external data back to current region")
	fs.IntVar(&cli.OwnRegionTagAdjust, "own-region-tag-adjust", 1000, "Bonus weight for 'own-region' to minimise cross-region redirects done by mist load balancer (MistUtilLoad)")
	fs.StringVar(&cli.StreamHealthHookURL, "stream-health-hook-url", "http://localhost:3004/api/stream/hook/health", "Address to POST stream health payloads to (response is ignored)")

	// catalyst-node parameters
	hostname, _ := os.Hostname()
	fs.StringVar(&cli.NodeName, "node", hostname, "Name of this node within the cluster")
	config.SpaceSliceFlag(fs, &cli.BalancerArgs, "balancer-args", []string{}, "arguments passed to MistUtilLoad")
	fs.StringVar(&cli.NodeHost, "node-host", "", "Hostname this node should handle requests for. Requests on any other domain will trigger a redirect. Useful as a 404 handler to send users to another node.")
	config.CommaWithPctSliceFlag(fs, &cli.CdnRedirectPlaybackPct, "cdn-redirect-playback-ids", map[string]float64{}, "PlaybackIDs to be redirected and percentage of traffic. E.g. 'dbe3q3g6q2kia036:100,6736xac7u1hj36pa:0.01'")
	config.URLVarFlag(fs, &cli.CdnRedirectPrefix, "cdn-redirect-prefix", "", "CDN URL where streams selected by -cdn-redirect-playback-ids are redirected. E.g. https://externalcdn.livepeer.com/mist/")
	config.InvertedBoolFlag(fs, &cli.CdnRedirectPrefixCatalystSubdomain, "cdn-redirect-prefix-catalyst-subdomain", true, "inject catalyst closest node domain into CDN URL. E.g. https://sin-prod-catalyst-0.lp-playback.studio.externalcdn.livepeer.com/mist/ ")
	fs.Float64Var(&cli.NodeLatitude, "node-latitude", 0, "Latitude of this Catalyst node. Used for load balancing.")
	fs.Float64Var(&cli.NodeLongitude, "node-longitude", 0, "Longitude of this Catalyst node. Used for load balancing.")
	config.CommaSliceFlag(fs, &cli.RedirectPrefixes, "redirect-prefixes", []string{}, "Set of valid prefixes of playback id which are handled by mistserver")
	config.CommaMapFlag(fs, &cli.Tags, "tags", map[string]string{"node": "media"}, "Serf tags for Catalyst nodes")
	fs.IntVar(&cli.MistLoadBalancerPort, "mist-load-balancer-port", 40010, "MistUtilLoad port (default random)")
	fs.StringVar(&cli.MistLoadBalancerTemplate, "mist-load-balancer-template", "http://%s:4242", "template for specifying the host that should be queried for Prometheus stat output for this node")
	config.CommaSliceFlag(fs, &cli.RetryJoin, "retry-join", []string{}, "An agent to join with. This flag be specified multiple times. Does not exit on failure like -join, used to retry until success.")
	fs.StringVar(&cli.EncryptKey, "encrypt", "", "Key for encrypting network traffic within Serf. Must be a base64-encoded 32-byte key.")
	fs.StringVar(&cli.VodDecryptPublicKey, "catalyst-public-key", "", "Public key of the catalyst node for encryption")
	fs.StringVar(&cli.VodDecryptPrivateKey, "catalyst-private-key", "", "Private key of the catalyst node for encryption")
	config.CommaMapFlag(fs, &cli.StorageFallbackURLs, "storage-fallback-urls", map[string]string{}, `Comma-separated map of primary to backup storage URLs. If a file fails downloading from one of the primary storages (detected by prefix), it will fallback to the corresponding backup URL after having the prefix replaced. E.g. https://storj.livepeer.com/catalyst-recordings-com/hls=https://google.livepeer.com/catalyst-recordings-com/hls`)
	fs.StringVar(&cli.GateURL, "gate-url", "http://localhost:3004/api/access-control/gate", "Address to contact playback gating API for access control verification")
	fs.StringVar(&cli.DataURL, "data-url", "http://localhost:3004/api/data", "Address of the Livepeer Data Endpoint")
	config.InvertedBoolFlag(fs, &cli.MistTriggerSetup, "mist-trigger-setup", true, "Overwrite Mist triggers with the ones built into catalyst-api")
	fs.IntVar(&cli.SerfQueueSize, "serf-queue-size", 50, "Size of internal serf queue before user events are dropped")
	fs.IntVar(&cli.SerfEventBuffer, "serf-event-buffer", 100000, "Size of serf 'recent event' buffer, outside of which things are dropped")
	fs.IntVar(&cli.SerfMaxQueueDepth, "serf-max-queue-depth", 100000, "Size of Serf queue, outside of which things are dropped")
	fs.StringVar(&cli.EnableAnalytics, "analytics", "disabled", "Enables analytics API: enabled or disabled")
	fs.StringVar(&cli.KafkaBootstrapServers, "kafka-bootstrap-servers", "", "URL of Kafka Bootstrap Servers")
	fs.StringVar(&cli.KafkaUser, "kafka-user", "", "Kafka Username")
	fs.StringVar(&cli.KafkaPassword, "kafka-password", "", "Kafka Password")
	fs.StringVar(&cli.AnalyticsKafkaTopic, "analytics-kafka-topic", "", "Kafka Topic used to send analytics logs")
	fs.StringVar(&cli.UserEndKafkaTopic, "user-end-kafka-topic", "", "Kafka Topic used to send USER_END events")
	fs.StringVar(&cli.SerfMembersEndpoint, "serf-members-endpoint", "", "Endpoint to get the current members in the cluster")
	fs.StringVar(&cli.EventsEndpoint, "events-endpoint", "", "Endpoint to send proxied events from catalyst-api into catalyst")
	fs.StringVar(&cli.CatalystApiURL, "catalyst-api-url", "", "Endpoint for externally deployed catalyst-api; if not set, use local catalyst-api")
	fs.StringVar(&cli.LBReplaceHostMatch, "lb-replace-host-match", "", "What to match on the hostname for node replacement e.g. sto")
	config.CommaSliceFlag(fs, &cli.LBReplaceHostList, "lb-replace-host-list", []string{}, "List of hostnames to replace with for node replacement")
	fs.IntVar(&cli.LBReplaceHostPercent, "lb-replace-host-percent", 0, "Percentage of matching requests to replace host on")
	pprofPort := fs.Int("pprof-port", 6061, "Pprof listen port")

	fs.String("send-audio", "", "[DEPRECATED] ignored, will be removed")

	// special parameters
	mistJson := fs.Bool("j", false, "Print application info as JSON. Used by Mist to present flags in its UI.")
	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	_ = fs.String("config", "", "config file (optional)")

	err = ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("CATALYST_API"),
	)
	if err != nil {
		glog.Fatalf("error parsing cli: %s", err)
	}
	cli.ParseLegacyEnv()
	if len(fs.Args()) > 0 {
		glog.Fatalf("unexpected extra arguments on command line: %v", fs.Args())
	}
	err = flag.CommandLine.Parse(nil)
	if err != nil {
		glog.Fatal(err)
	}

	if *version {
		fmt.Printf("catalyst-api version: %s", config.Version)
		return
	}

	go func() {
		log.Println(pprof.ListenAndServe(*pprofPort))
	}()

	if *verbosity != "" {
		err = vFlag.Value.Set(*verbosity)
		if err != nil {
			glog.Fatal(err)
		}
	}

	if *mistJson {
		mistconnector.PrintMistConfigJson("catalyst-api", "HTTP API server for translating Catalyst API requests into Mist calls", "Catalyst API", config.Version, fs)
		return
	}

	config.StorageFallbackURLs = cli.StorageFallbackURLs

	var (
		metricsDB *sql.DB
		vodEngine *pipeline.Coordinator
		mapic     mistapiconnector.IMac
		bal       balancer.Balancer
		broker    misttriggers.TriggerBroker
		mist      clients.MistAPIClient
		c         cluster.Cluster
	)

	// Initialize root context; cancelling this prompts all components to shut down cleanly
	group, ctx := errgroup.WithContext(context.Background())
	mistBalancerConfig := &balancer.Config{
		Args:                     cli.BalancerArgs,
		MistUtilLoadPort:         uint32(cli.MistLoadBalancerPort),
		MistLoadBalancerTemplate: cli.MistLoadBalancerTemplate,
		MistHost:                 cli.MistHost,
		MistPort:                 cli.MistPort,
		NodeName:                 cli.NodeName,
		OwnRegion:                cli.OwnRegion,
		OwnRegionTagAdjust:       cli.OwnRegionTagAdjust,

		ReplaceHostMatch:   cli.LBReplaceHostMatch,
		ReplaceHostPercent: cli.LBReplaceHostPercent,
		ReplaceHostList:    cli.LBReplaceHostList,
	}
	broker = misttriggers.NewTriggerBroker()

	catalystApiURL := resolveCatalystApiURL(cli)
	glog.Infof("Using Catalyst API URL: %s", catalystApiURL)

	serfMembersEndpoint := cli.SerfMembersEndpoint
	if serfMembersEndpoint == "" {
		serfMembersEndpoint = cli.OwnInternalURL() + "/api/serf/members"
	}

	if cli.MistEnabled {
		mist = clients.NewMistAPIClient(cli.MistUser, cli.MistPassword, cli.MistHost, cli.MistPort)
	}

	var nodeStatsDB *sql.DB
	if cli.NodeStatsConnectionString != "" {
		nodeStatsDB, err := sql.Open("postgres", cli.NodeStatsConnectionString)
		if err != nil {
			glog.Fatalf("Error creating postgres node stats connection: %s", err)
		}

		// Without this, we've run into issues with exceeding our open connection limit
		nodeStatsDB.SetMaxOpenConns(50)
		nodeStatsDB.SetMaxIdleConns(50)
		nodeStatsDB.SetConnMaxLifetime(time.Hour)
	}

	if cli.IsClusterMode() {
		c = cluster.NewCluster(&cli)
		group.Go(func() error {
			return c.Start(ctx)
		})

		group.Go(func() error {
			serfUserEventCallbackEndpoint := fmt.Sprintf("%s/api/serf/receiveUserEvent", catalystApiURL)
			return handleClusterEvents(ctx, serfUserEventCallbackEndpoint, c)
		})

		bal = mist_balancer.NewLocalBalancer(mistBalancerConfig)
		group.Go(func() error {
			return bal.Start(ctx)
		})
		group.Go(func() error {
			return reconcileBalancer(ctx, bal, c)
		})

		if balancer.CombinedBalancerEnabled(cli.CataBalancer) {
			if cli.Tags["node"] == "media" { // don't announce load balancing availability for testing nodes
				events.StartMetricSending(cli.NodeName, cli.NodeLatitude, cli.NodeLongitude, mist, nodeStatsDB)
			}
		}
	} else {
		bal = mist_balancer.NewRemoteBalancer(mistBalancerConfig)
		if balancer.CombinedBalancerEnabled(cli.CataBalancer) {
			cataBalancer := catabalancer.NewBalancer(cli.NodeName, cli.CataBalancerMetricTimeout, cli.CataBalancerIngestStreamTimeout, nodeStatsDB)
			// Temporary combined balancer to test cataBalancer logic alongside existing mist balancer
			bal = balancer.NewCombinedBalancer(cataBalancer, bal, cli.CataBalancer)
		}
	}

	if cli.IsApiMode() {
		// TODO: I don't love the global variables for these
		config.ImportIPFSGatewayURLs = cli.ImportIPFSGatewayURLs
		config.ImportArweaveGatewayURLs = cli.ImportArweaveGatewayURLs
		config.HTTPInternalAddress = cli.HTTPInternalAddress

		// Kick off the callback client, to send job update messages on a regular interval
		headers := map[string]string{"Authorization": fmt.Sprintf("Bearer %s", cli.APIToken)}
		statusClient := clients.NewPeriodicCallbackClient(15*time.Second, headers).Start()

		// Emit high-cardinality metrics to a Postrgres database if configured
		if cli.MetricsDBConnectionString != "" {
			metricsDB, err = sql.Open("postgres", cli.MetricsDBConnectionString)
			if err != nil {
				glog.Fatalf("Error creating postgres metrics connection: %v", err)
			}

			// Without this, we've run into issues with exceeding our open connection limit
			metricsDB.SetMaxOpenConns(2)
			metricsDB.SetMaxIdleConns(2)
			metricsDB.SetConnMaxLifetime(time.Hour)
		} else {
			glog.Info("Postgres metrics connection string was not set, postgres metrics are disabled.")
		}

		var vodDecryptPrivateKey *rsa.PrivateKey

		if cli.VodDecryptPrivateKey != "" && cli.VodDecryptPublicKey != "" {
			vodDecryptPrivateKey, err = crypto.LoadPrivateKey(cli.VodDecryptPrivateKey)
			if err != nil {
				glog.Fatalf("Error loading vod decrypt private key: %v", err)
			}
			isValidKeyPair, err := crypto.ValidateKeyPair(cli.VodDecryptPublicKey, *vodDecryptPrivateKey)
			if !isValidKeyPair || err != nil {
				glog.Fatalf("Invalid vod decrypt key pair")
			}
		}

		c2, err := createC2PA(&cli)
		if err != nil {
			// Log warning, but still start without C2PA signing
			glog.Warning(err)
		}
		// Start the "co-ordinator" that determines whether to send jobs to the Catalyst transcoding pipeline
		// or an external one
		vodEngine, err = pipeline.NewCoordinator(pipeline.Strategy(cli.VodPipelineStrategy), cli.SourceOutput, cli.ExternalTranscoder, statusClient, metricsDB, vodDecryptPrivateKey, cli.BroadcasterURL, cli.SourcePlaybackHosts, c2)
		if err != nil {
			glog.Fatalf("Error creating VOD pipeline coordinator: %v", err)
		}

		if cli.ShouldMapic() {
			mapic = mistapiconnector.NewMapic(&cli, broker, mist)
			group.Go(func() error {
				return mapic.Start(ctx)
			})
		}
	}

	if cli.IsClusterMode() {
		// Configure Mist Triggers
		if cli.MistEnabled && cli.MistTriggerSetup {
			mistTriggerHandlerEndpoint := fmt.Sprintf("%s/api/mist/trigger", cli.OwnInternalURL())
			err := broker.SetupMistTriggers(mist, mistTriggerHandlerEndpoint)
			if err != nil {
				glog.Error("catalyst-api was unable to communicate with MistServer to set up its triggers.")
				glog.Error("hint: are you trying to boot catalyst-api without Mist for development purposes? use the flag -no-mist")
				glog.Fatalf("error setting up Mist triggers err=%s", err)
			}
		}

		// Start cron style apps to run periodically
		if cli.ShouldMistCleanup() {
			app := "mist-cleanup.sh"
			// schedule mist-cleanup every 15min with a timeout of 15min
			mistCleanup, err := middleware.NewShell(15*60*time.Second, 15*60*time.Second-10, app)
			if err != nil {
				glog.Info("Failed to shell out:", app, err)
			}
			mistCleanupTick := mistCleanup.RunBg()
			defer mistCleanupTick.Stop()
		}
		if cli.ShouldLogSysUsage() {
			app := "pod-mon.sh"
			// schedule pod-mon every 5min with timeout of 5s
			podMon, err := middleware.NewShell(300*time.Second, 5*time.Second, app)
			if err != nil {
				glog.Info("Failed to shell out:", app, err)
			}
			podMonTick := podMon.RunBg()
			defer podMonTick.Stop()
		}

		group.Go(func() error {
			return handleSignals(ctx)
		})
	}

	group.Go(func() error {
		return api.ListenAndServe(ctx, cli, vodEngine, bal, mapic, serfMembersEndpoint)
	})

	group.Go(func() error {
		return api.ListenAndServeInternal(ctx, cli, vodEngine, mapic, bal, c, broker, metricsDB, serfMembersEndpoint, cli.EventsEndpoint, catalystApiURL)
	})

	err = group.Wait()
	glog.Infof("Shutdown complete. Reason for shutdown: %s", err)
}

func resolveCatalystApiURL(cli config.Cli) string {
	if cli.CatalystApiURL != "" {
		return cli.CatalystApiURL
	}

	switch cli.Mode {
	case "all":
		return cli.OwnInternalURL()
	case "cluster-only":
		// Hack to reason about the corresponding stateless catalyst-api service
		// Otherwise we would need to specify CATALYST_API_CATALYST_API_URL env variable, which requires restarting
		// the whole catalyst node
		hostname := os.Getenv("HOSTNAME")                                                                // e.g. "staging-catalyst-0"
		correspondingCatalystApiHostname := strings.Replace(hostname, "-catalyst-", "-catalyst-api-", 1) // e.g. "staging-catalyst-api-0"
		return fmt.Sprintf("http://%s:7979", correspondingCatalystApiHostname)
	}
	// not used for other modes
	return ""
}

// Eventually this will be the main loop of the state machine, but we just have one variable right now.
func reconcileBalancer(ctx context.Context, bal balancer.Balancer, c cluster.Cluster) error {
	memberCh := c.MemberChan()
	// Start from retrying every 4s, but after the first successful update (Serf cluster formed), retry every 1 min
	ticker := time.NewTicker(4 * time.Second)
	for {
		var members []cluster.Member
		var err error
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			members, err = c.MembersFiltered(cluster.MediaFilter, "alive", "")
			if err != nil {
				glog.Errorf("Error getting serf members: %v", err)
				continue
			}
			ticker.Reset(1 * time.Minute)
		case members = <-memberCh:
		}
		err = bal.UpdateMembers(ctx, members)
		if err != nil {
			glog.Errorf("Failed to update load balancer from member list: %v", err)
			continue
		}
	}
}

func handleClusterEvents(ctx context.Context, callbackEndpoint string, c cluster.Cluster) error {
	eventCh := c.EventChan()
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-eventCh:
			processClusterEvent(callbackEndpoint, e)
		}
	}
}

func processClusterEvent(callbackEndpoint string, userEvent serf.UserEvent) {
	client := &http.Client{}
	glog.V(5).Infof("received serf user event, propagating to %s, event=%s", callbackEndpoint, userEvent.String())

	req, err := http.NewRequest("POST", callbackEndpoint, bytes.NewBuffer(userEvent.Payload))
	if err != nil {
		glog.Errorf("error creating request: %v", err)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		glog.Errorf("error sending request: %v", err)
		return
	}
	defer resp.Body.Close()

	glog.V(5).Infof("propagated serf user event to %s, event=%s", callbackEndpoint, userEvent.String())
}

func handleSignals(ctx context.Context) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case s := <-c:
			glog.Errorf("caught signal=%v, attempting clean shutdown", s)
			return fmt.Errorf("caught signal=%v", s)
		case <-ctx.Done():
			return nil
		}
	}
}

func createC2PA(cli *config.Cli) (*c2pa.C2PA, error) {
	if cli == nil {
		return nil, nil
	}
	if cli.C2PAPrivateKeyPath == "" || cli.C2PACertsPath == "" {
		glog.Infof("C2PA private key and/or C2PA certs are not set, will not use C2PA signing")
		return nil, nil
	}
	if _, err := os.Stat(cli.C2PAPrivateKeyPath); err != nil {
		return nil, fmt.Errorf("C2PA private key file not found: %s", cli.C2PAPrivateKeyPath)
	}
	if _, err := os.Stat(cli.C2PACertsPath); err != nil {
		return nil, fmt.Errorf("C2PA certs file not found: %s", cli.C2PACertsPath)
	}
	c := c2pa.NewC2PA("ps256", cli.C2PAPrivateKeyPath, cli.C2PACertsPath)
	return &c, nil
}

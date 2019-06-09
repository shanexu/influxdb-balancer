package relay

import (
	"github.com/tecbot/gorocksdb"
	"sync"
	"time"
)

type Config struct {
	HTTPRelays []HTTPConfig `toml:"http"`
	UDPRelays  []UDPConfig  `toml:"udp"`
	PprofPort  int          `toml:"pprof-port"`
}

type ConfigServer struct {
	Name           string `toml:"name"`
	BindClient     string `toml:"bind-client"`
	BindPeer       string `toml:"bind-peer"`
	Dir            string `toml:"dir"`
	InitialCluster string `toml:"initial-cluster"`
	ClusterState   string `toml:"cluster-state"`
}

type HTTPConfig struct {
	// Name identifies the HTTP relay
	Name string `toml:"name"`

	// Addr should be set to the desired listening host:port
	Addr string `toml:"bind-addr"`

	ConfigServer ConfigServer `toml:"config-server"`

	// Set certificate in order to handle HTTPS requests
	SSLCombinedPem string `toml:"ssl-combined-pem"`
	// Default retention policy to set for forwarded requests
	DefaultRetentionPolicy string `toml:"default-retention-policy"`

	// Outputs is a list of backed servers where writes will be forwarded
	Outputs []HTTPOutputConfig `toml:"output"`

	Databases []DatabaseConfig `toml:"database"`

	DataDir string `toml:"data-dir"`

	UseKafkaOutput   bool          `toml:"use-kafka-output"`
	UseKafkaInput    bool          `toml:"use-kafka-input"`
	KafkaGroup       string        `toml:"kafka-group"`
	SlowLogThreshold time.Duration `toml:"slow-log-threshold"`
	QueryMaxRange    time.Duration `toml:"query-max-range"`
	MetricDb         string        `toml:"metric-db"`
}

type DatabaseConfig struct {
	Name    string   `toml:"name" json:"name"`
	Owner   string   `toml:"owner" json:"owner"`
	Outputs []string `toml:"output" json:"output"`
}

type HTTPOutputConfig struct {
	// Name of the backend server
	Name string `toml:"name" json:"name"`

	// Location should be set to the URL of the backend server's write endpoint
	Location string `toml:"location" json:"location"`

	// WriteEndpoint only for write endpoint
	WriteEndpoint string `toml:"write-endpoint" json:"write-endpoint"`

	// QueryEndpoint only for query endpoint
	QueryEndpoint string `toml:"query-endpoint" json:"query-endpoint"`

	// HealthCheck
	HealthCheck string `toml:"health-check" json:"health-check"`

	// Timeout sets a per-backend timeout for write requests. (Default 10s)
	// The format used is the same seen in time.ParseDuration
	Timeout string `toml:"timeout" json:"timeout"`

	RocksDBBufferSizeMB int `toml:"rocksdb-buffer-size-mb" json:"rocksdb-buffer-size-mb"`

	// Buffer failed writes up to maximum count. (Default 0, retry/buffering disabled)
	BufferSizeMB int `toml:"buffer-size-mb" json:"buffer-size-mb"`

	// Maximum batch size in KB (Default 512)
	MaxBatchKB int `toml:"max-batch-kb" json:"max-batch-kb"`

	// Maximum delay between retry attempts.
	// The format used is the same seen in time.ParseDuration (Default 10s)
	MaxDelayInterval string `toml:"max-delay-interval" json:"max-delay-interval"`

	// Skip TLS verification in order to use self signed certificate.
	// WARNING: It's insecure. Use it only for developing and don't use in production.
	SkipTLSVerification bool `toml:"skip-tls-verification" json:"skip-tls-verification"`

	RateLimit float64 `toml:"rate-limit" json:"rate-limit"`

	RateBurst int `toml:"rate-burst" json:"rate-burst"`

	Global bool `toml:"global" json:"global"`
}

type UDPConfig struct {
	// Name identifies the UDP relay
	Name string `toml:"name"`

	// Addr is where the UDP relay will listen for packets
	Addr string `toml:"bind-addr"`

	// Precision sets the precision of the timestamps (input and output)
	Precision string `toml:"precision"`

	// ReadBuffer sets the socket buffer for incoming connections
	ReadBuffer int `toml:"read-buffer"`

	// Outputs is a list of backend servers where writes will be forwarded
	Outputs []UDPOutputConfig `toml:"output"`
}

type UDPOutputConfig struct {
	// Name identifies the UDP backend
	Name string `toml:"name"`

	// Location should be set to the host:port of the backend server
	Location string `toml:"location"`

	// MTU sets the maximum output payload size, default is 1024
	MTU int `toml:"mtu"`
}

type KafkaClusterConfig struct {
	// Name kafka cluster name
	Name string `toml:"name" json:"name"`

	// Zookeeper zookeeper [host:port]
	Zookeeper []string `toml:"zookeeper" json:"zookeeper"`

	// BootstrapServer kafka server [host:port]
	BootstrapServer []string `toml:"bootstrap-server" json:"bootstrap-server"`

	// Version kafka version 1.1.0
	Version string `toml:"version" json:"version"`
}

type KafkaOutputConfig struct {
	// Name name
	Name string `toml:"name" json:"name"`

	// Cluster kafka cluster name
	Cluster string `toml:"cluster" json:"cluster"`

	// Topic name
	Topic string `toml:"topic" json:"topic"`

	// Protocol 默认为空时 influxdb, json, falcon 其他按需增加
	Protocol map[string]interface{} `toml:"protocol" json:"protocol"`
}

var (
	config       *Config
	OnShutdownWg sync.WaitGroup
	readOpts     = gorocksdb.NewDefaultReadOptions()
	writeOpts    = gorocksdb.NewDefaultWriteOptions()
)

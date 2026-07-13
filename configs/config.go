package configs

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App           AppConfig           `yaml:"app"`
	IDGen         IDGenConfig         `yaml:"idgen"`
	DB            DBConfig            `yaml:"db"`
	Redis         RedisConfig         `yaml:"redis"`
	Elasticsearch ElasticsearchConfig `yaml:"elasticsearch"`
	Chains        []ChainConfig       `yaml:"chains"`
	Workers       WorkersConfig       `yaml:"workers"`
	Batch         BatchConfig         `yaml:"batch"`
	Notifier      NotifierConfig      `yaml:"notifier"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type AppConfig struct {
	Name string `yaml:"name"`
	Env  string `yaml:"env"`
}

type IDGenConfig struct {
	WorkerID *int64 `yaml:"worker_id"`

	EpochMillis int64 `yaml:"epoch_millis"`

	MaxClockBackwardMS int `yaml:"max_clock_backward_ms"`
}

func (c IDGenConfig) WorkerIDValue() int64 {
	if c.WorkerID == nil {
		return 0
	}
	return *c.WorkerID
}

func (c IDGenConfig) MaxClockBackward() time.Duration {
	if c.MaxClockBackwardMS <= 0 {
		return 5 * time.Millisecond
	}
	return time.Duration(c.MaxClockBackwardMS) * time.Millisecond
}

type DBConfig struct {
	DSN string `yaml:"dsn"`

	MaxOpenConns           int `yaml:"max_open_conns"`
	MaxIdleConns           int `yaml:"max_idle_conns"`
	ConnMaxLifetimeSeconds int `yaml:"conn_max_lifetime_seconds"`
	ConnMaxIdleTimeSeconds int `yaml:"conn_max_idle_time_seconds"`
	PingTimeoutSeconds     int `yaml:"ping_timeout_seconds"`

	LogLevel string `yaml:"log_level"`
}

func (c DBConfig) ConnMaxLifetime() time.Duration {
	if c.ConnMaxLifetimeSeconds <= 0 {
		return 30 * time.Minute
	}
	return time.Duration(c.ConnMaxLifetimeSeconds) * time.Second
}

func (c DBConfig) ConnMaxIdleTime() time.Duration {
	if c.ConnMaxIdleTimeSeconds <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(c.ConnMaxIdleTimeSeconds) * time.Second
}

func (c DBConfig) PingTimeout() time.Duration {
	if c.PingTimeoutSeconds <= 0 {
		return 5 * time.Second
	}
	return time.Duration(c.PingTimeoutSeconds) * time.Second
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`

	PoolSize     int `yaml:"pool_size"`
	MinIdleConns int `yaml:"min_idle_conns"`

	DialTimeoutSeconds  int `yaml:"dial_timeout_seconds"`
	ReadTimeoutSeconds  int `yaml:"read_timeout_seconds"`
	WriteTimeoutSeconds int `yaml:"write_timeout_seconds"`
}

func (c RedisConfig) DialTimeout() time.Duration {
	if c.DialTimeoutSeconds <= 0 {
		return 5 * time.Second
	}
	return time.Duration(c.DialTimeoutSeconds) * time.Second
}

func (c RedisConfig) ReadTimeout() time.Duration {
	if c.ReadTimeoutSeconds <= 0 {
		return 3 * time.Second
	}
	return time.Duration(c.ReadTimeoutSeconds) * time.Second
}

func (c RedisConfig) WriteTimeout() time.Duration {
	if c.WriteTimeoutSeconds <= 0 {
		return 3 * time.Second
	}
	return time.Duration(c.WriteTimeoutSeconds) * time.Second
}

type ElasticsearchConfig struct {
	Addresses []string `yaml:"addresses"`

	Username string `yaml:"username"`
	Password string `yaml:"password"`

	IndexPrefix string `yaml:"index_prefix"`

	RequestTimeoutSeconds int `yaml:"request_timeout_seconds"`

	BulkWorkers         int `yaml:"bulk_workers"`
	BulkBatchSize       int `yaml:"bulk_batch_size"`
	BulkFlushIntervalMS int `yaml:"bulk_flush_interval_ms"`
}

func (c ElasticsearchConfig) RequestTimeout() time.Duration {
	if c.RequestTimeoutSeconds <= 0 {
		return 10 * time.Second
	}
	return time.Duration(c.RequestTimeoutSeconds) * time.Second
}

func (c ElasticsearchConfig) BulkFlushInterval() time.Duration {
	if c.BulkFlushIntervalMS <= 0 {
		return time.Second
	}
	return time.Duration(c.BulkFlushIntervalMS) * time.Millisecond
}

type ChainConfig struct {
	Name    string `yaml:"name"`
	Network string `yaml:"network"`

	// evm / solana / btc / etc
	Type string `yaml:"type"`

	ChainID uint64 `yaml:"chain_id"`

	Enabled bool `yaml:"enabled"`

	StartBlock    uint64 `yaml:"start_block"`
	Confirmations uint64 `yaml:"confirmations"`
	ChunkSize     uint64 `yaml:"chunk_size"`

	RPC RPCConfig `yaml:"rpc"`

	// EVM专有字段
	Source EVMSourceConfig `yaml:"source"`
	// EVM的业务Handler
	Handler EVMHandlerConfig `yaml:"handler"`
}

type RPCConfig struct {
	URLs []string `yaml:"urls"`

	TimeoutSeconds int `yaml:"timeout_seconds"`

	MaxConcurrentRequests int `yaml:"max_concurrent_requests"`
	RequestsPerSecond     int `yaml:"requests_per_second"`

	RetryMax             int `yaml:"retry_max"`
	RetryBaseDelayMS     int `yaml:"retry_base_delay_ms"`
	RetryMaxDelaySeconds int `yaml:"retry_max_delay_seconds"`
}

func (c RPCConfig) Timeout() time.Duration {
	if c.TimeoutSeconds <= 0 {
		return 10 * time.Second
	}
	return time.Duration(c.TimeoutSeconds) * time.Second
}

func (c RPCConfig) RetryBaseDelay() time.Duration {
	if c.RetryBaseDelayMS <= 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(c.RetryBaseDelayMS) * time.Millisecond
}

func (c RPCConfig) RetryMaxDelay() time.Duration {
	if c.RetryMaxDelaySeconds <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.RetryMaxDelaySeconds) * time.Second
}

type WorkersConfig struct {
	FetchWorkers   int `yaml:"fetch_workers"`
	DecodeWorkers  int `yaml:"decode_workers"`
	PersistWorkers int `yaml:"persist_workers"`
	ESWorkers      int `yaml:"es_workers"`
	NotifyWorkers  int `yaml:"notify_workers"`

	QueueSize int `yaml:"queue_size"`
}

type BatchConfig struct {
	EventBatchSize       int `yaml:"event_batch_size"`
	EventFlushIntervalMS int `yaml:"event_flush_interval_ms"`

	OutboxBatchSize       int `yaml:"outbox_batch_size"`
	OutboxFlushIntervalMS int `yaml:"outbox_flush_interval_ms"`

	ESBatchSize       int `yaml:"es_batch_size"`
	ESFlushIntervalMS int `yaml:"es_flush_interval_ms"`
}

func (c BatchConfig) EventFlushInterval() time.Duration {
	if c.EventFlushIntervalMS <= 0 {
		return time.Second
	}
	return time.Duration(c.EventFlushIntervalMS) * time.Millisecond
}

func (c BatchConfig) OutboxFlushInterval() time.Duration {
	if c.OutboxFlushIntervalMS <= 0 {
		return time.Second
	}
	return time.Duration(c.OutboxFlushIntervalMS) * time.Millisecond
}

func (c BatchConfig) ESFlushInterval() time.Duration {
	if c.ESFlushIntervalMS <= 0 {
		return time.Second
	}
	return time.Duration(c.ESFlushIntervalMS) * time.Millisecond
}

type NotifierConfig struct {
	Telegram TelegramConfig `yaml:"telegram"`
}

// TG配置
type TelegramConfig struct {
	Enabled bool `yaml:"enabled"`

	BotToken string `yaml:"bot_token"`
	ChatID   string `yaml:"chat_id"`

	RequestTimeoutSeconds int `yaml:"request_timeout_seconds"`
	MaxConcurrentSends    int `yaml:"max_concurrent_sends"`
}

func (c TelegramConfig) RequestTimeout() time.Duration {
	if c.RequestTimeoutSeconds <= 0 {
		return 10 * time.Second
	}
	return time.Duration(c.RequestTimeoutSeconds) * time.Second
}

type ObservabilityConfig struct {
	LogLevel string `yaml:"log_level"`
	Metrics  bool   `yaml:"metrics"`
	Tracing  bool   `yaml:"tracing"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		path = "configs/local.yaml"
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}

	cfg.applyDefault()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config %s: %w", path, err)
	}

	return &cfg, nil
}

func (c *Config) applyDefault() {
	if c.App.Name == "" {
		c.App.Name = "token-discover"
	}

	if c.App.Env == "" {
		c.App.Env = "local"
	}

	if c.IDGen.MaxClockBackwardMS <= 0 {
		c.IDGen.MaxClockBackwardMS = 5
	}

	if c.DB.MaxOpenConns <= 0 {
		c.DB.MaxOpenConns = 80
	}

	if c.DB.MaxIdleConns <= 0 {
		c.DB.MaxIdleConns = 40
	}

	if c.DB.ConnMaxLifetimeSeconds <= 0 {
		c.DB.ConnMaxLifetimeSeconds = 1800
	}

	if c.DB.ConnMaxIdleTimeSeconds <= 0 {
		c.DB.ConnMaxIdleTimeSeconds = 300
	}

	if c.DB.PingTimeoutSeconds <= 0 {
		c.DB.PingTimeoutSeconds = 5
	}

	if c.DB.LogLevel == "" {
		c.DB.LogLevel = "warn"
	}

	if c.Redis.PoolSize <= 0 {
		c.Redis.PoolSize = 50
	}

	if c.Redis.MinIdleConns <= 0 {
		c.Redis.MinIdleConns = 10
	}

	if c.Redis.DialTimeoutSeconds <= 0 {
		c.Redis.DialTimeoutSeconds = 5
	}

	if c.Redis.ReadTimeoutSeconds <= 0 {
		c.Redis.ReadTimeoutSeconds = 3
	}

	if c.Redis.WriteTimeoutSeconds <= 0 {
		c.Redis.WriteTimeoutSeconds = 3
	}

	if c.Elasticsearch.IndexPrefix == "" {
		c.Elasticsearch.IndexPrefix = "token_discover"
	}

	if c.Elasticsearch.RequestTimeoutSeconds <= 0 {
		c.Elasticsearch.RequestTimeoutSeconds = 10
	}

	if c.Elasticsearch.BulkWorkers <= 0 {
		c.Elasticsearch.BulkWorkers = 4
	}

	if c.Elasticsearch.BulkBatchSize <= 0 {
		c.Elasticsearch.BulkBatchSize = 500
	}

	if c.Elasticsearch.BulkFlushIntervalMS <= 0 {
		c.Elasticsearch.BulkFlushIntervalMS = 1000
	}

	if c.Workers.FetchWorkers <= 0 {
		c.Workers.FetchWorkers = 10
	}

	if c.Workers.DecodeWorkers <= 0 {
		c.Workers.DecodeWorkers = 20
	}

	if c.Workers.PersistWorkers <= 0 {
		c.Workers.PersistWorkers = 4
	}

	if c.Workers.ESWorkers <= 0 {
		c.Workers.ESWorkers = 4
	}

	if c.Workers.NotifyWorkers <= 0 {
		c.Workers.NotifyWorkers = 5
	}

	if c.Workers.QueueSize <= 0 {
		c.Workers.QueueSize = 10000
	}

	if c.Batch.EventBatchSize <= 0 {
		c.Batch.EventBatchSize = 500
	}

	if c.Batch.EventFlushIntervalMS <= 0 {
		c.Batch.EventFlushIntervalMS = 1000
	}

	if c.Batch.OutboxBatchSize <= 0 {
		c.Batch.OutboxBatchSize = 200
	}

	if c.Batch.OutboxFlushIntervalMS <= 0 {
		c.Batch.OutboxFlushIntervalMS = 1000
	}

	if c.Batch.ESBatchSize <= 0 {
		c.Batch.ESBatchSize = 500
	}

	if c.Batch.ESFlushIntervalMS <= 0 {
		c.Batch.ESFlushIntervalMS = 1000
	}

	for i := range c.Chains {
		if c.Chains[i].Type == "" {
			c.Chains[i].Type = "evm"
		}

		if c.Chains[i].Confirmations == 0 {
			c.Chains[i].Confirmations = 3
		}

		if c.Chains[i].ChunkSize == 0 {
			c.Chains[i].ChunkSize = 500
		}

		if c.Chains[i].RPC.TimeoutSeconds <= 0 {
			c.Chains[i].RPC.TimeoutSeconds = 10
		}

		if c.Chains[i].RPC.MaxConcurrentRequests <= 0 {
			c.Chains[i].RPC.MaxConcurrentRequests = 20
		}

		if c.Chains[i].RPC.RequestsPerSecond <= 0 {
			c.Chains[i].RPC.RequestsPerSecond = 30
		}

		if c.Chains[i].RPC.RetryMax <= 0 {
			c.Chains[i].RPC.RetryMax = 5
		}

		if c.Chains[i].RPC.RetryBaseDelayMS <= 0 {
			c.Chains[i].RPC.RetryBaseDelayMS = 500
		}

		if c.Chains[i].RPC.RetryMaxDelaySeconds <= 0 {
			c.Chains[i].RPC.RetryMaxDelaySeconds = 30
		}

		if c.Chains[i].Handler.EventStore.BatchSize <= 0 {
			c.Chains[i].Handler.EventStore.BatchSize = c.Batch.EventBatchSize
		}
	}

	if c.Notifier.Telegram.RequestTimeoutSeconds <= 0 {
		c.Notifier.Telegram.RequestTimeoutSeconds = 10
	}

	if c.Notifier.Telegram.MaxConcurrentSends <= 0 {
		c.Notifier.Telegram.MaxConcurrentSends = 5
	}

	if c.Observability.LogLevel == "" {
		c.Observability.LogLevel = "info"
	}
}

func (c *Config) Validate() error {
	if c.IDGen.WorkerID == nil {
		return fmt.Errorf("config idgen.worker_id is required")
	}

	if *c.IDGen.WorkerID < 0 || *c.IDGen.WorkerID > 1023 {
		return fmt.Errorf("config idgen.worker_id out of range: %d, allowed range: 0-1023", *c.IDGen.WorkerID)
	}

	if c.DB.DSN == "" {
		return fmt.Errorf("config db.dsn is required")
	}

	if c.Redis.Addr == "" {
		return fmt.Errorf("config redis.addr is required")
	}

	if len(c.Elasticsearch.Addresses) == 0 {
		return fmt.Errorf("config elasticsearch.addresses is required")
	}

	if len(c.Chains) == 0 {
		return fmt.Errorf("config chains is required")
	}

	if c.Notifier.Telegram.Enabled && c.Notifier.Telegram.ChatID == "" {
		return fmt.Errorf("config notifier.telegram.chat_id is required when telegram is enabled")
	}

	for i, chain := range c.Chains {
		if chain.Name == "" {
			return fmt.Errorf("config chains[%d].name is required", i)
		}

		if chain.Network == "" {
			return fmt.Errorf("config chains[%d].network is required", i)
		}

		if chain.Type == "" {
			return fmt.Errorf("config chains[%d].type is required", i)
		}

		if chain.ChainID == 0 {
			return fmt.Errorf("config chains[%d].chain_id is required", i)
		}

		if len(chain.RPC.URLs) == 0 {
			return fmt.Errorf("config chains[%d].rpc.urls is required", i)
		}

		if chain.Enabled && chain.Type == "evm" {
			if chain.Source.Name == "" {
				return fmt.Errorf("config chains[%d].source.name is required", i)
			}

			if len(chain.Source.Specs) == 0 && !chain.Source.AllowEmptyFilter {
				return fmt.Errorf("config chains[%d].source.specs is required when allow_empty_filter=false", i)
			}

			if len(chain.Handler.Decoders) == 0 {
				return fmt.Errorf("config chains[%d].handler.decoders is required", i)
			}

			for j, decoder := range chain.Handler.Decoders {
				if decoder.Type == "" {
					return fmt.Errorf("config chains[%d].handler.decoders[%d].type is required", i, j)
				}
			}

			for j, filter := range chain.Handler.Filters {
				if filter.Type == "" {
					return fmt.Errorf("config chains[%d].handler.filters[%d].type is required", i, j)
				}
			}
		}
	}

	if c.Notifier.Telegram.Enabled && c.Notifier.Telegram.BotToken == "" {
		return fmt.Errorf("config notifier.telegram.bot_token is required when telegram is enabled")
	}

	return nil
}

func (c *Config) EnabledChains() []ChainConfig {
	chains := make([]ChainConfig, 0, len(c.Chains))

	for _, chain := range c.Chains {
		if chain.Enabled {
			chains = append(chains, chain)
		}
	}

	return chains
}

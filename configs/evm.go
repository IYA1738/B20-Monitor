package configs

import (
	"time"

	"token-discover-demo/chains/evm"
	"token-discover-demo/database"
)

// EVMSourceConfig 是配置层的 eth_getLogs source 配置。
// 它对应 chains/evm/source.go 里的 QuerySourceFromConfig。
type EVMSourceConfig struct {
	Name string `yaml:"name" json:"name"`

	Specs []EVMQuerySpecConfig `yaml:"specs" json:"specs"`

	MaxAddressesPerQuery int `yaml:"max_addresses_per_query" json:"max_addresses_per_query"`

	AllowEmptyFilter bool `yaml:"allow_empty_filter" json:"allow_empty_filter"`
}

type EVMQuerySpecConfig struct {
	Addresses []string   `yaml:"addresses" json:"addresses"`
	Topics    [][]string `yaml:"topics" json:"topics"`
}

func (c EVMSourceConfig) ToEVMQuerySourceConfig() evm.QuerySourceFromConfig {
	specs := make([]evm.QuerySpecConfig, 0, len(c.Specs))

	for _, spec := range c.Specs {
		specs = append(specs, evm.QuerySpecConfig{
			Addresses: spec.Addresses,
			Topics:    spec.Topics,
		})
	}

	return evm.QuerySourceFromConfig{
		Name:                 c.Name,
		Specs:                specs,
		MaxAddressesPerQuery: c.MaxAddressesPerQuery,
		AllowEmptyFilter:     c.AllowEmptyFilter,
	}
}

// EVMHandlerConfig 是配置层的 handler 配置。
type EVMHandlerConfig struct {
	FailOnUnmatched bool `yaml:"fail_on_unmatched" json:"fail_on_unmatched"`

	IgnoreDecodeError bool `yaml:"ignore_decode_error" json:"ignore_decode_error"`

	EventStore EVMEventStoreConfig `yaml:"event_store" json:"event_store"`

	Decoders []evm.ComponentConfig `yaml:"decoders" json:"decoders"`

	Filters []evm.ComponentConfig `yaml:"filters" json:"filters"`
}

type EVMEventStoreConfig struct {
	BatchSize int `yaml:"batch_size" json:"batch_size"`
}

func (c EVMHandlerConfig) ToPipelineHandlerConfig() evm.PipelineHandlerConfig {
	return evm.PipelineHandlerConfig{
		FailOnUnmatched:   c.FailOnUnmatched,
		IgnoreDecodeError: c.IgnoreDecodeError,
	}
}

func (c EVMHandlerConfig) ToHandlerComponentsConfig() evm.HandlerComponentsConfig {
	return evm.HandlerComponentsConfig{
		Decoders: c.Decoders,
		Filters:  c.Filters,
	}
}

func (c EVMHandlerConfig) ToEventStoreConfig() database.EventStoreConfig {
	return database.EventStoreConfig{
		BatchSize: c.EventStore.BatchSize,
	}
}

// ToEVMRPCConfig 把 configs.RPCConfig 转成 chains/evm.RPCConfig。
// RPCConfig 仍然放在原来的 config.go，不移动，减少影响范围。
func (c RPCConfig) ToEVMRPCConfig() evm.RPCConfig {
	return evm.RPCConfig{
		URLs:                  c.URLs,
		Timeout:               c.Timeout(),
		MaxConcurrentRequests: c.MaxConcurrentRequests,
		RequestsPerSecond:     c.RequestsPerSecond,
		RetryMax:              c.RetryMax,
		RetryBaseDelay:        c.RetryBaseDelay(),
		RetryMaxDelay:         c.RetryMaxDelay(),
	}
}

func (c ChainConfig) ToEVMScannerConfig() evm.ScannerConfig {
	return evm.ScannerConfig{
		ChainID:   c.ChainID,
		ChainName: c.Name,
		Network:   c.Network,
		// Monitor:       c.Name,
		StartBlock:    c.StartBlock,
		Confirmations: c.Confirmations,
		ChunkSize:     c.ChunkSize,

		FetchWorkers:      0,
		MaxInflightChunks: 0,

		PollInterval: 3 * time.Second,
		ErrorBackoff: 5 * time.Second,
	}
}

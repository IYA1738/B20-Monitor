package evm

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

type QuerySpec struct {
	Addresses []common.Address
	Topics    [][]common.Hash
}

type QuerySourceConfig struct {
	Name  string
	Specs []QuerySpec

	// 一个查询最多允许查多少个地址
	MaxAddressesPerQuery int

	// 是否允许空过滤器 查询chunk内所有地址和日志
	AllowEmptyFilter bool
}

type QuerySpecConfig struct {
	// Addresses 是 hex address 字符串列表。
	Addresses []string `yaml:"addresses" json:"addresses"`

	// Topics 是 topic 字符串矩阵。
	//
	// 示例：
	// topics:
	//   - ["0xddf252ad..."]  // topic0
	//   - []                // topic1 不限制
	//   - ["0x0000..."]      // topic2
	Topics [][]string `yaml:"topics" json:"topics"`
}

type QuerySourceFromConfig struct {
	Name                 string            `yaml:"name" json:"name"`
	Specs                []QuerySpecConfig `yaml:"spec" json:"spec"`
	MaxAddressesPerQuery int               `yaml:"max_addresses_per_query" json:"max_addresses_per_query"`
	AllowEmptyFilter     bool              `yaml:"allow_empty_filter" json:"allow_empty_filter"`
}

type QuerySource struct {
	name string

	specs []QuerySpec

	maxAddressesPerQuery int

	allowEmptyFilter bool
}

func NewQuerySourceFromConfig(cfg QuerySourceFromConfig) (*QuerySource, error) {
	specs := make([]QuerySpec, 0, len(cfg.Specs))

	for i, rawSpec := range cfg.Specs {
		addresses, err := parseAddresses(rawSpec.Addresses)
		if err != nil {
			return nil, fmt.Errorf("spec[%d] addresses: %w", i, err)
		}

		topics, err := parseTopicMatrix(rawSpec.Topics)
		if err != nil {
			return nil, fmt.Errorf("spec[%d] topics: %w", i, err)
		}

		specs = append(specs, QuerySpec{
			Addresses: addresses,
			Topics:    topics,
		})
	}
	return NewQuerySource(QuerySourceConfig{
		Name:                 cfg.Name,
		Specs:                specs,
		MaxAddressesPerQuery: cfg.MaxAddressesPerQuery,
		AllowEmptyFilter:     cfg.AllowEmptyFilter,
	})
}

func NewQuerySource(cfg QuerySourceConfig) (*QuerySource, error) {
	name := strings.TrimSpace(cfg.Name)

	if name == "" {
		return nil, fmt.Errorf("query source name is required")
	}

	if cfg.MaxAddressesPerQuery <= 0 {
		cfg.MaxAddressesPerQuery = 100
	}

	if len(cfg.Specs) == 0 && !cfg.AllowEmptyFilter {
		return nil, fmt.Errorf("query source specs is empty: name=%s", name)
	}

	for i, spec := range cfg.Specs {
		if len(spec.Addresses) == 0 && len(spec.Topics) == 0 && !cfg.AllowEmptyFilter {
			return nil, fmt.Errorf("query source spec[%d] is empty: name=%s", i, name)
		}

		if len(spec.Topics) > 4 {
			return nil, fmt.Errorf("query spec[%d] topics length must be <= 4, got %d", i, len(spec.Topics))
		}
	}

	return &QuerySource{
		name:                 name,
		specs:                cfg.Specs,
		maxAddressesPerQuery: cfg.MaxAddressesPerQuery,
		allowEmptyFilter:     cfg.AllowEmptyFilter,
	}, nil
}

func (s *QuerySource) Name() string {
	if s == nil {
		return ""
	}

	return s.name
}

func parseTopicMatrix(rawTopics [][]string) ([][]common.Hash, error) {
	rawTopicsLen := len(rawTopics)

	if rawTopicsLen == 0 {
		return nil, fmt.Errorf("raw topics is nil")
	}

	if rawTopicsLen > 4 {
		return nil, fmt.Errorf("topics length must be <= 4, got %d", rawTopicsLen)
	}

	topics := make([][]common.Hash, rawTopicsLen)

	for topicIndex, alterNatives := range rawTopics {
		// 这个表示不限制这个topic的值
		alterNativesLen := len(alterNatives)
		if alterNativesLen == 0 {
			topics[topicIndex] = nil
			continue
		}

		topics[topicIndex] = make([]common.Hash, 0, alterNativesLen)

		for alterIndex, rawTopic := range alterNatives {
			hash, err := parseTopic(rawTopic)
			if err != nil {
				return nil, fmt.Errorf("topics[%d][%d]: %w", topicIndex, alterIndex, err)
			}
			topics[topicIndex] = append(topics[topicIndex], hash)
		}
	}
	return topics, nil
}

func parseTopic(rawTopic string) (common.Hash, error) {
	rawTopic = strings.TrimSpace(rawTopic)

	if rawTopic == "" {
		return common.Hash{}, fmt.Errorf("topic is empty")
	}

	if !strings.HasPrefix(rawTopic, "0x") && !strings.HasPrefix(rawTopic, "0X") {
		return common.Hash{}, fmt.Errorf("topic must be hex string with 0x prefix")
	}

	hexPart := rawTopic[2:]

	if len(hexPart) != 64 {
		return common.Hash{}, fmt.Errorf("topic hex string must be 64 characters long, got %d", len(hexPart))
	}

	if _, err := hex.DecodeString(hexPart); err != nil {
		return common.Hash{}, fmt.Errorf("hash contains non-hex chars: %s", rawTopic)
	}

	return common.HexToHash(rawTopic), nil
}

func parseAddresses(rawAddresses []string) ([]common.Address, error) {
	addresses := make([]common.Address, 0, len(rawAddresses))

	for i, rawAddress := range rawAddresses {
		rawAddress = strings.TrimSpace(rawAddress)

		if rawAddress == "" {
			return nil, fmt.Errorf("addresses[%d] is empty", i)
		}

		if !common.IsHexAddress(rawAddress) {
			return nil, fmt.Errorf("address[%d] is invalid:%s", i, rawAddress)
		}

		addresses = append(addresses, common.HexToAddress(rawAddress))
	}

	return addresses, nil
}

func (s *QuerySource) BuildQueries(ctx context.Context, fromBlock uint64, toBlock uint64) ([]ethereum.FilterQuery, error) {
	if s == nil {
		return nil, fmt.Errorf("Query source is nil")
	}

	if fromBlock > toBlock {
		return nil, fmt.Errorf("fromBlock=%d is greater than toBlock=%d", fromBlock, toBlock)
	}

	if len(s.specs) == 0 {
		if !s.allowEmptyFilter {
			return nil, fmt.Errorf("query source specs is empty and empty filter is not allowed: name=%s", s.name)
		}

		return []ethereum.FilterQuery{
			{
				FromBlock: new(big.Int).SetUint64(fromBlock),
				ToBlock:   new(big.Int).SetUint64(toBlock),
			},
		}, nil
	}

	queries := make([]ethereum.FilterQuery, 0, len(s.specs))

	for specIndex, spec := range s.specs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// 不按Address过滤 只按Topics过滤
		if len(spec.Addresses) == 0 {
			queries = append(queries, ethereum.FilterQuery{
				FromBlock: new(big.Int).SetUint64(fromBlock),
				ToBlock:   new(big.Int).SetUint64(toBlock),
				Topics:    cloneTopicMatrix(spec.Topics),
			})

			continue
		}

		// 如果Address很多 那就拆成多个Address chunks
		addressChunks := splitAddresses(spec.Addresses, s.maxAddressesPerQuery)

		for _, addressChunk := range addressChunks {
			if len(addressChunk) == 0 {
				continue
			}

			queries = append(queries, ethereum.FilterQuery{
				FromBlock: new(big.Int).SetUint64(fromBlock),
				ToBlock:   new(big.Int).SetUint64(toBlock),
				Addresses: cloneAddresses(addressChunk),
				Topics:    cloneTopicMatrix(spec.Topics),
			})
		}
		_ = specIndex // Debug的时候可能要用，但是现在不用，先随便用一下避免报错
	}
	if len(queries) == 0 {
		return nil, fmt.Errorf("no queries built: source=%s from=%d to=%d", s.name, fromBlock, toBlock)
	}
	return queries, nil
}

func cloneAddresses(in []common.Address) []common.Address {
	if len(in) == 0 {
		return nil
	}
	out := make([]common.Address, len(in))
	copy(out, in)
	return out
}

func splitAddresses(addresses []common.Address, size int) [][]common.Address {
	if len(addresses) == 0 {
		return nil
	}

	if size <= 0 || len(addresses) <= size {
		return [][]common.Address{addresses}
	}

	out := make([][]common.Address, 0, (len(addresses)+size-1)/size)

	for start := 0; start < len(addresses); start += size {
		end := start + size
		if end > len(addresses) {
			end = len(addresses) // 不用-1是因为下面的是左闭右开 本身就不包含最后一个index
		}
		out = append(out, addresses[start:end])
	}
	return out
}

func cloneTopicMatrix(in [][]common.Hash) [][]common.Hash {
	if len(in) == 0 {
		return nil
	}

	out := make([][]common.Hash, len(in))

	for i := range in {
		if len(in[i]) == 0 {
			out[i] = nil
			continue
		}

		out[i] = make([]common.Hash, len(in[i]))
		copy(out[i], in[i])
	}
	return out
}

package evm

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// 用于注册新的事件解码和过滤

type ComponentConfig struct {
	// 组件类型 例如当前component是检测b20_created 过滤blacklist 还是别的啥东西
	Type string `yaml:"type" json:"type"`

	// 组件名
	Name string `yaml:"name" json:"name"`

	// 是否启动当前组件进行监控和过滤
	Enabled *bool `yaml:"enabled" json:"enabled"`

	// 组件自己的参数
	Params map[string]any `yaml:"params" json:"params"`
}

// 默认启用
func (c ComponentConfig) IsEnabled() bool {
	if c.Enabled == nil {
		return true
	}

	return *c.Enabled
}

type HandlerComponentsConfig struct {
	Decoders []ComponentConfig `yaml:"decoders" json:"decoders"`
	Filters  []ComponentConfig `yaml:"filters" json:"filters"`
}

type DecoderFactory func(ctx context.Context, cfg ComponentConfig) (LogDecoder, error)

type FilterFactory func(ctx context.Context, cfg ComponentConfig) (EventFilter, error)

// 根据Type决定当前创建的是decoder还是filter
type ComponentRegistry struct {
	decoderFactories map[string]DecoderFactory
	filterFactories  map[string]FilterFactory
}

func NewComponentRegistry() *ComponentRegistry {
	return &ComponentRegistry{
		decoderFactories: make(map[string]DecoderFactory),
		filterFactories:  make(map[string]FilterFactory),
	}
}

func (r *ComponentRegistry) RegistryDecoder(typeName string, factory DecoderFactory) error {
	if r == nil {
		return fmt.Errorf("component registry is nil")
	}

	typeName = normalizeComponentType(typeName)

	if typeName == "" {
		return fmt.Errorf("type name is nil")
	}

	if factory == nil {
		return fmt.Errorf("decoder factory is nil: type=%s", typeName)
	}

	if r.decoderFactories == nil {
		r.decoderFactories = make(map[string]DecoderFactory)
	}

	if _, exists := r.decoderFactories[typeName]; exists {
		return fmt.Errorf("decoder type already registered: %s", typeName)
	}

	r.decoderFactories[typeName] = factory
	return nil
}

func normalizeComponentType(typeName string) string {
	return strings.TrimSpace(typeName)
}

func (r *ComponentRegistry) RegistryFilter(typeName string, factory FilterFactory) error {
	if r == nil {
		return fmt.Errorf("component registry is nil")
	}

	typeName = normalizeComponentType(typeName)
	if typeName == "" {
		return fmt.Errorf("filter type is empty")
	}

	if factory == nil {
		return fmt.Errorf("filter factory is nil: type=%s", typeName)
	}

	if r.filterFactories == nil {
		r.filterFactories = make(map[string]FilterFactory)
	}

	if _, exists := r.filterFactories[typeName]; exists {
		return fmt.Errorf("filter type already registered: %s", typeName)
	}

	r.filterFactories[typeName] = factory
	return nil
}

func (r *ComponentRegistry) BuildDecoders(ctx context.Context, configs []ComponentConfig) ([]LogDecoder, error) {
	if r == nil {
		return nil, fmt.Errorf("component registry is nil")
	}

	decoders := make([]LogDecoder, 0, len(configs))

	for i, cfg := range configs {
		if !cfg.IsEnabled() {
			continue
		}

		typeName := normalizeComponentType(cfg.Type)

		if typeName == "" {
			return []LogDecoder{}, fmt.Errorf("type name is nil")
		}

		factory := r.decoderFactories[typeName]

		if factory == nil {
			return nil, fmt.Errorf(
				"decoder config[%d] unknown type=%s registered=%v",
				i,
				typeName,
				r.RegisteredDecoderTypes(),
			)
		}

		decoder, err := factory(ctx, cfg)

		if err != nil {
			return nil, fmt.Errorf("build decoder config[%d] type=%s: %w", i, typeName, err)
		}

		if decoder == nil {
			return nil, fmt.Errorf("decoder factory returned nil: config[%d] type=%s", i, typeName)
		}

		decoders = append(decoders, decoder)
	}
	return decoders, nil
}

func (r *ComponentRegistry) RegisteredDecoderTypes() []string {
	if r == nil || len(r.decoderFactories) == 0 {
		return nil
	}

	out := make([]string, 0, len(r.decoderFactories))

	for typeName := range r.decoderFactories {
		out = append(out, typeName)
	}

	sort.Strings(out)
	return out
}

func (r *ComponentRegistry) BuildFilters(ctx context.Context, configs []ComponentConfig) ([]EventFilter, error) {
	if r == nil {
		return nil, fmt.Errorf("component registry is nil")
	}

	filters := make([]EventFilter, 0, len(configs))

	for i, cfg := range configs {
		if !cfg.IsEnabled() {
			continue
		}

		typeName := normalizeComponentType(cfg.Type)

		if typeName == "" {
			return nil, fmt.Errorf("type name is nil")
		}

		factory := r.filterFactories[typeName]

		if factory == nil {
			return nil, fmt.Errorf(
				"filter config[%d] unknown type=%s registered=%v",
				i,
				typeName,
				r.RegisteredFilterTypes(),
			)
		}

		filter, err := factory(ctx, cfg)

		if err != nil {
			return nil, fmt.Errorf("build filter config[%d] type=%s: %w", i, typeName, err)
		}

		if filter == nil {
			return nil, fmt.Errorf("filter factory returned nil: config[%d] type=%s", i, typeName)
		}

		filters = append(filters, filter)
	}

	return filters, nil
}

func (r *ComponentRegistry) RegisteredFilterTypes() []string {
	if r == nil || len(r.filterFactories) == 0 {
		return nil
	}

	out := make([]string, 0, len(r.filterFactories))

	for typeName := range r.filterFactories {
		out = append(out, typeName)
	}

	sort.Strings(out)

	return out
}

func DecodeComponentParams(cfg ComponentConfig, out any) error {
	if out == nil {
		return fmt.Errorf("params output is nil")
	}

	if len(cfg.Params) == 0 {
		return nil
	}

	body, err := json.Marshal(cfg.Params)

	if err != nil {
		return fmt.Errorf("marshal component params type=%s: %w", cfg.Type, err)
	}

	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode component params type=%s: %w", cfg.Type, err)
	}

	return nil
}

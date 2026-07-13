package b20

import (
	"context"
	"fmt"
	"token-discover-demo/chains/evm"
	"token-discover-demo/database"
)

const (
	CreatedDecoderType         = "b20_created"
	CreatorBlacklistFilterType = "b20_creator_blacklist"
)

func Register(registry *evm.ComponentRegistry) error {
	if registry == nil {
		return fmt.Errorf("component registry is nil")
	}

	if err := registry.RegistryDecoder(CreatedDecoderType, NewCreatedDecoderFromConfig); err != nil {
		return fmt.Errorf("register decoder %s: %w", CreatedDecoderType, err)
	}

	return nil
}

func RegisterCreatorBlacklistFilter(registry *evm.ComponentRegistry, store *database.BlacklistStore) error {
	if registry == nil {
		return fmt.Errorf("registry is nil")
	}
	if store == nil {
		return fmt.Errorf("blacklist store is nil")
	}
	err := registry.RegistryFilter(
		CreatorBlacklistFilterType,
		func(ctx context.Context, cfg evm.ComponentConfig) (evm.EventFilter, error) {
			return NewCreatorBlacklistFilter(store)
		},
	)
	if err != nil {
		return fmt.Errorf("register filter %s: %w", CreatorBlacklistFilterType, err)
	}

	return nil
}

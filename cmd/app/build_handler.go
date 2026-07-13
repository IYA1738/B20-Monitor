package app

import (
	"context"
	"fmt"
	"token-discover-demo/chains/evm"
	"token-discover-demo/database"
	"token-discover-demo/features/b20"

	"gorm.io/gorm"
)

type BuildEVMHandlerOptions struct {
	DB *gorm.DB

	IDGen database.IDGenerator

	HandlerConfig evm.PipelineHandlerConfig

	ComponentsConfig evm.HandlerComponentsConfig

	EventStoreConfig database.EventStoreConfig

	NotificationPlanner database.NotificationRoutePlanner
}

func buildEVMHandler(ctx context.Context, opt BuildEVMHandlerOptions) (*evm.PipelineHandler, error) {
	if opt.DB == nil {
		return nil, fmt.Errorf("db is nil")
	}

	if opt.IDGen == nil {
		return nil, fmt.Errorf("id generator is nil")
	}

	eventStore, err := database.NewEventStore(
		opt.DB,
		opt.IDGen,
		opt.EventStoreConfig,
		database.WithNotificationRoutePlanner(opt.NotificationPlanner),
	)

	if err != nil {
		return nil, fmt.Errorf("new event store: %w", err)
	}

	registry := evm.NewComponentRegistry()

	if err := b20.Register(registry); err != nil {
		return nil, fmt.Errorf("register b20 feature: %w", err)
	}

	blacklistStore, err := database.NewBlacklistStore(opt.DB)
	if err != nil {
		return nil, fmt.Errorf("new blacklist store: %w", err)
	}

	if err := b20.RegisterCreatorBlacklistFilter(registry, blacklistStore); err != nil {
		return nil, fmt.Errorf("register b20 creator blacklist filter: %w", err)
	}

	handler, err := registry.BuildPipelineHandler(
		ctx,
		opt.HandlerConfig,
		opt.ComponentsConfig,
		eventStore,
	)

	if err != nil {
		return nil, fmt.Errorf("build pipeline handler: %w", err)
	}

	return handler, nil
}

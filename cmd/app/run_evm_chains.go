package app

import (
	"context"
	"fmt"
	"log"
	"token-discover-demo/chains/evm"
	"token-discover-demo/common/graceful"
	"token-discover-demo/configs"
	"token-discover-demo/database"

	"gorm.io/gorm"
)

type RunEVMChainsOptions struct {
	Manager *graceful.Manager

	DB *gorm.DB

	IDGen database.IDGenerator

	Config *configs.Config
}

func runEVMChains(ctx context.Context, opt RunEVMChainsOptions) error {
	if opt.Manager == nil {
		return fmt.Errorf("graceful manager is nil")
	}

	if opt.DB == nil {
		return fmt.Errorf("db is nil")
	}

	if opt.IDGen == nil {
		return fmt.Errorf("id generator is nil")
	}

	if opt.Config == nil {
		return fmt.Errorf("config is nil")
	}

	enabledChains := opt.Config.EnabledChains()
	if len(enabledChains) == 0 {
		log.Printf("[evm-app] no enabled chains")
		return nil
	}

	notificationPlanner := newNotificationRoutePlanner(opt.Config.Notifier)

	for _, chainCfg := range enabledChains {
		if chainCfg.Type != "evm" {
			continue
		}

		// 避免闭包捕获 range 变量。
		chainCfg := chainCfg

		if err := startEVMChain(ctx, opt, chainCfg, notificationPlanner); err != nil {
			return fmt.Errorf(
				"start evm chain=%s network=%s chain_id=%d: %w",
				chainCfg.Name,
				chainCfg.Network,
				chainCfg.ChainID,
				err,
			)
		}
	}
	return nil
}

func startEVMChain(ctx context.Context, opt RunEVMChainsOptions, chainCfg configs.ChainConfig, notificationPlanner database.NotificationRoutePlanner) error {
	log.Printf(
		"[evm-app] starting evm chain name=%s network=%s chain_id=%d start_block=%d confirmations=%d chunk_size=%d",
		chainCfg.Name,
		chainCfg.Network,
		chainCfg.ChainID,
		chainCfg.StartBlock,
		chainCfg.Confirmations,
		chainCfg.ChunkSize,
	)
	rpcClient, err := evm.OpenRPCClient(ctx, chainCfg.RPC.ToEVMRPCConfig())
	if err != nil {
		return fmt.Errorf("open rpc client: %w", err)
	}

	cleanupName := fmt.Sprintf(
		"rpc/%s/%s/%d",
		chainCfg.Network,
		chainCfg.Name,
		chainCfg.ChainID,
	)

	opt.Manager.RegisterCleanup(cleanupName, func(ctx context.Context) error {
		rpcClient.Close()
		return nil
	})

	source, err := evm.NewQuerySourceFromConfig(chainCfg.Source.ToEVMQuerySourceConfig())
	if err != nil {
		return fmt.Errorf("new query source: %w", err)
	}

	cursorStore, err := database.NewCursorStore(opt.DB)
	if err != nil {
		return fmt.Errorf("new cursor store: %w", err)
	}

	eventStoreConfig := chainCfg.Handler.ToEventStoreConfig()
	eventStoreConfig.OutboxBatchSize = opt.Config.Batch.OutboxBatchSize

	handler, err := buildEVMHandler(ctx, BuildEVMHandlerOptions{
		DB:    opt.DB,
		IDGen: opt.IDGen,

		HandlerConfig: chainCfg.Handler.ToPipelineHandlerConfig(),

		ComponentsConfig: chainCfg.Handler.ToHandlerComponentsConfig(),

		EventStoreConfig: eventStoreConfig,

		NotificationPlanner: notificationPlanner,
	})

	if err != nil {
		return fmt.Errorf("build evm handler: %w", err)
	}

	scanner, err := evm.NewScanner(
		chainCfg.ToEVMScannerConfig(),
		rpcClient,
		source,
		cursorStore,
		handler,
	)

	if err != nil {
		return fmt.Errorf("new scanner: %w", err)
	}

	goroutineName := fmt.Sprintf(
		"evm-scanner/%s/%s/%d",
		chainCfg.Network,
		chainCfg.Name,
		chainCfg.ChainID,
	)

	opt.Manager.Go(
		goroutineName,
		func(ctx context.Context) error {
			log.Printf("[evm-app] scanner started name=%s", goroutineName)
			err := scanner.Run(ctx)
			log.Printf("[evm-app] scanner stopped name=%s err=%v", goroutineName, err)
			return err
		},
	)
	return nil
}

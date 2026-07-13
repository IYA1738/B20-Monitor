package app

import (
	"context"
	"fmt"
	"log"
	"token-discover-demo/common/graceful"
	"token-discover-demo/common/idgen"
	"token-discover-demo/configs"
	"token-discover-demo/database"
)

func Run() error {
	cfg, err := configs.Load("")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	manager := graceful.New()

	db, err := database.Open(manager.Context(), cfg.DB.ToDatabaseConfig())
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	manager.RegisterCleanup("database", func(ctx context.Context) error {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	})

	if err := database.Migrate(manager.Context(), db); err != nil {
		return fmt.Errorf("migrate database: %w", err)
	}

	idGen, err := idgen.New(cfg.IDGen.ToIDGenConfig())
	if err != nil {
		return fmt.Errorf("new id generator: %w", err)
	}

	if err := runNotificationOutbox(manager.Context(), RunNotificationOutboxOptions{
		Manager: manager,
		DB:      db,
		Config:  cfg,
	}); err != nil {
		return fmt.Errorf("run notification outbox: %w", err)
	}

	if err := runEVMChains(manager.Context(), RunEVMChainsOptions{
		Manager: manager,
		DB:      db,
		IDGen:   idGen,
		Config:  cfg,
	}); err != nil {
		return fmt.Errorf("run evm chains: %w", err)
	}

	log.Printf("[app] started name=%s env=%s", cfg.App.Name, cfg.App.Env)

	manager.Wait()

	return nil

}

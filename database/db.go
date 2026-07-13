package database

import (
	"context"
	"fmt"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Config struct {
	DSN string

	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	PingTimeout time.Duration
	LogLevel    logger.LogLevel
}

func Open(ctx context.Context, cfg Config) (*gorm.DB, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("DSN is required")
	}

	applyDefault(&cfg)

	gdb, err := gorm.Open(postgres.Open(cfg.DSN), &gorm.Config{
		Logger: logger.Default.LogMode(cfg.LogLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("open gorm db: %w", err)
	}

	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql db from gorm: %w", err)
	}

	sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime) // 空闲连接最久可以连多久
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime) // 连接最久可以连多久
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)       // 最大有几个空闲连接
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)       // 最大开几个连接

	pingCtx, cancel := context.WithTimeout(ctx, cfg.PingTimeout)
	defer cancel()

	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return gdb, nil
}

func Close(gdb *gorm.DB) error {
	if gdb == nil {
		return nil
	}

	sqlDB, err := gdb.DB()
	if err != nil {
		return fmt.Errorf("get sql db from gorm: %w", err)
	}

	return sqlDB.Close()
}

func applyDefault(cfg *Config) {
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = 80
	}

	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 40
	}

	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = 30 * time.Minute
	}

	if cfg.ConnMaxIdleTime <= 0 {
		cfg.ConnMaxIdleTime = 5 * time.Minute
	}

	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = 5 * time.Second
	}

	if cfg.LogLevel == 0 {
		cfg.LogLevel = logger.Warn
	}
}

func ParseLogLevel(v string) logger.LogLevel {
	switch v {
	case "silent":
		return logger.Silent
	case "error":
		return logger.Error
	case "warn":
		return logger.Warn
	case "info":
		return logger.Info
	default:
		return logger.Warn
	}
}

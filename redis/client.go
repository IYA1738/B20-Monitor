package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Config struct {
	Addr     string
	Password string
	DB       int

	PoolSize     int
	MinIdleConns int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func Open(ctx context.Context, cfg Config) (*goredis.Client, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis addr is require")
	}

	applyDefault(&cfg)

	client := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,

		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,

		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return client, nil
}

func applyDefault(cfg *Config) {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 50
	}

	if cfg.MinIdleConns <= 0 {
		cfg.MinIdleConns = 10
	}

	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}

	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 3 * time.Second
	}

	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 3 * time.Second
	}
}

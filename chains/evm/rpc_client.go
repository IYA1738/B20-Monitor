package evm

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/time/rate"
)

type RPCConfig struct {
	URLs    []string
	Timeout time.Duration

	MaxConcurrentRequests int
	RequestsPerSecond     int

	RetryMax       int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

type RPCClient struct {
	endpoints []*rpcEndpoint

	nextEndpoint atomic.Uint64

	timeout time.Duration

	limiter *rate.Limiter
	sem     chan struct{}

	retryMax       int
	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration
}

type rpcEndpoint struct {
	url string
	eth *ethclient.Client
}

func applyRPCDefault(cfg *RPCConfig) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Second
	}

	if cfg.MaxConcurrentRequests <= 0 {
		cfg.MaxConcurrentRequests = 5
	}

	if cfg.RequestsPerSecond <= 0 {
		cfg.RequestsPerSecond = 5
	}

	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 5
	}

	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 100 * time.Millisecond
	}

	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = 500 * time.Millisecond
	}
}

func OpenRPCClient(ctx context.Context, cfg RPCConfig) (*RPCClient, error) {
	if len(cfg.URLs) == 0 {
		return nil, fmt.Errorf("URL is required")
	}

	applyRPCDefault(&cfg)

	endpoints := make([]*rpcEndpoint, 0, len(cfg.URLs))

	var lastErr error

	for _, url := range cfg.URLs {
		if url == "" {
			continue
		}

		dialCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		eth, err := ethclient.DialContext(dialCtx, url)
		cancel()

		if err != nil {
			lastErr = err
			continue
		}

		endpoints = append(endpoints, &rpcEndpoint{
			url: url,
			eth: eth,
		})
	}

	if len(endpoints) == 0 {
		if lastErr != nil {
			return nil, fmt.Errorf("dial evm rpc failed: %w", lastErr)
		}

		return nil, fmt.Errorf("dial evm rpc failed: no valid rpc url")
	}

	client := &RPCClient{
		endpoints: endpoints,
		timeout:   cfg.Timeout,

		limiter: rate.NewLimiter(
			rate.Limit(cfg.RequestsPerSecond),
			cfg.RequestsPerSecond,
		),
		sem: make(chan struct{}, cfg.MaxConcurrentRequests),

		retryMax:       cfg.RetryMax,
		retryBaseDelay: cfg.RetryBaseDelay,
		retryMaxDelay:  cfg.RetryMaxDelay,
	}

	return client, nil
}

func (c *RPCClient) Close() {
	if c == nil {
		return
	}

	for _, ep := range c.endpoints {
		if ep != nil && ep.eth != nil {
			ep.eth.Close()
		}
	}
}

func (c *RPCClient) BlockNumber(ctx context.Context) (uint64, error) {
	var out uint64

	err := c.do(ctx, func(ctx context.Context, ep *rpcEndpoint) error {
		blockNumber, err := ep.eth.BlockNumber(ctx)
		if err != nil {
			return err
		}

		out = blockNumber
		return nil
	})

	if err != nil {
		return 0, err
	}

	return out, nil
}

func (c *RPCClient) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	var out []types.Log

	err := c.do(ctx, func(ctx context.Context, ep *rpcEndpoint) error {
		logs, err := ep.eth.FilterLogs(ctx, query)
		if err != nil {
			return err
		}
		out = logs
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *RPCClient) TransactionSender(ctx context.Context, txHash common.Hash, chainID uint64) (common.Address, error) {
	if txHash == (common.Hash{}) {
		return common.Address{}, fmt.Errorf("tx hash is nil")
	}

	if chainID == 0 {
		return common.Address{}, fmt.Errorf("chain id is nil")
	}

	var out common.Address

	err := c.do(ctx, func(ctx context.Context, ep *rpcEndpoint) error {
		tx, _, err := ep.eth.TransactionByHash(ctx, txHash)
		if err != nil {
			return err
		}

		signer := types.LatestSignerForChainID(new(big.Int).SetUint64(chainID))

		from, err := types.Sender(signer, tx)
		if err != nil {
			return err
		}
		out = from
		return nil
	})

	if err != nil {
		return common.Address{}, fmt.Errorf("get tx sender tx=%s: %w", txHash.Hex(), err)
	}

	return out, nil
}

func (c *RPCClient) do(ctx context.Context, fn func(ctx context.Context, ep *rpcEndpoint) error) error {
	if c == nil {
		return fmt.Errorf("RPC Client cannot be nil")
	}

	if len(c.endpoints) <= 0 {
		return fmt.Errorf("endpoints is required")
	}

	var lastErr error

	for attempt := 0; attempt <= c.retryMax; attempt++ {
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}

		ep := c.pickEndpoint()

		err := c.callEndpoint(ctx, ep, fn)

		if err == nil {
			return nil
		}

		lastErr = err

		if attempt == c.retryMax {
			break
		}

		delay := c.retryDelay(attempt)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("evm rpc request failed after retries: %w", lastErr)
}

func (c *RPCClient) retryDelay(attempt int) time.Duration {
	delay := c.retryBaseDelay

	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= c.retryMaxDelay {
			return c.retryMaxDelay
		}
	}
	if delay > c.retryMaxDelay {
		return c.retryMaxDelay
	}

	return delay
}

func (c *RPCClient) callEndpoint(ctx context.Context, ep *rpcEndpoint, fn func(context.Context, *rpcEndpoint) error) error {
	if ep == nil || ep.eth == nil {
		return fmt.Errorf("evm rpc endpoint is nil")
	}

	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return fn(reqCtx, ep)
}

func (c *RPCClient) release() {
	<-c.sem
}

func (c *RPCClient) acquire(ctx context.Context) error {
	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *RPCClient) pickEndpoint() *rpcEndpoint {
	idx := c.nextEndpoint.Add(1) - 1
	return c.endpoints[int(idx)%len(c.endpoints)]
}

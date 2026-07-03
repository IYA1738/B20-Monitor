package elasticsearch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	esv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type Config struct {
	Addresses []string

	Username string
	Password string

	RequestTimeout time.Duration

	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
}

type Client struct {
	raw       *esv8.Client
	transport *http.Transport
}

func Open(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Addresses) == 0 {
		return nil, fmt.Errorf("elasticsearch addresses is required")
	}

	applyDefault(&cfg)

	transport := &http.Transport{
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:     cfg.IdleConnTimeout,

		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: cfg.RequestTimeout,
		ExpectContinueTimeout: time.Second,
	}

	raw, err := esv8.NewClient(esv8.Config{
		Addresses: cfg.Addresses,
		Username:  cfg.Username,
		Password:  cfg.Password,
		Transport: transport,
	})

	if err != nil {
		return nil, fmt.Errorf("create elasticsearch client: %w", err)
	}

	client := &Client{
		raw:       raw,
		transport: transport,
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	if err := client.Ping(pingCtx); err != nil {
		client.Close()
		return nil, err
	}

	return client, nil
}

func (c *Client) Raw() *esv8.Client {
	if c == nil {
		return nil
	}

	return c.raw
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.raw == nil {
		return fmt.Errorf("es client is nil")
	}

	req := esapi.InfoRequest{}

	res, err := req.Do(ctx, c.raw)
	if err != nil {
		return fmt.Errorf("ping ES: %w", err)
	}

	defer closeResponse(res)

	if res.IsError() {
		return fmt.Errorf("ping es failed: %s", res.String())
	}
	return nil
}

func closeResponse(res *esapi.Response) {
	if res == nil || res.Body == nil {
		return
	}

	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
}

func (c *Client) Close() {
	if c == nil {
		return
	}

	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
}

func applyDefault(cfg *Config) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}

	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 100
	}

	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = 50
	}

	if cfg.IdleConnTimeout <= 0 {
		cfg.IdleConnTimeout = 90 * time.Second
	}
}

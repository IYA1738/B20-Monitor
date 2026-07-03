package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

var (
	ErrLockNotAcquired = errors.New("redis lock not acquire")
	ErrLockNotOwned    = errors.New("redis lock not owned")
)

const (
	defaultLockTTL           = 30 * time.Second
	defaultLockRetryInterval = 200 * time.Millisecond
)

const unlockScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`
const renewScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`

type LockManager struct {
	client    goredis.UniversalClient
	owner     string
	keyPrefix string
}

type LockOptions struct {
	TTL           time.Duration
	Timeout       time.Duration
	RetryInterval time.Duration
}

type Lock struct {
	manager *LockManager

	Key   string
	Token string
	TTL   time.Duration
}

func NewLockManager(client goredis.UniversalClient, owner string, keyPrefix string) (*LockManager, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client is nil")
	}

	if owner == "" {
		return nil, fmt.Errorf("redis lock owner is required")
	}

	return &LockManager{
		client:    client,
		owner:     owner,
		keyPrefix: keyPrefix,
	}, nil
}

func (m *LockManager) TryLock(ctx context.Context, key string, ttl time.Duration) (*Lock, bool, error) {
	if key == "" {
		return nil, false, fmt.Errorf("lock key is required")
	}

	if ttl <= 0 {
		ttl = defaultLockTTL
	}

	fullKey := m.fullKey(key)

	token, err := m.newToken()
	if err != nil {
		return nil, false, err
	}

	ok, err := m.client.SetNX(ctx, fullKey, token, ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("redis setnx lock key=%s: %w", fullKey, err)
	}

	if !ok {
		return nil, false, nil
	}

	return &Lock{
		manager: m,
		Key:     fullKey,
		Token:   token,
		TTL:     ttl,
	}, true, nil
}

func (m *LockManager) fullKey(key string) string {
	if m.keyPrefix == "" {
		return key
	}
	return m.keyPrefix + ":" + key
}

func (m *LockManager) Lock(ctx context.Context, key string, opts LockOptions) (*Lock, error) {
	if opts.TTL <= 0 {
		opts.TTL = defaultLockTTL
	}

	if opts.RetryInterval <= 0 {
		opts.RetryInterval = defaultLockRetryInterval
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	for {
		lock, ok, err := m.TryLock(ctx, key, opts.TTL)
		if err != nil {
			return nil, err
		}

		if ok {
			return lock, nil
		}

		timer := time.NewTimer(opts.RetryInterval)

		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w: key=%s", ErrLockNotAcquired, key)
		case <-timer.C:
		}
	}
}

func (l *Lock) Unlock(ctx context.Context) error {
	if l == nil {
		return nil
	}

	result, err := l.manager.client.Eval(
		ctx,
		unlockScript,
		[]string{l.Key},
		l.Token,
	).Int64()
	if err != nil {
		return fmt.Errorf("redis unlock key=%s: %w", l.Key, err)
	}

	if result == 0 {
		return fmt.Errorf("%w: key=%s", ErrLockNotOwned, l.Key)
	}
	return nil
}

func (l *Lock) Renew(ctx context.Context, ttl time.Duration) error {
	if l == nil {
		return fmt.Errorf("Lock is nil")
	}

	if ttl <= 0 {
		ttl = l.TTL
	}

	ttlMillis := ttl.Milliseconds()
	if ttlMillis <= 0 {
		return fmt.Errorf("invalid lock ttl: %s", ttl)
	}

	result, err := l.manager.client.Eval(
		ctx,
		renewScript,
		[]string{l.Key},
		l.Token,
		ttlMillis,
	).Int64()

	if err != nil {
		return fmt.Errorf("redis renew lock key=%s: %w", l.Key, err)
	}

	if result == 0 {
		return fmt.Errorf("%w: key=%s", ErrLockNotOwned, l.Key)
	}

	l.TTL = ttl

	return nil
}

func (m *LockManager) newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate lock token: %w", err)
	}
	return m.owner + ":" + hex.EncodeToString(b[:]), nil
}

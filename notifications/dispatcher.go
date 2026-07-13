package notifications

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"token-discover-demo/database"
)

type Sender interface {
	Channel() string
	Send(ctx context.Context, task database.NotificationOutbox) error
}

type retryAfterError interface {
	RetryAfterDelay() time.Duration
}

type DispatcherConfig struct {
	BatchSize int

	PollInterval time.Duration

	LockDuration time.Duration

	SendTimeout time.Duration

	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

type Dispatcher struct {
	store *database.NotificationOutboxStore

	cfg DispatcherConfig

	senders map[string]Sender
}

func NewDispatcher(store *database.NotificationOutboxStore, cfg DispatcherConfig, senders ...Sender) (*Dispatcher, error) {
	if store == nil {
		return nil, fmt.Errorf("notification outbox store is nil")
	}

	applyDispatcherDefaults(&cfg)

	senderMap := make(map[string]Sender, len(senders))
	for _, sender := range senders {
		if sender == nil {
			continue
		}

		channel := strings.TrimSpace(sender.Channel())
		if channel == "" {
			return nil, fmt.Errorf("notification sender channel is empty")
		}

		if _, exists := senderMap[channel]; exists {
			return nil, fmt.Errorf("duplicate notification sender channel=%s", channel)
		}

		senderMap[channel] = sender
	}

	if len(senderMap) == 0 {
		return nil, fmt.Errorf("no notification senders configured")
	}

	return &Dispatcher{
		store:   store,
		cfg:     cfg,
		senders: senderMap,
	}, nil
}

func applyDispatcherDefaults(cfg *DispatcherConfig) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}

	if cfg.SendTimeout <= 0 {
		cfg.SendTimeout = 10 * time.Second
	}

	if cfg.LockDuration <= 0 {
		cfg.LockDuration = cfg.SendTimeout * 3
	}

	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = time.Second
	}

	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = time.Minute
	}
}

func (d *Dispatcher) SupportedChannels() []string {
	if d == nil || len(d.senders) == 0 {
		return nil
	}

	channels := make([]string, 0, len(d.senders))
	for channel := range d.senders {
		channels = append(channels, channel)
	}

	sort.Strings(channels)
	return channels
}

func (d *Dispatcher) Run(ctx context.Context, workerID string) error {
	if d == nil {
		return fmt.Errorf("notification dispatcher is nil")
	}

	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return fmt.Errorf("notification worker id is required")
	}

	channels := d.SupportedChannels()
	log.Printf("[notification-dispatcher] worker started worker=%s channels=%v", workerID, channels)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rows, err := d.store.ClaimDue(ctx, database.ClaimNotificationOutboxOptions{
			WorkerID:     workerID,
			Channels:     channels,
			Limit:        d.cfg.BatchSize,
			LockDuration: d.cfg.LockDuration,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			log.Printf("[notification-dispatcher] claim failed worker=%s err=%v", workerID, err)
			if err := sleepContext(ctx, d.cfg.PollInterval); err != nil {
				return err
			}
			continue
		}

		if len(rows) == 0 {
			if err := sleepContext(ctx, d.cfg.PollInterval); err != nil {
				return err
			}
			continue
		}

		for _, row := range rows {
			if err := d.sendOne(ctx, row); err != nil {
				log.Printf(
					"[notification-dispatcher] send handled with error worker=%s id=%d channel=%s target=%s event_key=%s err=%v",
					workerID,
					row.ID,
					row.Channel,
					row.Target,
					row.EventKey,
					err,
				)
			}
		}
	}
}

func (d *Dispatcher) sendOne(ctx context.Context, row database.NotificationOutbox) error {
	sender := d.senders[row.Channel]
	if sender == nil {
		err := fmt.Errorf("notification sender not found channel=%s", row.Channel)
		if markErr := d.store.MarkFailed(ctx, row, err, d.retryConfig()); markErr != nil {
			return fmt.Errorf("%w; mark failed: %v", err, markErr)
		}
		return err
	}

	sendCtx, cancel := context.WithTimeout(ctx, d.cfg.SendTimeout)
	defer cancel()

	err := sender.Send(sendCtx, row)
	if err != nil {
		retryAfter := retryAfterDelay(err)
		retryCfg := d.retryConfig()
		retryCfg.RetryAfter = retryAfter + time.Second

		if markErr := d.store.MarkFailed(ctx, row, err, retryCfg); markErr != nil {
			return fmt.Errorf("send failed: %w; mark failed: %v", err, markErr)
		}

		if retryAfter > 0 {
			_ = sleepContext(ctx, retryAfter+time.Second)
		}

		return err
	}

	if err := d.store.MarkSent(ctx, row); err != nil {
		return err
	}

	log.Printf(
		"[notification-dispatcher] sent id=%d channel=%s target=%s event_key=%s",
		row.ID,
		row.Channel,
		row.Target,
		row.EventKey,
	)

	return nil
}

func (d *Dispatcher) retryConfig() database.NotificationRetryConfig {
	return database.NotificationRetryConfig{
		BaseDelay: d.cfg.RetryBaseDelay,
		MaxDelay:  d.cfg.RetryMaxDelay,
	}
}

func retryAfterDelay(err error) time.Duration {
	if err == nil {
		return 0
	}

	var retryErr retryAfterError
	if errors.As(err, &retryErr) {
		return retryErr.RetryAfterDelay()
	}

	return 0
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

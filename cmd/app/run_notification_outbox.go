package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"token-discover-demo/common/graceful"
	"token-discover-demo/configs"
	"token-discover-demo/database"
	"token-discover-demo/notifications"
	"token-discover-demo/telegram"

	"gorm.io/gorm"
)

type RunNotificationOutboxOptions struct {
	Manager *graceful.Manager

	DB *gorm.DB

	Config *configs.Config
}

func runNotificationOutbox(ctx context.Context, opt RunNotificationOutboxOptions) error {
	if opt.Manager == nil {
		return fmt.Errorf("graceful manager is nil")
	}

	if opt.DB == nil {
		return fmt.Errorf("db is nil")
	}

	if opt.Config == nil {
		return fmt.Errorf("config is nil")
	}

	senders, err := buildNotificationSenders(opt.Config)
	if err != nil {
		return err
	}

	if len(senders) == 0 {
		log.Printf("[notification-app] no notification senders enabled")
		return nil
	}

	store, err := database.NewNotificationOutboxStore(opt.DB)
	if err != nil {
		return fmt.Errorf("new notification outbox store: %w", err)
	}

	dispatcher, err := notifications.NewDispatcher(
		store,
		notifications.DispatcherConfig{
			BatchSize:      notificationBatchSize(opt.Config, len(senders)),
			PollInterval:   opt.Config.Batch.OutboxFlushInterval(),
			LockDuration:   notificationLockDuration(opt.Config),
			SendTimeout:    notificationSendTimeout(opt.Config),
			RetryBaseDelay: opt.Config.Batch.OutboxFlushInterval(),
			RetryMaxDelay:  time.Minute,
		},
		senders...,
	)
	if err != nil {
		return fmt.Errorf("new notification dispatcher: %w", err)
	}

	workerCount := notificationWorkerCount(opt.Config, len(senders))
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "local"
	}

	log.Printf(
		"[notification-app] starting workers=%d channels=%v",
		workerCount,
		dispatcher.SupportedChannels(),
	)

	for i := 0; i < workerCount; i++ {
		workerIndex := i
		workerID := fmt.Sprintf("%s-notify-%d", hostname, workerIndex)
		goroutineName := fmt.Sprintf("notification-outbox/%d", workerIndex)

		opt.Manager.Go(goroutineName, func(ctx context.Context) error {
			return dispatcher.Run(ctx, workerID)
		})
	}

	return nil
}

func buildNotificationSenders(cfg *configs.Config) ([]notifications.Sender, error) {
	senders := make([]notifications.Sender, 0, 1)

	if cfg.Notifier.Telegram.Enabled {
		client, err := telegram.NewClient(
			cfg.Notifier.Telegram.BotToken,
			cfg.Notifier.Telegram.RequestTimeout(),
		)
		if err != nil {
			return nil, fmt.Errorf("new telegram client: %w", err)
		}

		sender, err := notifications.NewTelegramSender(client)
		if err != nil {
			return nil, fmt.Errorf("new telegram sender: %w", err)
		}

		senders = append(senders, sender)
	}

	return senders, nil
}

func notificationWorkerCount(cfg *configs.Config, senderCount int) int {
	workerCount := cfg.Workers.NotifyWorkers
	if workerCount <= 0 {
		workerCount = 1
	}

	if senderCount == 1 && cfg.Notifier.Telegram.Enabled && cfg.Notifier.Telegram.MaxConcurrentSends > 0 {
		if workerCount > cfg.Notifier.Telegram.MaxConcurrentSends {
			workerCount = cfg.Notifier.Telegram.MaxConcurrentSends
		}
	}

	if workerCount <= 0 {
		workerCount = 1
	}

	return workerCount
}

func notificationBatchSize(cfg *configs.Config, senderCount int) int {
	if senderCount == 1 && cfg.Notifier.Telegram.Enabled {
		return 1
	}

	if cfg.Batch.OutboxBatchSize <= 0 {
		return 100
	}

	return cfg.Batch.OutboxBatchSize
}

func notificationSendTimeout(cfg *configs.Config) time.Duration {
	if cfg.Notifier.Telegram.Enabled {
		return cfg.Notifier.Telegram.RequestTimeout()
	}

	return 10 * time.Second
}

func notificationLockDuration(cfg *configs.Config) time.Duration {
	sendTimeout := notificationSendTimeout(cfg)
	if sendTimeout <= 0 {
		sendTimeout = 10 * time.Second
	}

	return sendTimeout * 3
}

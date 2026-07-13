package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type NotificationOutboxStore struct {
	db *gorm.DB
}

func NewNotificationOutboxStore(db *gorm.DB) (*NotificationOutboxStore, error) {
	if db == nil {
		return nil, fmt.Errorf("notification outbox store db is nil")
	}

	return &NotificationOutboxStore{
		db: db,
	}, nil
}

type ClaimNotificationOutboxOptions struct {
	WorkerID string

	Channels []string

	Limit int

	LockDuration time.Duration

	Now time.Time
}

func (s *NotificationOutboxStore) ClaimDue(ctx context.Context, opt ClaimNotificationOutboxOptions) ([]NotificationOutbox, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("notification outbox store is nil")
	}

	opt.WorkerID = strings.TrimSpace(opt.WorkerID)
	if opt.WorkerID == "" {
		return nil, fmt.Errorf("notification outbox worker id is required")
	}

	if opt.Limit <= 0 {
		opt.Limit = 100
	}

	if opt.LockDuration <= 0 {
		opt.LockDuration = 30 * time.Second
	}

	now := opt.Now
	if now.IsZero() {
		now = time.Now()
	}

	channels := normalizeChannels(opt.Channels)

	var rows []NotificationOutbox
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.
			Clauses(clause.Locking{
				Strength: "UPDATE",
				Options:  "SKIP LOCKED",
			}).
			Where("next_retry_at <= ?", now).
			Where(
				"(status IN ? OR (status = ? AND (locked_until IS NULL OR locked_until < ?)))",
				[]string{OutboxStatusPending, OutboxStatusFailed},
				OutboxStatusProcessing,
				now,
			)

		if len(channels) > 0 {
			query = query.Where("channel IN ?", channels)
		}

		if err := query.
			Order("next_retry_at ASC, id ASC").
			Limit(opt.Limit).
			Find(&rows).
			Error; err != nil {
			return fmt.Errorf("claim notification outbox rows: %w", err)
		}

		if len(rows) == 0 {
			return nil
		}

		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}

		lockUntil := now.Add(opt.LockDuration)
		if err := tx.Model(&NotificationOutbox{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"status":       OutboxStatusProcessing,
				"locked_by":    opt.WorkerID,
				"locked_until": lockUntil,
				"updated_at":   now,
			}).
			Error; err != nil {
			return fmt.Errorf("lock notification outbox rows count=%d: %w", len(rows), err)
		}

		for i := range rows {
			rows[i].Status = OutboxStatusProcessing
			rows[i].LockedBy = opt.WorkerID
			rows[i].LockedUntil = &lockUntil
			rows[i].UpdatedAt = now
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return rows, nil
}

type NotificationRetryConfig struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	RetryAfter time.Duration
}

func (s *NotificationOutboxStore) MarkSent(ctx context.Context, row NotificationOutbox) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("notification outbox store is nil")
	}

	if row.ID == 0 {
		return fmt.Errorf("notification outbox id is zero")
	}

	now := time.Now()
	query := s.db.WithContext(ctx).
		Model(&NotificationOutbox{}).
		Where("id = ?", row.ID)

	if row.LockedBy != "" {
		query = query.Where("locked_by = ?", row.LockedBy)
	}

	if err := query.Updates(map[string]any{
		"status":       OutboxStatusSent,
		"locked_by":    "",
		"locked_until": nil,
		"last_error":   "",
		"sent_at":      now,
		"updated_at":   now,
	}).Error; err != nil {
		return fmt.Errorf("mark notification outbox sent id=%d: %w", row.ID, err)
	}

	return nil
}

func (s *NotificationOutboxStore) MarkFailed(ctx context.Context, row NotificationOutbox, sendErr error, retryCfg NotificationRetryConfig) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("notification outbox store is nil")
	}

	if row.ID == 0 {
		return fmt.Errorf("notification outbox id is zero")
	}

	if retryCfg.BaseDelay <= 0 {
		retryCfg.BaseDelay = time.Second
	}

	if retryCfg.MaxDelay <= 0 {
		retryCfg.MaxDelay = time.Minute
	}

	now := time.Now()
	retryCount := row.RetryCount + 1
	maxRetries := row.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 10
	}

	status := OutboxStatusFailed
	delay := retryDelay(retryCount, retryCfg)
	if retryCfg.RetryAfter > delay {
		delay = retryCfg.RetryAfter
	}

	nextRetryAt := now.Add(delay)
	if retryCount >= maxRetries {
		status = OutboxStatusDead
		nextRetryAt = now
	}

	lastError := ""
	if sendErr != nil {
		lastError = truncateString(sendErr.Error(), 4000)
	}

	query := s.db.WithContext(ctx).
		Model(&NotificationOutbox{}).
		Where("id = ?", row.ID)

	if row.LockedBy != "" {
		query = query.Where("locked_by = ?", row.LockedBy)
	}

	if err := query.Updates(map[string]any{
		"status":        status,
		"retry_count":   retryCount,
		"next_retry_at": nextRetryAt,
		"locked_by":     "",
		"locked_until":  nil,
		"last_error":    lastError,
		"updated_at":    now,
	}).Error; err != nil {
		return fmt.Errorf("mark notification outbox failed id=%d status=%s: %w", row.ID, status, err)
	}

	return nil
}

func retryDelay(retryCount int, cfg NotificationRetryConfig) time.Duration {
	if retryCount <= 1 {
		return cfg.BaseDelay
	}

	shift := retryCount - 1
	if shift > 10 {
		shift = 10
	}

	delay := cfg.BaseDelay * time.Duration(1<<shift)
	if delay > cfg.MaxDelay {
		return cfg.MaxDelay
	}

	return delay
}

func normalizeChannels(channels []string) []string {
	if len(channels) == 0 {
		return nil
	}

	out := make([]string, 0, len(channels))
	seen := make(map[string]struct{}, len(channels))

	for _, channel := range channels {
		channel = strings.TrimSpace(channel)
		if channel == "" {
			continue
		}

		if _, ok := seen[channel]; ok {
			continue
		}

		seen[channel] = struct{}{}
		out = append(out, channel)
	}

	return out
}

func truncateString(v string, max int) string {
	if max <= 0 || len(v) <= max {
		return v
	}

	return v[:max]
}

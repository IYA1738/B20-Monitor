package database

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"token-discover-demo/chains/evm"

	"github.com/ethereum/go-ethereum/common"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type IDGenerator interface {
	NextID(ctx context.Context) (int64, error)
}
type EventStoreConfig struct {
	// 每次累积多少事件时一次写入DB
	BatchSize int

	// 每次累积多少通知 outbox 时一次写入DB
	OutboxBatchSize int
}

// 只负责把event写到chain_events表
type EventStore struct {
	db *gorm.DB

	idGen IDGenerator

	batchSize int

	outboxBatchSize int

	notificationPlanner NotificationRoutePlanner
}

type EventStoreOption func(*EventStore)

func WithNotificationRoutePlanner(planner NotificationRoutePlanner) EventStoreOption {
	return func(store *EventStore) {
		store.notificationPlanner = planner
	}
}

func NewEventStore(db *gorm.DB, idGen IDGenerator, cfg EventStoreConfig, opts ...EventStoreOption) (*EventStore, error) {
	if db == nil {
		return nil, fmt.Errorf("gorm db is nil")
	}

	if idGen == nil {
		return nil, fmt.Errorf("id generator is nil")
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500 // 先大概给个500试试看
	}

	if cfg.OutboxBatchSize <= 0 {
		cfg.OutboxBatchSize = 200
	}

	store := &EventStore{
		db:              db,
		idGen:           idGen,
		batchSize:       cfg.BatchSize,
		outboxBatchSize: cfg.OutboxBatchSize,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}

	return store, nil
}

func (s *EventStore) SaveEvents(ctx context.Context, events []evm.EventEnvelope) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("event store db is nil")
	}

	if s.idGen == nil {
		return fmt.Errorf("event store id generator is nil")
	}

	if len(events) == 0 {
		return nil
	}

	// 先给内存里的事件去重
	events = dedupeEvents(events)

	if len(events) == 0 {
		return nil
	}

	rows := make([]ChainEvent, 0, len(events))

	for i := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		id, err := s.idGen.NextID(ctx)
		if err != nil {
			return fmt.Errorf("generate event id event_key=%s: %w", events[i].EventKey, err)
		}

		row, err := eventEnvelopeToRow(id, events[i])
		if err != nil {
			return fmt.Errorf("convert event[%d] event_key=%s: %w", i, events[i].EventKey, err)
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return nil
	}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			DoNothing: true,
		}).CreateInBatches(rows, s.batchSize).Error; err != nil {
			return fmt.Errorf("batch insert chain_events count=%d: %w", len(rows), err)
		}

		if s.notificationPlanner == nil {
			return nil
		}

		if err := s.createNotificationOutbox(ctx, tx, events); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *EventStore) createNotificationOutbox(ctx context.Context, tx *gorm.DB, events []evm.EventEnvelope) error {
	if len(events) == 0 {
		return nil
	}

	keys := make([]string, 0, len(events))
	for _, event := range events {
		if event.EventKey != "" {
			keys = append(keys, event.EventKey)
		}
	}

	if len(keys) == 0 {
		return nil
	}

	var persistedEvents []ChainEvent
	if err := tx.WithContext(ctx).
		Where("event_key IN ?", keys).
		Find(&persistedEvents).
		Error; err != nil {
		return fmt.Errorf("load persisted events for notification outbox: %w", err)
	}

	eventRowsByKey := make(map[string]ChainEvent, len(persistedEvents))
	for _, row := range persistedEvents {
		eventRowsByKey[row.EventKey] = row
	}

	now := time.Now()
	outboxRows := make([]NotificationOutbox, 0, len(events))

	for _, event := range events {
		eventRow, ok := eventRowsByKey[event.EventKey]
		if !ok {
			return fmt.Errorf("persisted event not found event_key=%s", event.EventKey)
		}

		routes, err := s.notificationPlanner.RoutesForEvent(ctx, event)
		if err != nil {
			return fmt.Errorf("plan notification routes event_key=%s: %w", event.EventKey, err)
		}

		for _, route := range routes {
			route = normalizeNotificationRoute(route, event)
			if route.Channel == "" {
				return fmt.Errorf("notification route channel is empty event_key=%s", event.EventKey)
			}
			if route.Target == "" {
				return fmt.Errorf("notification route target is empty event_key=%s channel=%s", event.EventKey, route.Channel)
			}
			if route.MessageType == "" {
				return fmt.Errorf("notification route message type is empty event_key=%s channel=%s", event.EventKey, route.Channel)
			}

			id, err := s.idGen.NextID(ctx)
			if err != nil {
				return fmt.Errorf("generate notification outbox id event_key=%s channel=%s: %w", event.EventKey, route.Channel, err)
			}

			payload, err := buildNotificationPayload(eventRow, event)
			if err != nil {
				return fmt.Errorf("build notification payload event_key=%s channel=%s: %w", event.EventKey, route.Channel, err)
			}

			outboxRows = append(outboxRows, NotificationOutbox{
				ID:          id,
				EventID:     eventRow.ID,
				EventKey:    event.EventKey,
				Channel:     route.Channel,
				Target:      route.Target,
				MessageType: route.MessageType,
				Payload:     payload,
				Status:      OutboxStatusPending,
				RetryCount:  0,
				MaxRetries:  route.MaxRetries,
				NextRetryAt: now,
				CreatedAt:   now,
				UpdatedAt:   now,
			})
		}
	}

	if len(outboxRows) == 0 {
		return nil
	}

	if err := tx.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "event_key"},
				{Name: "channel"},
				{Name: "target"},
				{Name: "message_type"},
			},
			DoNothing: true,
		}).
		CreateInBatches(outboxRows, s.outboxBatchSize).
		Error; err != nil {
		return fmt.Errorf("batch insert notification outbox count=%d: %w", len(outboxRows), err)
	}

	return nil
}

type NotificationRoute struct {
	// Channel 例如 telegram / webhook / email / slack。
	Channel string

	// Target 例如 chat_id / webhook_url / email address。
	Target string

	MessageType string

	MaxRetries int
}

type NotificationRoutePlanner interface {
	RoutesForEvent(ctx context.Context, event evm.EventEnvelope) ([]NotificationRoute, error)
}

type NotificationPayload struct {
	Version int                      `json:"version"`
	Event   NotificationEventPayload `json:"event"`
}

type NotificationEventPayload struct {
	EventID         int64           `json:"event_id"`
	EventKey        string          `json:"event_key"`
	ChainID         uint64          `json:"chain_id"`
	ChainName       string          `json:"chain_name"`
	Network         string          `json:"network"`
	Monitor         string          `json:"monitor"`
	EventName       string          `json:"event_name"`
	BlockNumber     uint64          `json:"block_number"`
	BlockHash       string          `json:"block_hash"`
	TxHash          string          `json:"tx_hash"`
	TxIndex         uint            `json:"tx_index"`
	LogIndex        uint            `json:"log_index"`
	ContractAddress string          `json:"contract_address"`
	Topic0          string          `json:"topic0"`
	Payload         json.RawMessage `json:"payload"`
	ObservedAt      time.Time       `json:"observed_at"`
}

func DecodeNotificationPayload(raw datatypes.JSON) (NotificationPayload, error) {
	if len(raw) == 0 {
		return NotificationPayload{}, fmt.Errorf("notification payload is empty")
	}

	var payload NotificationPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return NotificationPayload{}, fmt.Errorf("decode notification payload: %w", err)
	}

	if payload.Event.EventKey == "" {
		return NotificationPayload{}, fmt.Errorf("notification payload event_key is empty")
	}

	return payload, nil
}

func buildNotificationPayload(eventRow ChainEvent, event evm.EventEnvelope) (datatypes.JSON, error) {
	if eventRow.ID == 0 {
		return nil, fmt.Errorf("event id is zero")
	}

	observedAt := event.ObservedAt
	if observedAt.IsZero() {
		observedAt = eventRow.ObservedAt
	}

	payload := NotificationPayload{
		Version: 1,
		Event: NotificationEventPayload{
			EventID:         eventRow.ID,
			EventKey:        event.EventKey,
			ChainID:         event.ChainID,
			ChainName:       event.ChainName,
			Network:         event.Network,
			Monitor:         event.Monitor,
			EventName:       event.EventName,
			BlockNumber:     event.BlockNumber,
			BlockHash:       event.BlockHash.Hex(),
			TxHash:          event.TxHash.Hex(),
			TxIndex:         event.TxIndex,
			LogIndex:        event.LogIndex,
			ContractAddress: event.ContractAddress.Hex(),
			Topic0:          event.Topic0.Hex(),
			Payload:         event.Payload,
			ObservedAt:      observedAt,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal notification payload: %w", err)
	}

	return datatypes.JSON(body), nil
}

func normalizeNotificationRoute(route NotificationRoute, event evm.EventEnvelope) NotificationRoute {
	route.Channel = strings.TrimSpace(route.Channel)
	route.Target = strings.TrimSpace(route.Target)
	route.MessageType = strings.TrimSpace(route.MessageType)

	if route.MessageType == "" {
		route.MessageType = event.EventName
	}

	if route.MaxRetries <= 0 {
		route.MaxRetries = 10
	}

	return route
}

func eventEnvelopeToRow(id int64, event evm.EventEnvelope) (ChainEvent, error) {
	err := eventEnvelopeToRowValid(id, event)

	if err != nil {
		return ChainEvent{}, err
	}

	observedAt := event.ObservedAt
	if observedAt.IsZero() {
		observedAt = time.Now()
	}

	now := time.Now()

	return ChainEvent{
		ID: id,

		ChainID:   event.ChainID,
		ChainName: event.ChainName,
		Network:   event.Network,

		Monitor:   event.Monitor,
		EventName: event.EventName,

		BlockNumber: event.BlockNumber,
		BlockHash:   event.BlockHash.Hex(),

		TxHash:   event.TxHash.Hex(),
		TxIndex:  event.TxIndex,
		LogIndex: event.LogIndex,

		ContractAddress: event.ContractAddress.Hex(),
		Topic0:          event.Topic0.Hex(),

		EventKey: event.EventKey,

		Payload: datatypes.JSON(event.Payload),
		RawLog:  datatypes.JSON(event.RawLog),

		Removed: event.Removed,

		ObservedAt: observedAt,

		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func eventEnvelopeToRowValid(id int64, event evm.EventEnvelope) error {
	if id == 0 {
		return fmt.Errorf("id is zero")
	}

	if event.EventKey == "" {
		return fmt.Errorf("event key is empty")
	}

	if event.ChainID == 0 {
		return fmt.Errorf("chain id is zero")
	}

	if event.ChainName == "" {
		return fmt.Errorf("chain name is empty")
	}

	if event.Network == "" {
		return fmt.Errorf("network is empty")
	}

	if event.Monitor == "" {
		return fmt.Errorf("monitor is empty")
	}

	if event.EventName == "" {
		return fmt.Errorf("event name is empty")
	}

	if event.TxHash == (common.Hash{}) {
		return fmt.Errorf("tx hash is empty")
	}

	if event.ContractAddress == (common.Address{}) {
		return fmt.Errorf("contract address is empty")
	}

	if len(event.Payload) == 0 {
		return fmt.Errorf("payload is empty")
	}

	if !json.Valid(event.Payload) {
		return fmt.Errorf("payload is not valid json")
	}

	if len(event.RawLog) == 0 {
		return fmt.Errorf("raw log is empty")
	}

	if !json.Valid(event.RawLog) {
		return fmt.Errorf("raw log is not valid json")
	}

	return nil
}

func dedupeEvents(events []evm.EventEnvelope) []evm.EventEnvelope {
	if len(events) == 1 {
		return events
	}

	seen := make(map[string]struct{}, len(events))
	out := make([]evm.EventEnvelope, 0, len(events))

	for _, event := range events {
		if event.EventKey == "" {
			// 空 EventKey 后面 eventEnvelopeToRow 会报错。
			// 这里不丢，让错误在转换阶段带上下文返回。
			out = append(out, event)
			continue
		}

		if _, ok := seen[event.EventKey]; ok {
			continue
		}
		seen[event.EventKey] = struct{}{}
		out = append(out, event)
	}

	return out
}

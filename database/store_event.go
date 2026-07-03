package database

import (
	"context"
	"encoding/json"
	"fmt"
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
}

// 只负责把event写到chain_events表
type EventStore struct {
	db *gorm.DB

	idGen IDGenerator

	batchSize int
}

func NewEventStore(db *gorm.DB, idGen IDGenerator, cfg EventStoreConfig) (*EventStore, error) {
	if db == nil {
		return nil, fmt.Errorf("gorm db is nil")
	}

	if idGen == nil {
		return nil, fmt.Errorf("id generator is nil")
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500 // 先大概给个500试试看
	}

	return &EventStore{
		db:        db,
		idGen:     idGen,
		batchSize: cfg.BatchSize,
	}, nil
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

	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		DoNothing: true,
	}).CreateInBatches(rows, s.batchSize).Error

	if err != nil {
		return fmt.Errorf("batch insert chain_events count=%d: %w", len(rows), err)
	}

	return nil
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

package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"token-discover-demo/chains/evm"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type CursorStore struct {
	db *gorm.DB
}

func NewCursorStore(db *gorm.DB) (*CursorStore, error) {
	if db == nil {
		return nil, fmt.Errorf("cursor store db is nil")
	}

	return &CursorStore{
		db: db,
	}, nil
}

func (s *CursorStore) GetCursor(ctx context.Context, key string) (*evm.Cursor, bool, error) {
	if s == nil || s.db == nil {
		return nil, false, fmt.Errorf("cursor store is nil")
	}

	if key == "" {
		return nil, false, fmt.Errorf("cursor key is required")
	}

	var row IndexerCursor

	err := s.db.WithContext(ctx).
		Where("key = ?", key).
		First(&row).
		Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("get cursor key=%s: %w", key, err)
	}

	cursor := &evm.Cursor{
		Key:         row.Key,
		ChainID:     row.ChainID,
		ChainName:   row.ChainName,
		Network:     row.Network,
		Monitor:     row.Monitor,
		NextBlock:   row.NextBlock,
		SafeBlock:   row.SafeBlock,
		LatestBlock: row.LatestBlock,
	}

	return cursor, true, nil
}

func (s *CursorStore) SaveCursor(ctx context.Context, cursor evm.Cursor) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("cursor store is nil")
	}

	if cursor.Key == "" {
		return fmt.Errorf("cursor key is required")
	}

	if cursor.ChainID == 0 {
		return fmt.Errorf("cursor chain id is required")
	}

	if cursor.ChainName == "" {
		return fmt.Errorf("cursor chain name is required")
	}

	if cursor.Network == "" {
		return fmt.Errorf("cursor network is required")
	}

	if cursor.Monitor == "" {
		return fmt.Errorf("cursor monitor is required")
	}

	now := time.Now()

	row := IndexerCursor{
		Key:         cursor.Key,
		ChainID:     cursor.ChainID,
		ChainName:   cursor.ChainName,
		Network:     cursor.Network,
		Monitor:     cursor.Monitor,
		NextBlock:   cursor.NextBlock,
		SafeBlock:   cursor.SafeBlock,
		LatestBlock: cursor.LatestBlock,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "key"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"chain_id":   row.ChainID,
				"chain_name": row.ChainName,
				"network":    row.Network,
				"monitor":    row.Monitor,

				"next_block": gorm.Expr(
					"GREATEST(indexer_cursors.next_block, EXCLUDED.next_block)",
				),
				"safe_block": gorm.Expr(
					"GREATEST(indexer_cursors.safe_block, EXCLUDED.safe_block)",
				),
				"latest_block": gorm.Expr(
					"GREATEST(indexer_cursors.latest_block, EXCLUDED.latest_block)",
				),

				"updated_at": now,
			}),
		}).
		Create(&row).
		Error

	if err != nil {
		return fmt.Errorf("save cursor key=%s next_block=%d: %w", cursor.Key, cursor.NextBlock, err)
	}

	return nil
}

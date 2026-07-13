package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"gorm.io/gorm"
)

const (
	BlacklistScopeB20Creator = "b20_creator"
)

type BlacklistStore struct {
	db *gorm.DB
}

func NewBlacklistStore(db *gorm.DB) (*BlacklistStore, error) {
	if db == nil {
		return nil, fmt.Errorf("blacklist db is nil")
	}
	return &BlacklistStore{db: db}, nil
}

func (s *BlacklistStore) IsBlacklisted(ctx context.Context, chainID uint64, scope string, address common.Address) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("blackstore is nil")
	}

	if chainID == 0 {
		return false, fmt.Errorf("chain id is zero")
	}

	if scope == "" {
		return false, fmt.Errorf("blacklist scope is required")
	}

	addr := strings.ToLower(address.Hex())

	var count int64

	err := s.db.WithContext(ctx).Model(&BlacklistAddress{}).Where("chain_id = ?", chainID).Where("scope = ?", scope).Where("address = ?", addr).Where("enabled = ?", true).Count(&count).Error

	if err != nil {
		return false, fmt.Errorf("query blacklist chain_id=%d scope=%s address=%s: %w", chainID, scope, addr, err)
	}

	return count > 0, nil
}

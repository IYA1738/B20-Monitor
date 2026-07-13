package database

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

const migrationAdvisoryLockKey int64 = 910007001 // 没有实际意义 给迁移锁的ID 稳定不冲突就行

func Migrate(ctx context.Context, gdb *gorm.DB) error {
	if gdb == nil {
		return fmt.Errorf("[migrate.go] gorm db is nil")
	}

	err := gdb.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"SELECT pg_advisory_xact_lock(?)",
			migrationAdvisoryLockKey,
		).Error; err != nil {
			return fmt.Errorf("acquire migration advisory lock: %w", err)
		}
		if err := tx.AutoMigrate(
			&IndexerCursor{},
			&ChainEvent{},
			&NotificationOutbox{},
			&ESIndexOutbox{},
			&BlacklistAddress{},
		); err != nil {
			return fmt.Errorf("auto migrate database: %w", err)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("migrate database: %w", err)
	}

	return nil
}

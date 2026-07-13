package database

import (
	"time"

	"gorm.io/datatypes"
)

const (
	OutboxStatusPending    = "pending"
	OutboxStatusProcessing = "processing"
	OutboxStatusSent       = "sent"
	OutboxStatusFailed     = "failed"
	OutboxStatusDead       = "dead"

	ESOutboxStatusPending    = "pending"
	ESOutboxStatusProcessing = "processing"
	ESOutboxStatusIndexed    = "indexed"
	ESOutboxStatusFailed     = "failed"
	ESOutboxStatusDead       = "dead"
)

const (
	NotificationChannelTelegram = "telegram"
)

const (
	ESOperationIndex  = "index"
	ESOperationUpdate = "update"
	ESOperationDelete = "delete"
)

// IndexerCursor 记录某个链、某个 monitor 的扫链进度。
// Key 建议格式：chain_id:network:monitor_name
// 例如：84532:base-sepolia:some-monitor
type IndexerCursor struct {
	Key string `gorm:"primaryKey;type:text"`

	ChainID   uint64 `gorm:"not null;index:idx_cursor_chain_monitor,priority:1"`
	ChainName string `gorm:"not null;type:text;index"`

	Network string `gorm:"not null;type:text;index"`

	Monitor string `gorm:"not null;type:text;index:idx_cursor_chain_monitor,priority:2"`

	// NextBlock 表示下一次从哪个 block 开始扫。
	// 如果已经扫完 100，则 NextBlock = 101。
	NextBlock uint64 `gorm:"not null"`

	// SafeBlock 表示上一次处理完成时使用的 safe height。
	// 这个字段方便排查追块延迟。
	SafeBlock uint64 `gorm:"not null;default:0"`

	// LatestBlock 表示上一次看到的链上最新高度。
	LatestBlock uint64 `gorm:"not null;default:0"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (IndexerCursor) TableName() string {
	return "indexer_cursors"
}

// ChainEvent 是链上事件主表。
// 这张表是 source of truth。
// Redis / ES / notification 都不能替代这张表。
type ChainEvent struct {
	// ID 使用雪花 ID，由应用层生成。
	ID int64 `gorm:"primaryKey;autoIncrement:false"`

	ChainID   uint64 `gorm:"not null;index;uniqueIndex:uniq_chain_tx_log,priority:1"`
	ChainName string `gorm:"not null;type:text;index"`

	Network string `gorm:"not null;type:text;index"`

	Monitor   string `gorm:"not null;type:text;index"`
	EventName string `gorm:"not null;type:text;index"`

	BlockNumber uint64 `gorm:"not null;index"`
	BlockHash   string `gorm:"not null;type:text;index"`

	TxHash   string `gorm:"not null;type:text;index;uniqueIndex:uniq_chain_tx_log,priority:2"`
	TxIndex  uint   `gorm:"not null"`
	LogIndex uint   `gorm:"not null;uniqueIndex:uniq_chain_tx_log,priority:3"`

	ContractAddress string `gorm:"not null;type:text;index"`
	Topic0          string `gorm:"not null;type:text;index"`

	// EventKey 是链上事件的确定性唯一键。
	// 建议格式：chain_id:tx_hash:log_index
	// 例如：84532:0xabc...:7
	EventKey string `gorm:"not null;type:text;uniqueIndex"`

	// Payload 存解析后的业务数据。
	// 基础设施层不关心里面是什么。
	Payload datatypes.JSON `gorm:"not null;type:jsonb"`

	// RawLog 存原始 log 的关键数据，方便排查、重放、重建 ES。
	RawLog datatypes.JSON `gorm:"not null;type:jsonb"`

	// Removed 用于 reorg 场景。
	// confirmed block 扫描时通常是 false。
	Removed bool `gorm:"not null;default:false;index"`

	// ObservedAt 是本服务首次看到该事件的时间。
	ObservedAt time.Time `gorm:"not null;index"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (ChainEvent) TableName() string {
	return "chain_events"
}

// NotificationOutbox 是通用通知任务表。
// 扫链主流程只写 outbox，不直接发通知。
// 具体通知 worker 后续异步消费。
type NotificationOutbox struct {
	ID int64 `gorm:"primaryKey;autoIncrement:false"`

	EventID  int64  `gorm:"not null;index"`
	EventKey string `gorm:"not null;type:text;index;uniqueIndex:uniq_notification_outbox_delivery,priority:1"`

	// Channel 例如：telegram / webhook / email / slack
	Channel string `gorm:"not null;type:text;index;uniqueIndex:uniq_notification_outbox_delivery,priority:2"`

	// Target 例如：chat_id / webhook_url / email address
	Target string `gorm:"not null;type:text;index;uniqueIndex:uniq_notification_outbox_delivery,priority:3"`

	// MessageType 是业务自定义消息类型。
	// 基础设施不关心它的具体含义。
	MessageType string `gorm:"not null;type:text;index;uniqueIndex:uniq_notification_outbox_delivery,priority:4"`

	Payload datatypes.JSON `gorm:"not null;type:jsonb"`

	Status string `gorm:"not null;type:text;index"`

	RetryCount int `gorm:"not null;default:0"`
	MaxRetries int `gorm:"not null;default:10"`

	NextRetryAt time.Time `gorm:"not null;index"`

	// 高并发 worker 抢任务时使用。
	LockedBy    string     `gorm:"type:text;index"`
	LockedUntil *time.Time `gorm:"index"`

	LastError string `gorm:"type:text"`

	SentAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (NotificationOutbox) TableName() string {
	return "notification_outbox"
}

// ESIndexOutbox 是 Elasticsearch 同步任务表。
// ES 是二级索引，失败可以重试，也可以从 chain_events 重建。
type ESIndexOutbox struct {
	ID int64 `gorm:"primaryKey;autoIncrement:false"`

	EventID  int64  `gorm:"not null;index"`
	EventKey string `gorm:"not null;type:text;index"`

	IndexName string `gorm:"not null;type:text;index"`

	// DocumentID 建议使用 EventKey。
	// 这样 ES 写入是幂等的。
	DocumentID string `gorm:"not null;type:text;uniqueIndex"`

	// Operation: index / update / delete
	Operation string `gorm:"not null;type:text;index"`

	Payload datatypes.JSON `gorm:"not null;type:jsonb"`

	Status string `gorm:"not null;type:text;index"`

	RetryCount int `gorm:"not null;default:0"`
	MaxRetries int `gorm:"not null;default:10"`

	NextRetryAt time.Time `gorm:"not null;index"`

	// 高并发 ES worker 抢任务时使用。
	LockedBy    string     `gorm:"type:text;index"`
	LockedUntil *time.Time `gorm:"index"`

	LastError string `gorm:"type:text"`

	IndexedAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (ESIndexOutbox) TableName() string {
	return "es_index_outbox"
}

type BlacklistAddress struct {
	ID uint64 `gorm:"primaryKey"`

	ChainID uint64 `gorm:"uniqueIndex:uidx_blacklist_chain_scope_addr;not null"`
	Scope   string `gorm:"size:64;uniqueIndex:uidx_blacklist_chain_scope_addr;not null"`
	Address string `gorm:"size:42;uniqueIndex:uidx_blacklist_chain_scope_addr;not null"`

	Reason string `gorm:"type:text"`

	Enabled bool `gorm:"index;not null;default:true"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

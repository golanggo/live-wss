package sdk

import (
	"context"
	"time"
)

// DataSource 数据源接口，用于发送和获取消息
type DataSource interface {
	// SendMessage 发送消息到数据源
	SendMessage(ctx context.Context, streamKey string, msg *MessagePb) error

	// GetMessage 从数据源获取消息
	GetMessage(ctx context.Context, streamKey string) []*MessagePb

	// GetRedisBytesSent 获取发送到Redis的字节数
	GetRedisBytesSent(streamKey string) int64

	// GetRedisBytesRecv 获取从Redis接收的字节数
	GetRedisBytesRecv(streamKey string) int64

	// Store 存储键值对到Redis
	Store(ctx context.Context, key string, value any, duration time.Duration) error

	// Get 从Redis获取值
	Get(ctx context.Context, key string) (string, error)

	AccumulatedBy(ctx context.Context, key string, value int64) error
}

// MaxValueDataSource 是数据源可选实现的原子最大值写入能力。
// Room 会优先使用该能力；未实现时会回退为普通 Store，以保持 DataSource 向后兼容。
type MaxValueDataSource interface {
	StoreMax(ctx context.Context, key string, value uint32, duration time.Duration) error
}

type OnlineViewerOperation string

const (
	OnlineViewerJoin      OnlineViewerOperation = "join"
	OnlineViewerLeave     OnlineViewerOperation = "leave"
	OnlineViewerHeartbeat OnlineViewerOperation = "heartbeat"
	OnlineViewerClose     OnlineViewerOperation = "close"
)

type OnlineViewerSyncStatus int64

const (
	OnlineViewerSyncAccepted OnlineViewerSyncStatus = iota
	OnlineViewerSyncRoomFull
	OnlineViewerSyncDuplicate
)

// DistributedOnlineDataSource 是数据源可选实现的分布式在线用户能力。
// SyncOnlineViewerPresence 必须原子清理过期实例、按 viewerID 跨实例去重、校验容量并更新区间峰值。
type DistributedOnlineDataSource interface {
	SyncOnlineViewerPresence(
		ctx context.Context,
		userOwnerKey string,
		userExpiryKey string,
		totalCountKey string,
		roomMaxKey string,
		peakCountKey string,
		instanceID string,
		operation OnlineViewerOperation,
		viewerIDs []string,
		maxViewer uint32,
		ttl time.Duration,
		peakRetention time.Duration,
	) (globalCount uint32, status OnlineViewerSyncStatus, rejectedViewerIDs []string, err error)
}

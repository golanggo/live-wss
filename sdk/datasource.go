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

// CommentStatisticsDataSource 是数据源可选实现的评论人数统计能力。
// RecordCommentUser 必须按 viewerID 原子去重，并返回本场直播的评论人数。
type CommentStatisticsDataSource interface {
	RecordCommentUser(ctx context.Context, userSetKey, userCountKey, viewerID string, retention time.Duration) (commentUserCount uint32, err error)
	ResetCommentUsers(ctx context.Context, userSetKey, userCountKey string) error
}

// CommentEvent 是写入 RabbitMQ 的评论事件契约。消费者应以 EventID 做幂等落库。
type CommentEvent struct {
	MessageID     string    `json:"message_id"`
	FirmUUID      string    `json:"firm_uuid"`
	RoomID        string    `json:"room_id"`
	ViewerID      string    `json:"viewer_id"`
	NickName      string    `json:"nick_name"`
	Avatar        string    `json:"avatar"`
	UserLevel     string    `json:"user_level"`
	Code          string    `json:"code"`
	Data          string    `json:"data"`
	CreatedAt     time.Time `json:"created_at"`
	Timestamp     int64     `json:"timestamp"`
	ShopUUID      int64     `json:"shop_uuid,omitempty"`
	ShopName      string    `json:"shop_name,omitempty"`
	ShopClerkUUID int64     `json:"shop_clerk_uuid,omitempty"`
	ShopClerkName string    `json:"shop_clerk_name,omitempty"`
}

// CommentPublisher 是评论 MQ 发布器的可选能力。PublishComment 成功返回时表示 Broker 已确认持久化。
type CommentPublisher interface {
	PublishComment(ctx context.Context, event CommentEvent) error
	Close() error
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

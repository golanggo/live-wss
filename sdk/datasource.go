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
	Get(ctx context.Context, key string) (any, error)
}

package sdk

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const RedisDataSourceRingBuffer = 8192

// 简化的Redis Stream数据源
type RedisDataSource struct {
	rdbClient *redis.ClusterClient
	streams   map[string]*StreamHandler // StreamKey -> Stream处理器
}

// 创建简单数据源
// 创建新的RedisDataSource实例
func NewRedisDataSource(rdbClient *redis.ClusterClient) *RedisDataSource {
	fmt.Printf("NewRedisDataSource被调用\n") // 添加明显的日志
	// 返回RedisDataSource实例
	return &RedisDataSource{
		rdbClient: rdbClient,
		streams:   make(map[string]*StreamHandler),
	}
}

// Client 获取Redis客户端（用于测试和诊断）
func (s *RedisDataSource) Client() *redis.ClusterClient {
	return s.rdbClient
}

// CreateStreamHandler - 为指定房间创建Stream处理器
func (s *RedisDataSource) CreateStreamHandler(ctx context.Context, roomNumber string, streamKey string) {
	// 创建Stream处理器
	handler := &StreamHandler{
		roomNumber:  roomNumber,
		streamKey:   streamKey,
		rdbClient:   s.rdbClient,                   // 设置client引用
		messageChan: make(chan *MessagePb, 100000), // 保留通道用于向房间传递消息
	}
	// 初始化原子计数器
	handler.messageWriteAt.Store(0)
	handler.messageReadAt.Store(0)
	handler.sendWriteAt.Store(0)
	handler.sendReadAt.Store(0)
	// 创建上下文
	handler.ctx, handler.cancel = context.WithCancel(ctx)

	// 保存Stream处理器到映射表
	s.streams[streamKey] = handler

	// 启动发送协程,发送消息到Redis Stream
	go func() {
		fmt.Printf("runSender协程开始执行\n") // 添加日志
		handler.runSender()
	}()

	// 启动接收协程,从Redis Stream接收消息
	go func() {
		fmt.Printf("runReceiver协程开始执行\n") // 添加日志
		handler.runReceiver()
	}()
}

// SendMessage - 发送消息到Stream
func (s *RedisDataSource) SendMessage(ctx context.Context, steamKey string, msg *MessagePb) error {
	handler, ok := s.streams[steamKey]
	if !ok {
		return fmt.Errorf("stream handler not found for stream key %s", steamKey)
	}

	// 使用环形缓冲区发送消息
	return handler.sendToRingBuffer(msg)
}

// GetMessage - 从Stream获取消息
func (s *RedisDataSource) GetMessage(ctx context.Context, streamKey string) []*MessagePb {
	handler, ok := s.streams[streamKey]
	if !ok {
		return nil
	}

	// 从环形缓冲区读取消息
	return handler.readFromRingBuffer()
}

// GetRedisBytesSent 获取发送到Redis的字节数
func (s *RedisDataSource) GetRedisBytesSent(streamKey string) int64 {
	h, ok := s.streams[streamKey]
	if !ok {
		return 0
	}
	return h.redisBytesSent.Load()
}

// GetRedisBytesRecv 获取从Redis接收的字节数
func (s *RedisDataSource) GetRedisBytesRecv(streamKey string) int64 {
	h, ok := s.streams[streamKey]
	if !ok {
		return 0
	}
	return h.redisBytesRecv.Load()
}

// Store 存储键值对到Redis
func (s *RedisDataSource) Store(ctx context.Context, key string, value any, duration time.Duration) error {
	return s.rdbClient.Set(ctx, key, value, duration).Err()
}

// Get 从Redis获取值
func (s *RedisDataSource) Get(ctx context.Context, key string) (any, error) {
	return s.rdbClient.Get(ctx, key).Result()
}

// Accumulated 累计键值对到Redis
func (s *RedisDataSource) AccumulatedBy(ctx context.Context, key string, value int64) error {
	return s.rdbClient.IncrBy(ctx, key, value).Err()
}

package sdk

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const RedisDataSourceRingBuffer = 8192

// 简化的Redis Stream数据源
type RedisDataSource struct {
	rdbClient *redis.ClusterClient
	streams   map[string]*StreamHandler // 房间号 -> Stream处理器
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
func (s *RedisDataSource) CreateStreamHandler(roomNumber RoomNumber, ctx context.Context) {
	// 构造Stream键
	streamKey := fmt.Sprintf("live:stream:%s", roomNumber)

	// 创建Stream处理器
	handler := &StreamHandler{
		roomNumber:  roomNumber,
		streamKey:   streamKey,
		rdbClient:   s.rdbClient,                 // 设置client引用
		messageChan: make(chan *Message, 100000), // 保留通道用于向房间传递消息
	}
	// 初始化原子计数器
	handler.messageWriteAt.Store(0)
	handler.messageReadAt.Store(0)
	handler.sendWriteAt.Store(0)
	handler.sendReadAt.Store(0)
	// 创建上下文
	handler.ctx, handler.cancel = context.WithCancel(ctx)

	// 保存Stream处理器到映射表
	s.streams[string(roomNumber)] = handler

	// 启动发送协程
	go func() {
		fmt.Printf("runSender协程开始执行\n") // 添加日志
		handler.runSender()
	}()

	// 启动接收协程
	go func() {
		fmt.Printf("runReceiver协程开始执行\n") // 添加日志
		handler.runReceiver()
	}()
}

// SendMessage - 发送消息到Stream
func (s *RedisDataSource) SendMessage(ctx context.Context, roomNumber RoomNumber, msg *Message) error {
	handler, ok := s.streams[string(roomNumber)]
	if !ok {
		return fmt.Errorf("stream handler not found for room %s", roomNumber)
	}

	// 使用环形缓冲区发送消息
	return handler.sendToRingBuffer(msg)
}

// sendToRingBuffer - 发送消息到环形缓冲区
func (h *StreamHandler) sendToRingBuffer(msg *Message) error {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()

	// 检查缓冲区是否已满
	writePos := h.sendWriteAt.Load()
	readPos := h.sendReadAt.Load()
	bufferSize := int64(len(h.sendRingBuf))

	if writePos-readPos >= bufferSize {
		// 缓冲区满，丢弃最旧的消息以腾出空间
		fmt.Printf("警告: Redis数据源房间 %s 的发送缓冲区已满，丢弃最旧消息\n", h.roomNumber)
		h.sendReadAt.Store(readPos + 1)
	}

	// 写入消息到环形缓冲区
	h.sendRingBuf[writePos%bufferSize] = msg
	h.sendWriteAt.Store(writePos + 1)

	fmt.Printf("Redis数据源成功将消息发送到房间 %s 的发送缓冲区\n", h.roomNumber)
	return nil
}

// GetMessage - 从Stream获取消息
func (s *RedisDataSource) GetMessage(ctx context.Context, roomNumber RoomNumber) []*Message {
	handler, ok := s.streams[string(roomNumber)]
	if !ok {
		return nil
	}

	// 从环形缓冲区读取消息
	return handler.readFromRingBuffer()
}

// GetRedisBytesSent 获取发送到Redis的字节数
func (s *RedisDataSource) GetRedisBytesSent(roomNumber RoomNumber) int64 {
	h, ok := s.streams[string(roomNumber)]
	if !ok {
		return 0
	}
	return h.redisBytesSent.Load()
}

// GetRedisBytesRecv 获取从Redis接收的字节数
func (s *RedisDataSource) GetRedisBytesRecv(roomNumber RoomNumber) int64 {
	h, ok := s.streams[string(roomNumber)]
	if !ok {
		return 0
	}
	return h.redisBytesRecv.Load()
}

package live

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const DataSourceRingBuffer = 81920

// 简化的Redis Stream数据源
type SimpleRedisStream struct {
	client  *redis.ClusterClient
	streams map[string]*StreamHandler // 房间号 -> Stream处理器
}

// Stream处理器
type StreamHandler struct {
	roomNumber RoomNumber
	streamKey  string
	client     *redis.ClusterClient // 添加client字段

	// 使用环形缓冲区替代管道
	messageRingBuf [DataSourceRingBuffer]*Message // 消息环形缓冲区
	messageWriteAt atomic.Int64                   // 写入位置
	messageReadAt  atomic.Int64                   // 读取位置
	messageMu      sync.RWMutex                   // 保护环形缓冲区

	sendRingBuf [DataSourceRingBuffer]*Message // 发送环形缓冲区
	sendWriteAt atomic.Int64                   // 发送写入位置
	sendReadAt  atomic.Int64                   // 发送读取位置
	sendMu      sync.RWMutex                   // 保护发送环形缓冲区

	// 用于向房间传递消息的通道
	messageChan chan *Message

	// Redis带宽统计
	redisBytesSent atomic.Int64 // 发送到Redis的字节数
	redisBytesRecv atomic.Int64 // 从Redis接收的字节数

	ctx    context.Context
	cancel context.CancelFunc
}

// 创建简单数据源
// 创建新的SimpleRedisStream实例
func NewSimpleRedisStream() *SimpleRedisStream {
	fmt.Printf("NewSimpleRedisStream被调用\n") // 添加明显的日志
	// 创建Redis集群客户端
	client := redis.NewClusterClient(&redis.ClusterOptions{
		// 配置Redis集群节点地址
		Addrs: []string{
			"18.139.180.76:7001", // Redis集群节点1
			"18.139.180.76:7002", // Redis集群节点2
			"18.139.180.76:7003", // Redis集群节点3
		},
		// 配置密码
		Password: "root", // Redis密码
		// 配置连接池
		PoolSize:     500, // 增加连接池大小到500
		MinIdleConns: 200, // 最小空闲连接200
		MaxIdleConns: 300, // 最大空闲连接300
		ClientName:   "wss_live",
		// 4万人房间优化：增加并发连接处理能力

		// 超时设置，增加超时时间以提高稳定性
		DialTimeout:  15 * time.Second, // 增加拨号超时时间
		ReadTimeout:  20 * time.Second, // 增加读取超时时间
		WriteTimeout: 20 * time.Second, // 增加写入超时时间
		// 优化重试机制
		MaxRetries:      5,                      // 增加重试次数
		MinRetryBackoff: 100 * time.Millisecond, // 增加最小重试间隔
		MaxRetryBackoff: 500 * time.Millisecond, // 增加最大重试间隔
		// 优化集群配置
		MaxRedirects: 3,
		// 连接池优化
		ConnMaxIdleTime: 30 * time.Second, // 连接最大空闲时间
		ConnMaxLifetime: 60 * time.Second, // 连接最大生命周期
		PoolTimeout:     15 * time.Second, // 增加从连接池获取连接超时时间

		// 禁用只读模式，减少不必要的连接
		ReadOnly: false,
		// 使用 Hash Tag 确保 Stream 和消费组在同一节点
		RouteByLatency: true,
		RouteRandomly:  false,
	})

	// 返回SimpleRedisStream实例
	return &SimpleRedisStream{
		client:  client,
		streams: make(map[string]*StreamHandler),
	}
}

// Client 获取Redis客户端（用于测试和诊断）
func (s *SimpleRedisStream) Client() *redis.ClusterClient {
	return s.client
}

// CreateStreamHandler - 为指定房间创建Stream处理器
func (s *SimpleRedisStream) CreateStreamHandler(roomNumber RoomNumber, ctx context.Context) {
	// 构造Stream键
	streamKey := fmt.Sprintf("live:stream:%s", roomNumber)

	// 创建Stream处理器
	handler := &StreamHandler{
		roomNumber:  roomNumber,
		streamKey:   streamKey,
		client:      s.client,                    // 设置client引用
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
func (s *SimpleRedisStream) SendMessage(ctx context.Context, roomNumber RoomNumber, msg *Message) error {
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
func (s *SimpleRedisStream) GetMessage(ctx context.Context, roomNumber RoomNumber) []*Message {
	handler, ok := s.streams[string(roomNumber)]
	if !ok {
		return nil
	}

	// 从环形缓冲区读取消息
	return handler.readFromRingBuffer()
}

// readFromRingBuffer - 从环形缓冲区读取消息
func (h *StreamHandler) readFromRingBuffer() []*Message {
	h.messageMu.Lock()
	defer h.messageMu.Unlock()

	// 批量读取消息
	var messages []*Message
	readPos := h.messageReadAt.Load()
	writePos := h.messageWriteAt.Load()

	// 限制每次读取的消息数量
	maxMessages := int64(100)
	available := writePos - readPos
	if available > maxMessages {
		available = maxMessages
	}

	bufferSize := int64(len(h.messageRingBuf))
	for i := int64(0); i < available; i++ {
		index := (readPos + i) % bufferSize
		messages = append(messages, h.messageRingBuf[index])
		h.messageRingBuf[index] = nil // 清空引用
	}

	// 更新读取位置
	if len(messages) > 0 {
		h.messageReadAt.Store(readPos + int64(len(messages)))
	}

	return messages
}

// runSender - 发送消息到Redis Stream
func (h *StreamHandler) runSender() {
	batch := make([]*Message, 0, h.getBatchSize())
	ticker := time.NewTicker(h.getTickerDuration())
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			// 退出前发送剩余消息
			if len(batch) > 0 {
				h.sendBatch(batch)
			}
			return

		case <-ticker.C:
			// 从环形缓冲区读取待发送的消息
			messages := h.readFromSendBuffer()
			if len(messages) > 0 {
				batch = append(batch, messages...)
			}

			if len(batch) >= h.getBatchSize() {
				h.sendBatch(batch)
				batch = batch[:0]
			} else if len(batch) > 0 {
				// 定时发送剩余消息
				h.sendBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// readFromSendBuffer - 从发送环形缓冲区读取消息
func (h *StreamHandler) readFromSendBuffer() []*Message {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()

	var messages []*Message
	readPos := h.sendReadAt.Load()
	writePos := h.sendWriteAt.Load()

	// 限制每次读取的消息数量
	maxMessages := int64(50)
	available := writePos - readPos
	if available > maxMessages {
		available = maxMessages
	}

	bufferSize := int64(len(h.sendRingBuf))
	for i := int64(0); i < available; i++ {
		index := (readPos + i) % bufferSize
		messages = append(messages, h.sendRingBuf[index])
		h.sendRingBuf[index] = nil // 清空引用
	}

	// 更新读取位置
	if len(messages) > 0 {
		h.sendReadAt.Store(readPos + int64(len(messages)))
	}

	return messages
}

// sendBatch - 批量发送到Redis
func (h *StreamHandler) sendBatch(messages []*Message) {
	if len(messages) == 0 {
		return
	}

	// 增加上下文超时时间到10秒
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Printf("[DEBUG] sendBatch: 准备发送 %d 条消息到Redis，房间=%s\n", len(messages), h.roomNumber)

	pipe := h.client.Pipeline()

	// 限制每次批量发送的消息数量为40条，避免单次发送过多消息导致超时
	maxBatchSize := 40
	for i := 0; i < len(messages); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(messages) {
			end = len(messages)
		}

		batch := messages[i:end]
		for _, msg := range batch {
			data, err := json.Marshal(msg)
			if err != nil {
				fmt.Printf("序列化消息失败: %v\n", err)
				continue
			}
			// 统计发送到Redis的字节数
			h.redisBytesSent.Add(int64(len(data)))
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: h.streamKey,
				Values: map[string]interface{}{
					"data": string(data),     // 转换为string，Redis Stream中存储为string
					"type": string(msg.Type), // 将MessageType转换为字符串
				},
				MaxLen: 100000, // 最多保留100000条消息
				Approx: true,   // 近似修剪，性能更好
			})
		}
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Printf("[ERROR] 批量发送消息到Redis Stream失败: %v，房间=%s\n", err, h.roomNumber)
		// 可以在这里添加重试逻辑
	} else {
		fmt.Printf("[DEBUG] sendBatch: 成功发送！房间=%s，结果数=%d\n", h.roomNumber, len(results))
	}
}

// runReceiver - 从Redis接收消息
func (h *StreamHandler) runReceiver() {
	// 获取服务器hostname作为组名和消费者名
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("获取hostname失败: %v，使用默认值\n", err)
		hostname = "default-host"
	}
	// 为每个hostname创建独立的消费组，这样多个实例可以各自接收完整的消息
	groupName := fmt.Sprintf("%s:group", hostname) // 每个实例有独立的消费组
	consumerName := "consumer1"                    // 消费者名称统一

	// 创建消费组（如果不存在）
	ctx := context.Background()
	err = h.client.XGroupCreateMkStream(ctx, h.streamKey, groupName, "0-0").Err()
	if err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			// 消费组创建失败，回退到普通读取模式
			fmt.Printf("[WARN] 创建消费组失败: %v，房间=%s，尝试回退到普通读取模式\n", err, h.roomNumber)
			return
		}
	}

	// 先检查一下是否有消息
	testResults, err := h.client.XLen(ctx, h.streamKey).Result()
	if err != nil {
		fmt.Printf("[DEBUG] 检查Redis Stream长度失败: %v，房间=%s\n", err, h.roomNumber)
	} else {
		fmt.Printf("[DEBUG] Redis Stream %s 当前消息数量: %d\n", h.streamKey, testResults)
	}

	for {
		select {
		case <-h.ctx.Done():
			return
		default:
			// 从消费组读取消息
			ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
			results, err := h.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{h.streamKey, ">"}, // 使用 ">" 读取未消费的消息
				Count:    100,
				Block:    1 * time.Second, // 阻塞1秒等待新消息
				NoAck:    true,            // 自动确认消息
			}).Result()
			cancel()

			if err != nil {
				if err == redis.Nil {
					// 没有新消息，继续等待
					continue
				}
				// 其他错误，短暂等待后重试
				fmt.Printf("[DEBUG] 接收消息错误: %v，房间=%s\n", err, h.roomNumber)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// 处理消息
			messageCount := 0
			for _, stream := range results {
				for _, msg := range stream.Messages {
					messageCount++
					// 解析消息
					if data, ok := msg.Values["data"].(string); ok {
						// 统计从Redis接收的字节数
						h.redisBytesRecv.Add(int64(len(data)))
						var message Message
						if err := json.Unmarshal([]byte(data), &message); err == nil {
							// 发送到消息环形缓冲区
							h.writeToMessageRingBuffer(&message)
						} else {
							fmt.Printf("[ERROR] 解析消息失败: %v\n", err)
						}
					}
				}
			}
			if messageCount > 0 {
				// 追踪消息沈变过程 - 从 Redis 接收
				fmt.Printf("[STAT] Redis接收: 房间=%s, 消息数=%d, ringBuf位置: write=%d, read=%d\n",
					h.roomNumber, messageCount, h.messageWriteAt.Load(), h.messageReadAt.Load())
			}
		}
	}
}

// writeToMessageRingBuffer - 写入消息到消息环形缓冲区
func (h *StreamHandler) writeToMessageRingBuffer(msg *Message) {
	h.messageMu.Lock()
	defer h.messageMu.Unlock()

	// 检查缓冲区是否已满
	writePos := h.messageWriteAt.Load()
	readPos := h.messageReadAt.Load()
	bufferSize := int64(len(h.messageRingBuf))

	if writePos-readPos >= bufferSize {
		// 缓冲区满，丢弃最旧的消息
		fmt.Printf("警告: 消息环形缓冲区已满，丢弃最旧消息 room=%s\n", h.roomNumber)
		h.messageReadAt.Store(readPos + 1)
	}

	// 写入消息到环形缓冲区
	h.messageRingBuf[writePos%bufferSize] = msg
	h.messageWriteAt.Store(writePos + 1)

	// 同时发送到channel以保持兼容性
	select {
	case h.messageChan <- msg:
		fmt.Printf("成功将消息发送到messageChan: room=%s, viewer=%s\n", h.roomNumber, msg.ViewerID)
	default:
		// 通道满，记录警告但不阻塞
		fmt.Printf("警告: Stream处理器消息通道已满，消息可能丢失 room=%s\n", h.roomNumber)
	}

	// 记录消息处理
	fmt.Printf("房间 %s 处理消息: viewer=%s\n", h.roomNumber, msg.ViewerID)
}

// getBatchSize 根据用户规模动态设置batch大小
func (h *StreamHandler) getBatchSize() int {
	// 获取房间的实际观众数
	actualUsers := h.getActualUserCount()

	// 根据内存规范设置batch大小
	switch {
	case actualUsers <= 10000:
		return 64
	case actualUsers <= 20000:
		return 128
	case actualUsers <= 40000:
		return 256
	default:
		return 512
	}
}

// getTickerDuration 根据用户规模动态设置定时器间隔
func (h *StreamHandler) getTickerDuration() time.Duration {
	// 获取房间的实际观众数
	actualUsers := h.getActualUserCount()

	// 用户数越多，定时器间隔越短
	switch {
	case actualUsers <= 10000:
		return 50 * time.Millisecond
	case actualUsers <= 20000:
		return 30 * time.Millisecond
	case actualUsers <= 40000:
		return 20 * time.Millisecond
	default:
		return 10 * time.Millisecond
	}
}

// getActualUserCount 获取房间的实际观众数
// 这是一个示例实现，实际应用中应该从房间管理器获取准确的用户数
func (h *StreamHandler) getActualUserCount() int {
	// 在实际应用中，这里应该通过房间管理器获取准确的用户数
	// 例如：return roomManager.GetRoomUserCount(h.roomNumber)

	// 临时实现，可以根据房间号或其他信息估算用户数
	// 这里我们简单返回一个默认值
	return 10000
}

// GetRedisBytesSent 获取发送到Redis的字节数
func (s *SimpleRedisStream) GetRedisBytesSent(roomNumber RoomNumber) int64 {
	h, ok := s.streams[string(roomNumber)]
	if !ok {
		return 0
	}
	return h.redisBytesSent.Load()
}

// GetRedisBytesRecv 获取从Redis接收的字节数
func (s *SimpleRedisStream) GetRedisBytesRecv(roomNumber RoomNumber) int64 {
	h, ok := s.streams[string(roomNumber)]
	if !ok {
		return 0
	}
	return h.redisBytesRecv.Load()
}

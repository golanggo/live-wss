package sdk

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// Stream处理器
type StreamHandler struct {
	roomNumber string
	streamKey  string
	rdbClient  *redis.ClusterClient // 添加client字段

	// 使用环形缓冲区替代管道
	messageRingBuf [RedisDataSourceRingBuffer]*MessagePb // 消息环形缓冲区
	messageWriteAt atomic.Int64                          // 写入位置
	messageReadAt  atomic.Int64                          // 读取位置
	messageMu      sync.RWMutex                          // 保护环形缓冲区

	sendRingBuf [RedisDataSourceRingBuffer]*MessagePb // 发送环形缓冲区
	sendWriteAt atomic.Int64                          // 发送写入位置
	sendReadAt  atomic.Int64                          // 发送读取位置
	sendMu      sync.RWMutex                          // 保护发送环形缓冲区

	// 用于向房间传递消息的通道
	messageChan chan *MessagePb

	// Redis带宽统计
	redisBytesSent atomic.Int64 // 发送到Redis的字节数
	redisBytesRecv atomic.Int64 // 从Redis接收的字节数

	ctx    context.Context
	cancel context.CancelFunc

	// 添加一个发送计数器，用于控制Expire操作频率
	sendCounter atomic.Int64
}

// readFromRingBuffer - 从环形缓冲区读取消息
func (h *StreamHandler) readFromRingBuffer() []*MessagePb {
	h.messageMu.Lock()
	defer h.messageMu.Unlock()

	// 批量读取消息
	var messages []*MessagePb
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
	batch := make([]*MessagePb, 0, h.getBatchSize())
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
func (h *StreamHandler) readFromSendBuffer() []*MessagePb {
	h.sendMu.Lock()
	defer h.sendMu.Unlock()

	var messages []*MessagePb
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

// sendToRingBuffer - 发送消息到环形缓冲区
func (h *StreamHandler) sendToRingBuffer(msg *MessagePb) error {
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

	//fmt.Printf("Redis数据源成功将消息发送到房间 %s 的发送缓冲区\n", h.roomNumber)
	return nil
}

// sendBatch - 批量发送到Redis
func (h *StreamHandler) sendBatch(messages []*MessagePb) {
	if len(messages) == 0 {
		return
	}

	// 增加上下文超时时间到10秒
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//fmt.Printf("[DEBUG] sendBatch: 准备发送 %d 条消息到Redis，房间=%s\n", len(messages), h.roomNumber)

	pipe := h.rdbClient.Pipeline()

	// 限制每次批量发送的消息数量为40条，避免单次发送过多消息导致超时
	maxBatchSize := 40
	for i := 0; i < len(messages); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(messages) {
			end = len(messages)
		}

		batch := messages[i:end]
		for _, msg := range batch {
			data, err := proto.Marshal(msg)
			if err != nil {
				fmt.Printf("序列化消息失败: %v\n", err)
				continue
			}

			// 对消息数据进行压缩
			compressedData, err := CompressMessagePb(data)
			if err != nil {
				fmt.Printf("压缩消息失败: %v, 使用原始数据\n", err)
				compressedData = data
			}
			// 统计发送到Redis的字节数
			h.redisBytesSent.Add(int64(len(data)))
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: h.streamKey,
				Values: map[string]interface{}{"payload": string(compressedData)},
				MaxLen: 100000, // 最多保留100000条消息
				Approx: true,   // 近似修剪，性能更好
			})
		}
	}

	// 使用计数器控制Expire操作频率，每20次发送才设置一次过期时间
	sendCount := h.sendCounter.Add(1)
	if sendCount%20 == 0 {
		// 基础时长：7天 + 6小时
		baseDuration := 7*24*time.Hour + 6*time.Hour
		// 随机时长：0~6小时（6*3600秒 = 21600秒）
		randDuration := time.Duration(rand.Intn(6*3600)) * time.Second
		// 最终过期时间 = 基础时长 + 随机时长
		totalExpireDuration := baseDuration + randDuration

		// 设置Stream key过期时间
		pipe.Expire(ctx, h.streamKey, totalExpireDuration)
	}
	results, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Printf("[ERROR] 批量发送消息到Redis Stream失败: %v，房间=%s\n", err, h.roomNumber)
		// 可以在这里添加重试逻辑
	} else {
		fmt.Printf("[DEBUG] sendBatch: 成功发送！房间=%s，结果数=%d\n", h.roomNumber, len(results))
	}
}

// runReceiver - 从Redis Stream接收消息
func (h *StreamHandler) runReceiver() {
	// 获取服务器hostname作为组名和消费者名
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("获取hostname失败: %v，使用默认值\n", err)
		hostname = DefaultHostName
	}
	// 为每个hostname创建独立的消费组，这样多个实例可以各自接收完整的消息
	groupName := fmt.Sprintf(GroupName, hostname) // 每个实例有独立的消费组
	consumerName := DefaultConsumerName           // 消费者名称统一

	// 创建消费组（如果不存在）
	ctx := context.Background()
	err = h.rdbClient.XGroupCreateMkStream(ctx, h.streamKey, groupName, "$").Err()
	if err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			// 消费组创建失败，回退到普通读取模式
			fmt.Printf("[WARN] 创建消费组失败: %v，房间=%s，尝试回退到普通读取模式\n", err, h.roomNumber)
			return
		}
	}

	// 先检查一下是否有消息
	testResults, err := h.rdbClient.XLen(ctx, h.streamKey).Result()
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
			results, err := h.rdbClient.XReadGroup(ctx, &redis.XReadGroupArgs{
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
					if data, ok := msg.Values["payload"].(string); ok {
						// 统计从Redis接收的字节数
						h.redisBytesRecv.Add(int64(len(data)))
						var message MessagePb
						compressedBytes := []byte(data)
						// 尝试解压缩消息
						decompressedData, err := DecompressMessagePb(compressedBytes)
						if err != nil {
							fmt.Printf("解压缩消息失败: %v, 尝试直接解析\n", err)
							// 如果解压缩失败，尝试直接解析原始数据
							decompressedData = compressedBytes
						}

						if err := proto.Unmarshal([]byte(decompressedData), &message); err == nil {
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
				// fmt.Printf("[STAT] Redis接收: 房间=%s, 消息数=%d, ringBuf位置: write=%d, read=%d\n",
				// 	h.roomNumber, messageCount, h.messageWriteAt.Load(), h.messageReadAt.Load())
			}
		}
	}
}

// writeToMessageRingBuffer - 写入消息到消息环形缓冲区
func (h *StreamHandler) writeToMessageRingBuffer(msg *MessagePb) {
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
		//fmt.Printf("成功将消息发送到messageChan: room=%s \n", h.roomNumber)
	default:
		// 通道满，记录警告但不阻塞
		fmt.Printf("警告: Stream处理器消息通道已满，消息可能丢失 room=%s\n", h.roomNumber)
	}

	// 记录消息处理
	//fmt.Printf("房间 %s 处理消息 \n", h.roomNumber)
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
	case actualUsers <= 500:
		return 200 * time.Millisecond
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

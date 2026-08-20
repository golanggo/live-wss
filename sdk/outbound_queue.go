package sdk

import (
	"context"
	"sync"
	"sync/atomic"
)

const defaultViewerOutboundQueueCapacity = 1024

// FanoutQueueStats 是单个观众的出站队列统计。DroppedLow 包含因队列满或被高优先级消息逐出的低优先级消息。
type FanoutQueueStats struct {
	QueuedHigh  uint64
	QueuedLow   uint64
	DroppedLow  uint64
	DroppedHigh uint64
}

type outboundMessage struct {
	data     []byte
	priority MessagePriority
}

// viewerOutboundQueue 是每个观众独占的、有界优先级队列。
//
// 队列在同一个互斥锁下维护高、低两个 FIFO。高优先级写入会先清空所有尚未扇出的
// 低优先级消息，再入队；读取端始终先取高优先级消息。因此高优先级消息不会排在低优先级
// 积压之后，也不会因低优先级积压而被覆盖。
type viewerOutboundQueue struct {
	mu       sync.Mutex
	notify   chan struct{}
	closed   bool
	capacity int

	high     []outboundMessage
	highHead int
	low      []outboundMessage
	lowHead  int

	queuedHigh  atomic.Uint64
	queuedLow   atomic.Uint64
	droppedLow  atomic.Uint64
	droppedHigh atomic.Uint64
}

func newViewerOutboundQueue(capacity int) *viewerOutboundQueue {
	if capacity <= 0 {
		capacity = defaultViewerOutboundQueueCapacity
	}
	return &viewerOutboundQueue{
		capacity: capacity,
		notify:   make(chan struct{}, 1),
	}
}

// enqueue 非阻塞地写入一条已经序列化的消息。返回 false 仅表示队列已关闭，或高优先级
// 消息本身已超过有界容量。低优先级消息在容量不足时会直接丢弃，避免慢用户反压直播间。
func (q *viewerOutboundQueue) enqueue(data []byte, priority MessagePriority) bool {
	if q == nil {
		return false
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}

	if priority == MessagePriority_HIGH {
		// 高优先级消息到达时，所有未发送的低优先级消息都不再有价值，立即逐出。
		if dropped := len(q.low) - q.lowHead; dropped > 0 {
			for i := q.lowHead; i < len(q.low); i++ {
				q.low[i].data = nil
			}
			q.low = q.low[:0]
			q.lowHead = 0
			q.droppedLow.Add(uint64(dropped))
		}
		if len(q.high)-q.highHead >= q.capacity {
			q.droppedHigh.Add(1)
			q.mu.Unlock()
			return false
		}
		q.high = append(q.high, outboundMessage{data: data, priority: MessagePriority_HIGH})
		q.queuedHigh.Add(1)
	} else {
		if len(q.high)-q.highHead+len(q.low)-q.lowHead >= q.capacity {
			q.droppedLow.Add(1)
			q.mu.Unlock()
			return false
		}
		q.low = append(q.low, outboundMessage{data: data, priority: MessagePriority_LOW})
		q.queuedLow.Add(1)
	}
	q.mu.Unlock()

	// 合并唤醒信号；消费者会在一次唤醒后持续抽取队列，生产者不会被慢消费者阻塞。
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

func (q *viewerOutboundQueue) dequeue(ctx context.Context) (outboundMessage, bool) {
	var zero outboundMessage
	if q == nil {
		return zero, false
	}

	for {
		q.mu.Lock()
		if q.highHead < len(q.high) {
			msg := q.high[q.highHead]
			q.high[q.highHead].data = nil
			q.highHead++
			if q.highHead == len(q.high) {
				q.high = q.high[:0]
				q.highHead = 0
			}
			q.mu.Unlock()
			return msg, true
		}
		if q.lowHead < len(q.low) {
			msg := q.low[q.lowHead]
			q.low[q.lowHead].data = nil
			q.lowHead++
			if q.lowHead == len(q.low) {
				q.low = q.low[:0]
				q.lowHead = 0
			}
			q.mu.Unlock()
			return msg, true
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return zero, false
		}

		select {
		case <-ctx.Done():
			return zero, false
		case <-q.notify:
		}
	}
}

func (q *viewerOutboundQueue) close() {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	for i := range q.high {
		q.high[i].data = nil
	}
	for i := range q.low {
		q.low[i].data = nil
	}
	q.high = nil
	q.highHead = 0
	q.low = nil
	q.lowHead = 0
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *viewerOutboundQueue) stats() FanoutQueueStats {
	if q == nil {
		return FanoutQueueStats{}
	}
	return FanoutQueueStats{
		QueuedHigh:  q.queuedHigh.Load(),
		QueuedLow:   q.queuedLow.Load(),
		DroppedLow:  q.droppedLow.Load(),
		DroppedHigh: q.droppedHigh.Load(),
	}
}

// GetFanoutQueueStats 返回该观众出站扇出队列的统计，用于观测用户侧丢弃与高优先级保护效果。
func (v *Viewer) GetFanoutQueueStats() FanoutQueueStats {
	if v == nil {
		return FanoutQueueStats{}
	}
	return v.outbound.stats()
}

func (v *Viewer) enqueueOutboundMessage(data []byte, priority MessagePriority) bool {
	if v == nil || v.closed.Load() {
		return false
	}
	return v.outbound.enqueue(data, priority)
}

func (v *Viewer) nextOutboundMessage() (outboundMessage, bool) {
	if v == nil {
		return outboundMessage{}, false
	}
	return v.outbound.dequeue(v.viewerCtx)
}

// deliverOutboundMessage 保持每条消息独立发送，使高优先级消息最多只等待当前正在进行的一次 WebSocket 写入。
func (v *Viewer) deliverOutboundMessage(msg outboundMessage) {
	if len(msg.data) == 0 {
		return
	}
	v.SendMessagesToWebSocket([][]byte{msg.data})
}

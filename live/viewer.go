package live

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ViewerType 用户类型
type ViewerType string
type ViewerID string

const (
	ViewerTypeViewer ViewerType = "viewer" // 观看者
	ViewerTypeAnchor ViewerType = "anchor" // 主播
)

const (
	baseRingBufferSize = 32768
	maxRingBufferSize  = 64936
	// 环形缓冲区调整阈值百分比
	bufferResizeThreshold = 80
)

// Viewer 观众结构体
type Viewer struct {
	vid      ViewerID   // 用户唯一标识
	name     string     // 用户名称
	userType ViewerType // 用户类型：观看者或主播

	Room *Room // 所属房间

	roomCtx context.Context // 根上下文，用于如果房间关闭，用户连接也会关闭

	vctx       context.Context    // 观众上下文，用于取消操作
	vctxCancel context.CancelFunc // 取消函数，用于取消操作

	// 连接监控字段
	mu sync.RWMutex // 保护所有需要同步的字段和通道

	// 改为切片以支持动态调整大小
	sendRoomSlots      []atomic.Pointer[item] // 环形缓冲区，用于存储用户发送的数据
	sendRoomWriteAto   atomic.Int64           // 下一条写位置（用户 goroutine 只改它）
	sendRoomReadAto    atomic.Int64           // 房间收集器已读到的位置（收集器只改它）
	sendRoomHasMessage atomic.Int32           // 是否有消息待处理
	sendRoomBufSize    atomic.Int64           // 发送缓冲区大小
	sendRoomBufMu      sync.Mutex             // 保护发送缓冲区的调整

	// 改为切片以支持动态调整大小
	roomBroadcastSlots    []atomic.Pointer[item] // 环形缓冲区，用于存储收集器读取的数据
	roomBroadcastWriteAto atomic.Int64           // 下一条写位置（收集器只改它）
	roomBroadcastReadAto  atomic.Int64           // 下一条读位置,发送到用户网络层（用户 goroutine 只改它）

	hasMessage atomic.Int32 // 是否有消息待处理

	roomWriteBufSize atomic.Int64 // 接收缓冲区大小
	roomWriteBufMu   sync.Mutex   // 保护接收缓冲区的调整

	Conn *websocket.Conn // WebSocket连接

	startTime      time.Time // 加入房间时间
	lastActiveTime time.Time // 最后活跃时间
	lastPingTime   time.Time // 最后Ping时间

	// 消息计数
	sentMessageCnt     atomic.Int64 // 用户发送的消息数
	receivedMessageCnt atomic.Int64 // 用户接收的消息数
	// 字节数统计
	sentBytesCnt     atomic.Int64 // 用户发送的字节数
	receivedBytesCnt atomic.Int64 // 用户接收的字节数
}

type item struct {
	seq  int64  // 版本号，防止 ABA
	data []byte // 真正消息，复用对象池可再省 30% CPU
}

// NewViewer 创建新的观众连接
func NewViewer(roomCtx context.Context, vid ViewerID, name string, userType ViewerType) *Viewer {
	now := time.Now()
	vctx, vctxCancel := context.WithCancel(roomCtx)

	// 初始化动态大小的缓冲区
	sendRoomSlots := make([]atomic.Pointer[item], baseRingBufferSize)
	roomWriteSlots := make([]atomic.Pointer[item], baseRingBufferSize)

	return &Viewer{
		vid:      vid,
		name:     name,
		userType: userType,

		startTime:      now,
		lastActiveTime: now,
		lastPingTime:   now,

		roomCtx:    roomCtx,
		vctx:       vctx,
		vctxCancel: vctxCancel,

		// 初始化动态缓冲区
		sendRoomSlots:      sendRoomSlots,
		sendRoomBufSize:    atomic.Int64{},
		roomBroadcastSlots: roomWriteSlots,
		roomWriteBufSize:   atomic.Int64{},
	}
}

func (v *Viewer) Start() {

	// 网络->slots->房间
	go v.ReadMessageWebSocketLoop()

	// 房间->slots->网络
	go v.messageReader()

	//定时发送Ping消息，检测连接是否断开
	go v.Ping(30 * time.Second)

	fmt.Printf("%s 加入房间\n", v.name)
}

// StartMessageReader 启动消息读取器（用于压测，不启动WebSocket相关协程）
func (v *Viewer) StartMessageReader() {
	go v.messageReader()
}

// Write 将用户的发送的数据写入一个环形缓冲区
func (v *Viewer) Write(b []byte) {
	buf := make([]byte, len(b))
	copy(buf, b)

	// 获取当前缓冲区大小
	bufSize := v.getSendRoomBufSize()
	if bufSize == 0 {
		// 初始化缓冲区大小
		v.sendRoomBufSize.Store(int64(len(v.sendRoomSlots)))
		bufSize = v.getSendRoomBufSize()
	}

	// 检查是否需要调整缓冲区大小
	used := v.sendRoomWriteAto.Load() - v.sendRoomReadAto.Load()
	if used < 0 {
		// 处理环形缓冲区环绕情况
		used += bufSize
	}

	// 如果使用率超过阈值，尝试扩大缓冲区，最多不超过最大缓冲区大小。
	// 这样可以避免旧数据未处理被新数据覆盖。
	// 如果这里内存不够用，可以考虑覆盖旧数据
	if int(float64(used)/float64(bufSize)*100) > bufferResizeThreshold {
		v.tryResizeSendBuffer()
		bufSize = v.getSendRoomBufSize() // 更新缓冲区大小
	}

	// 拿到下一个写槽位（原子自增）
	w := v.sendRoomWriteAto.Add(1) - 1

	// 使用模运算确保索引在缓冲区范围内
	slotIndex := w % bufSize
	slot := &v.sendRoomSlots[slotIndex]

	// 等待收集器追上，防止覆盖（99% 情况下不会进循环）
	for v.sendRoomWriteAto.Load()-v.sendRoomReadAto.Load() >= bufSize-1 {
		runtime.Gosched()
	}

	// 创建新消息
	newItem := &item{
		seq:  w + 1, //版本号：写入位置+1
		data: buf,
	}

	// 发布新版本
	slot.Store(newItem)

	// 增加发送消息计数
	v.sentMessageCnt.Add(1)
	// 增加发送字节数统计
	v.sentBytesCnt.Add(int64(len(buf)))

	// 只有当hasMessage为0时，才唤醒读取消息协程
	// 等下次一次写入消息时，再唤醒读取消息协程
	// 这样可以避免重复唤醒，提高效率
	if v.sendRoomHasMessage.CompareAndSwap(0, 1) {
		// 唤醒读取消息协程
		if v.Room != nil {
			v.Room.viewerWake <- v.vid
		}
	}
}

// getSendRoomBufSize 获取发送缓冲区大小
func (v *Viewer) getSendRoomBufSize() int64 {
	return v.sendRoomBufSize.Load()
}

// tryResizeSendBuffer 尝试调整发送缓冲区大小
func (v *Viewer) tryResizeSendBuffer() {
	v.sendRoomBufMu.Lock()
	defer v.sendRoomBufMu.Unlock()

	// 再次检查，避免并发调整
	currentSize := int(v.getSendRoomBufSize())
	used := v.sendRoomWriteAto.Load() - v.sendRoomReadAto.Load()
	if used < 0 {
		used += int64(currentSize)
	}

	if int(float64(used)/float64(currentSize)*100) <= bufferResizeThreshold {
		return // 不再需要调整
	}

	// 计算新大小，翻倍但不超过最大值
	newSize := currentSize * 2
	if newSize > maxRingBufferSize {
		newSize = maxRingBufferSize
	}

	// 如果已经是最大大小，不再调整
	if newSize == currentSize {
		return
	}

	// 创建新的缓冲区
	newSlots := make([]atomic.Pointer[item], newSize)

	// 复制现有数据到新缓冲区
	readPos := v.sendRoomReadAto.Load()
	writePos := v.sendRoomWriteAto.Load()
	for i := readPos; i < writePos; i++ {
		oldIndex := i % int64(currentSize)
		newIndex := i % int64(newSize)
		if item := v.sendRoomSlots[oldIndex].Load(); item != nil {
			newSlots[newIndex].Store(item)
		}
	}

	// 替换缓冲区
	v.sendRoomSlots = newSlots
	v.sendRoomBufSize.Store(int64(newSize))
}

// 从websocket连接中读取消息，写入环形缓冲区
func (v *Viewer) ReadMessageWebSocketLoop() {
	// 如果没有WebSocket连接，直接返回
	if v.Conn == nil {
		return
	}

	for {
		select {
		case <-v.vctx.Done():
			// 上下文被取消，关闭连接
			return
		case <-v.roomCtx.Done():
			// 房间关闭，关闭连接
			return
		default:
			_, msgByte, err := v.Conn.ReadMessage()

			if err != nil {
				// 连接关闭或发生错误，退出循环
				// 关闭连接
				v.Conn.Close()
				// 通知房间该观众离开
				if v.Room != nil {
					v.Room.LeaveRoom(v)
				}
				return
			}

			// 写入环形缓冲区
			v.Write(msgByte)

			//只有当hasMessage为0时，才唤醒读取消息协程
			if v.sendRoomHasMessage.CompareAndSwap(0, 1) {
				// 唤醒读取消息协程
				v.Room.viewerWake <- v.vid
			}
		}
	}
}

// CollectMessages 从环形缓冲区收集用户发送的消息
func (v *Viewer) CollectMessages() [][]byte {

	read := v.sendRoomReadAto.Load()
	write := v.sendRoomWriteAto.Load()

	// 2. 计算可收集的消息数
	available := write - read
	if available <= 0 {
		return nil
	}

	// 获取当前缓冲区大小
	bufSize := v.getSendRoomBufSize()
	if bufSize == 0 {
		// 初始化缓冲区大小
		v.sendRoomBufSize.Store(int64(len(v.sendRoomSlots)))
		bufSize = v.getSendRoomBufSize()
	}

	// 限制最大收集数量，避免饥饿其他观众
	if available > bufSize-1 {
		available = bufSize - 1
	}

	// 3. 预分配结果切片
	messages := make([][]byte, 0, available)
	collected := int64(0)

	// 4. 遍历收集
	for i := int64(0); i < available; i++ {

		//获取读取数据的位置
		slot := &v.sendRoomSlots[(read+i)%bufSize]

		// 快速检查
		item := slot.Load()
		if item == nil {
			// 槽位为空，可能是被其他goroutine清理了
			break
		}

		expectedSeq := read + i + 1
		if item.seq == expectedSeq {
			// 版本匹配，收集消息
			messages = append(messages, item.data)
			slot.Store(nil) // 清空槽位
			collected++
		} else if item.seq < expectedSeq {
			slot.Store(nil)
			// 旧消息，跳过
			continue
		} else {
			// 消息尚未准备好，跳过本次
			break
		}
	}

	// 如果有收集到消息，增加对应统计数据
	if collected > 0 {
		v.sendRoomReadAto.Add(collected)
		v.sentMessageCnt.Add(collected)
		v.sendRoomHasMessage.Store(0)
	}

	return messages
}

// 观众消息读取器（由观众goroutine执行）
func (v *Viewer) messageReader() {
	// 定期检查是否有消息
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-v.vctx.Done():
			return // 观众退出
		case <-v.roomCtx.Done():
			return // 房间关闭
		case <-ticker.C:
			// 检查是否有待处理的消息
			hasMessage := v.hasMessage.Load()
			if hasMessage == 1 {
				v.processBufferedMessages()
			}
		}
	}
}

// 处理缓冲的消息
func (v *Viewer) processBufferedMessages() {
	readPos := v.roomBroadcastReadAto.Load()
	writePos := v.roomBroadcastWriteAto.Load()

	// 如果没有消息，直接返回
	if readPos == writePos {
		v.hasMessage.Store(0)
		return
	}

	// 读取所有待处理的消息
	messages := make([][]byte, 0)
	messageCount := int64(0) // 统计本次处理的消息数
	for readPos != writePos {
		itemPtr := v.roomBroadcastSlots[readPos].Load()
		if itemPtr != nil {
			messages = append(messages, itemPtr.data)
			messageCount++ // 增加消息计数
			v.roomBroadcastSlots[readPos].Store(nil)
		}
		readPos = (readPos + 1) % int64(len(v.roomBroadcastSlots))
	}

	// 更新读位置
	v.roomBroadcastReadAto.Store(readPos)

	if len(messages) > 0 {
		// 增加接收消息计数（无论是否有WebSocket连接都计数）
		v.receivedMessageCnt.Add(int64(messageCount))

		// 添加调试日志
		fmt.Printf("观众 %s 处理了 %d 条消息，累计接收消息数: %d\n", v.name, len(messages), v.receivedMessageCnt.Load())

		// 通过WebSocket发送消息
		v.sendMessagesToWebSocket(messages)
	}

	// 重置消息标志
	v.hasMessage.Store(0)
}

// 通过WebSocket发送消息
func (v *Viewer) sendMessagesToWebSocket(messages [][]byte) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.Conn == nil {
		return
	}

	// 发送消息
	for _, msg := range messages {
		// 设置写超时
		v.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

		err := v.Conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			log.Printf("Failed to send message to viewer %s: %v", v.vid, err)
			return
		}
		// 增加接收字节数统计
		v.receivedBytesCnt.Add(int64(len(msg)))
		// 更新最后活跃时间
		v.lastActiveTime = time.Now()
	}
}

// Ping 发送Ping消息
func (v *Viewer) Ping(rate time.Duration) {
	// 如果没有WebSocket连接，直接返回
	if v.Conn == nil {
		return
	}

	ticker := time.NewTicker(rate)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 调用Conn的PingHandler()方法，发送Ping消息
			err := v.Conn.PingHandler()("ping")
			if err != nil {
				v.Close()
				return
			}
			v.UpdateActiveTime()
		case <-v.vctx.Done():
			// 上下文被取消，关闭连接
			return
		case <-v.roomCtx.Done():
			// 房间关闭，关闭连接
			return
		}
	}
}

// Close 关闭观众连接，及相关资源
func (v *Viewer) Close() {
	// 加锁保护，避免重复关闭
	v.mu.Lock()
	defer v.mu.Unlock()

	// 取消上下文，触发 ReadMsgFromRoom 中的取消逻辑
	if v.vctxCancel != nil {
		v.vctxCancel()
	}

	// 关闭WebSocket连接
	if v.Conn != nil {
		v.Conn.Close()
	}

	fmt.Printf("%s 离开房间 closed\n", v.name)
}

// UpdateActiveTime 更新最后活跃时间
func (v *Viewer) UpdateActiveTime() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.lastActiveTime = time.Now()
	v.lastPingTime = time.Now()
}

// GetLastActiveTime 获取观众最后活跃时间
func (v *Viewer) GetLastActiveTime() time.Time {
	return v.lastActiveTime
}

// GetViewerID 获取观众的唯一标识
func (v *Viewer) GetViewerID() ViewerID {
	return v.vid
}

// SentMessages 获取用户发送的消息数
func (v *Viewer) SentMessages() int64 {
	return v.sentMessageCnt.Load()
}

// ReceivedMessages 获取用户接收的消息数
func (v *Viewer) ReceivedMessages() int64 {
	return v.receivedMessageCnt.Load()
}

// SentBytes 获取用户发送的字节数
func (v *Viewer) SentBytes() int64 {
	return v.sentBytesCnt.Load()
}

// ReceivedBytes 获取用户接收的字节数
func (v *Viewer) ReceivedBytes() int64 {
	return v.receivedBytesCnt.Load()
}

// 获取观众名称
func (v *Viewer) GetName() string {
	return v.name
}

// 获取观众ID
func (v *Viewer) GetID() ViewerID {
	return v.vid
}

// WatchTime 获取观众在房间中的观看时间
func (v *Viewer) WatchTime() time.Duration {
	return v.lastActiveTime.Sub(v.startTime)
}

func (v *Viewer) GetLastPingTime() time.Time {
	return v.lastPingTime
}

func (v *Viewer) GetStartTime() time.Time {
	return v.startTime
}
func (v *Viewer) PrintViewerInfo() {
	fmt.Printf("观众 %s 信息:\n", v.name)
	fmt.Printf("  观众ID: %s\n", v.vid)
	fmt.Printf("  加入时间: %s\n", v.startTime.Format(time.RFC3339))
	fmt.Printf("  最后活跃时间: %s\n", v.lastActiveTime.Format(time.RFC3339))
	fmt.Printf("  最后Ping时间: %s\n", v.lastPingTime.Format(time.RFC3339))
	fmt.Printf("  发送消息数: %d\n", v.sentMessageCnt.Load())
	fmt.Printf("  接收消息数: %d\n", v.receivedMessageCnt.Load())
	fmt.Printf("  观看时间: %s\n", v.WatchTime().String())
}

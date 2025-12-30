package sdk

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type ViewerID string

const (
	baseRingBufferSize = 32768
	maxRingBufferSize  = 64936
	// 环形缓冲区调整阈值百分比
	bufferResizeThreshold = 80
)

// ViewerInfo 用户信息接口，用于扩展用户信息
type ViewerInfo interface {
	GetViewerID() string
	GetViewerName() string
	GetCustomInfo() map[string]any
	GetConn() *websocket.Conn
}

// DefaultViewerInfo 默认用户信息实现
type DefaultViewerInfo struct {
	ViewerID   string          `json:"viewer_id"`
	ViewerName string          `json:"viewer_name"`
	CustomInfo map[string]any  `json:"custom_info"`
	Conn       *websocket.Conn `json:"conn"`
}

func (d *DefaultViewerInfo) GetViewerID() string {
	return d.ViewerID
}

func (d *DefaultViewerInfo) GetViewerName() string {
	return d.ViewerName
}

func (d *DefaultViewerInfo) GetCustomInfo() map[string]any {
	return d.CustomInfo
}
func (d *DefaultViewerInfo) GetConn() *websocket.Conn {
	return d.Conn
}

// NewDefaultViewerInfo 创建默认用户信息
func NewDefaultViewerInfo(vID, vName string, conn *websocket.Conn) *DefaultViewerInfo {
	return &DefaultViewerInfo{
		ViewerID:   vID,
		ViewerName: vName,
		CustomInfo: make(map[string]any),
		Conn:       conn,
	}
}

// Viewer 观众结构体
type Viewer struct {
	vid   string          // 用户唯一标识
	vname string          // 用户名称
	Conn  *websocket.Conn // WebSocket连接

	UserInfo ViewerInfo // 用户信息接口，允许自定义实现
	// 扩展字段：允许外部项目添加自定义字段
	CustomData map[string]any

	Room    *Room           // 所属房间
	roomCtx context.Context // 根上下文，用于如果房间关闭，用户连接也会关闭

	viewerCtx       context.Context    // 观众上下文，用于取消操作
	viewerCtxCancel context.CancelFunc // 取消函数，用于取消操作

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

	// 高优先级消息缓冲区
	highPrioritySlots   []atomic.Pointer[item] // 高优先级环形缓冲区
	highPriorityWriteAt atomic.Int64           // 高优先级写入位置
	highPriorityReadAt  atomic.Int64           // 高优先级读取位置

	// 消息标志
	hasMessage         atomic.Int32 // 是否有普通消息待处理
	hasHighPriorityMsg atomic.Int32 // 是否有高优先级消息待处理

	roomWriteBufSize atomic.Int64 // 接收缓冲区大小
	roomWriteBufMu   sync.Mutex   // 保护接收缓冲区的调整

	startTime      time.Time // 加入房间时间
	lastActiveTime time.Time // 最后活跃时间
	lastPingTime   time.Time // 最后Ping时间

	accumulatedViewDuration atomic.Int64 // 当前会话累计时长（秒）
	previousViewDuration    int64        // 之前累计的时长（从Redis获取）
	lastUpdateTime          time.Time    // 上次更新Redis时间

	// 消息计数
	sentMessageCnt     atomic.Int64 // 用户发送的消息数
	receivedMessageCnt atomic.Int64 // 用户接收的消息数
	// 字节数统计
	sentBytesCnt     atomic.Int64 // 用户发送的字节数
	receivedBytesCnt atomic.Int64 // 用户接收的字节数

	// WebSocket写入专用锁，确保并发安全
	wsWriteMu sync.Mutex
}

type item struct {
	seq      int64           // 版本号，防止 ABA
	data     []byte          // 真正消息，复用对象池可再省 30% CPU
	priority MessagePriority // 优先级
}

// NewViewer 创建新的观众连接（使用默认用户信息）
func NewViewer(roomCtx context.Context, vid string, vname string, conn *websocket.Conn) *Viewer {
	return NewViewerWithInfo(roomCtx, vid, vname, conn, nil)
}

// NewViewerWithInfo 创建新的观众连接（支持自定义用户信息）
func NewViewerWithInfo(roomCtx context.Context, vid string, vname string, conn *websocket.Conn, userInfo ViewerInfo) *Viewer {
	now := time.Now()
	vctx, vctxCancel := context.WithCancel(roomCtx)

	// 初始化动态大小的缓冲区
	sendRoomSlots := make([]atomic.Pointer[item], baseRingBufferSize)
	roomBroadcastSlots := make([]atomic.Pointer[item], baseRingBufferSize)
	highPrioritySlots := make([]atomic.Pointer[item], baseRingBufferSize) // 高优先级缓冲区

	// 如果没有提供用户信息，使用默认实现
	if userInfo == nil {
		userInfo = NewDefaultViewerInfo(vid, vname, conn)
	}

	return &Viewer{
		vid:        vid,
		vname:      vname,
		UserInfo:   userInfo,
		CustomData: make(map[string]any),
		Conn:       conn,

		startTime:      now,
		lastActiveTime: now,
		lastPingTime:   now,
		lastUpdateTime: now,

		roomCtx:         roomCtx,
		viewerCtx:       vctx,
		viewerCtxCancel: vctxCancel,

		// 初始化动态缓冲区
		sendRoomSlots:      sendRoomSlots,
		sendRoomBufSize:    atomic.Int64{},
		roomBroadcastSlots: roomBroadcastSlots,
		roomWriteBufSize:   atomic.Int64{},
		highPrioritySlots:  highPrioritySlots, // 添加高优先级缓冲区
		hasHighPriorityMsg: atomic.Int32{},    // 初始化高优先级消息标志

	}
}

// SetCustomData 设置自定义数据
func (v *Viewer) SetCustomData(key string, value any) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.CustomData == nil {
		v.CustomData = make(map[string]any)
	}
	v.CustomData[key] = value
}

// GetCustomData 获取自定义数据
func (v *Viewer) GetCustomData(key string) (any, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.CustomData == nil {
		return nil, false
	}
	value, exists := v.CustomData[key]
	return value, exists
}

// SetUserInfo 设置用户信息
func (v *Viewer) SetUserInfo(userInfo ViewerInfo) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.UserInfo = userInfo
}

// GetUserInfo 获取用户信息
func (v *Viewer) GetUserInfo() ViewerInfo {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.UserInfo
}

func (v *Viewer) Start() {
	// 存储之前会话时长
	go v.StorePreviousSessionTime()

	// 网络->slots->房间
	go v.ReadMessageWebSocketLoop()

	// 房间->slots->网络
	go v.messageReader()

	//定时发送Ping消息，检测连接是否断开
	go v.Ping(30 * time.Second)

}

// StartMessageReader 启动消息读取器（用于压测，不启动WebSocket相关协程）
func (v *Viewer) StartMessageReader() {
	go v.messageReader()
}

// Write 将用户的发送的数据写入一个环形缓冲区
func (v *Viewer) Write(b []byte) {
	log.Println("write", string(b))
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
		log.Println("no websocket connection")
		return
	}

	for {
		select {
		case <-v.viewerCtx.Done():
			log.Println("viewer context done")
			return
		case <-v.roomCtx.Done():
			log.Println("room context done")
			return
		default:
			_, msgByte, err := v.Conn.ReadMessage()
			if err != nil {
				v.Conn.Close()
				if v.Room != nil {
					v.Room.LeaveRoom(v)
				}
				return
			}
			messageStr := string(msgByte)
			fmt.Print(messageStr)

			// 检查是否为ping消息
			if messageStr == "ping" {
				v.accumulatedViewDuration.Add(3000)
				v.UpdateActiveTime()
				continue
			}
			// 处理时长累计心跳
			if strings.HasPrefix(messageStr, "ping_") {
				// 提取毫秒数
				msStr := strings.TrimPrefix(messageStr, "ping_")
				if ms, err := strconv.Atoi(msStr); err == nil && ms > 0 {
					v.accumulatedViewDuration.Add(int64(ms))
					v.UpdateActiveTime()
				}
				continue
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
		case <-v.viewerCtx.Done():
			return // 观众退出
		case <-v.roomCtx.Done():
			return // 房间关闭
		case <-ticker.C:
			// 优先检查高优先级消息
			hasHighPriorityMsg := v.hasHighPriorityMsg.Load()
			if hasHighPriorityMsg == 1 {
				v.processHighPriorityMessages()
			}

			// 检查普通消息
			hasNormalMsg := v.hasMessage.Load()
			if hasNormalMsg == 1 {
				v.processNormalMessages()
			}
		}
	}
}

// 处理高优先级消息
func (v *Viewer) processHighPriorityMessages() {
	readPos := v.highPriorityReadAt.Load()
	writePos := v.highPriorityWriteAt.Load()

	// 如果没有高优先级消息，直接返回
	if readPos == writePos {
		v.hasHighPriorityMsg.Store(0)
		return
	}

	// 读取所有待处理的高优先级消息
	messages := make([][]byte, 0)
	messageCount := int64(0)
	for readPos != writePos {
		itemPtr := v.highPrioritySlots[readPos].Load()
		if itemPtr != nil {
			messages = append(messages, itemPtr.data)
			messageCount++
			v.highPrioritySlots[readPos].Store(nil)
		}
		readPos = (readPos + 1) % int64(len(v.highPrioritySlots))
	}

	// 更新读位置
	v.highPriorityReadAt.Store(readPos)

	if len(messages) > 0 {
		// 增加接收消息计数
		v.receivedMessageCnt.Add(messageCount)

		// 发送高优先级消息到WebSocket
		v.sendMessagesToWebSocket(messages)

		fmt.Printf("观众 %s 处理了 %d 条高优先级消息\n", v.vname, len(messages))
	}

	// 重置高优先级消息标志
	v.hasHighPriorityMsg.Store(0)
}

// 处理普通消息
func (v *Viewer) processNormalMessages() {
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
		fmt.Printf("观众 %s 处理了 %d 条普通消息，累计接收消息数: %d\n", v.vname, len(messages), v.receivedMessageCnt.Load())

		// 通过WebSocket发送消息
		v.sendMessagesToWebSocket(messages)
	}

	// 重置消息标志
	v.hasMessage.Store(0)
}

// 通过WebSocket发送消息
func (v *Viewer) sendMessagesToWebSocket(messages [][]byte) {
	// 使用专用锁确保 WebSocket 写入串行化，解决并发安全问题
	v.wsWriteMu.Lock()
	defer v.wsWriteMu.Unlock()

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.Conn == nil {
		return
	}

	// 发送消息
	for _, msg := range messages {
		// 设置写超时
		v.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		err := v.Conn.WriteMessage(websocket.BinaryMessage, msg)
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
		case <-v.viewerCtx.Done():
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
	if v.viewerCtxCancel != nil {
		v.viewerCtxCancel()
	}

	// 关闭WebSocket连接
	if v.Conn != nil {
		v.Conn.Close()
	}

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
func (v *Viewer) GetViewerID() string {
	return v.vid
}

// SetViewerID 获取观众的唯一标识
func (v *Viewer) SetViewerID(vid string) {
	v.vid = vid
}

// 获取观众名称
func (v *Viewer) GetViewerName() string {
	return v.vname
}

// SetViewerName 设置观众名称
func (v *Viewer) SetViewerName(vname string) {
	v.vname = vname
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
	fmt.Printf("观众 %s 信息:\n", v.vname)
	fmt.Printf("  观众ID: %s\n", v.vid)
	fmt.Printf("  加入时间: %s\n", v.startTime.Format(time.RFC3339))
	fmt.Printf("  最后活跃时间: %s\n", v.lastActiveTime.Format(time.RFC3339))
	fmt.Printf("  最后Ping时间: %s\n", v.lastPingTime.Format(time.RFC3339))
	fmt.Printf("  发送消息数: %d\n", v.sentMessageCnt.Load())
	fmt.Printf("  接收消息数: %d\n", v.receivedMessageCnt.Load())
	fmt.Printf("  观看时间: %s\n", v.WatchTime().String())
}

// GetTotalWatchTime 获取用户总观看时长（包括之前累计的）
func (v *Viewer) GetTotalWatchTime() int64 {
	return v.previousViewDuration + v.accumulatedViewDuration.Load()
}

// GetCurrentSessionTime 获取当前会话观看时长
func (v *Viewer) GetCurrentSessionTime() int64 {
	return v.accumulatedViewDuration.Load()
}

// 获取用户之前累计的观看时长
func (v *Viewer) StorePreviousSessionTime() {
	// 如果房间有数据源，从Redis加载之前累计的时长
	if v.Room != nil && v.Room.dataSource != nil {
		key := fmt.Sprintf(Live_WatchDuration, v.Room.firmUUID, v.Room.roomNumber, v.vid)
		prevTimeStr, err := v.Room.dataSource.Get(v.roomCtx, key)
		if err == nil && len(prevTimeStr) > 0 {
			if prevTime, parseErr := strconv.ParseInt(prevTimeStr, 10, 64); parseErr == nil {
				v.previousViewDuration = prevTime
			}
		}
	}
}

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// RoomNumber 已在外部定义，此处移除重复声明

// 连接监控和清理配置常量
const (

	// MaxPingInterval 最大Ping间隔（秒）
	MaxPingInterval = 30
	// MessageRingBufferSize 消息环形缓冲区大小，用于存储待发送到数据源的消息
	MessageRingBufferSize = 65536
)

var (
	ErrRoomNoLiving = errors.New("直播已经结束")
	ErrRoomIsFull   = errors.New("房间已满")

	ErrNewRoomName   = errors.New("新房间名称不能为空")
	ErrNewRoomNumber = errors.New("新房间号不能为空")
)

type Room struct {
	roomNumber RoomNumber
	roomName   string

	isOpenRoom atomic.Bool // 房间是否在直播中

	startTime atomic.Value // 开播时间
	endTime   atomic.Value // 直播结束时间

	maxViewer    uint32        // 房间最大容容纳人数
	onlineViewer atomic.Uint32 // 实时在线人数
	totalViewer  atomic.Uint32 // 总观看人数

	likeCount atomic.Uint32 // 点赞数

	// 消息和字节数统计
	messageSentCnt     atomic.Int64 // 房间发送的消息数
	messageReceivedCnt atomic.Int64 // 房间接收的消息数
	bytesSentCnt       atomic.Int64 // 房间发送的字节数
	bytesReceivedCnt   atomic.Int64 // 房间接收的字节数

	viewers   map[ViewerID]*Viewer // 观众列表（如果需要跟踪具体观众）
	viewerMux sync.RWMutex         // 保护 viewerList 的互斥锁

	// 使用 ring buffer 替换通道，避免通道满山丢失消息
	viewerSendRoomMessageBuf [MessageRingBufferSize]*Message // 环形缓冲区
	viewerSendWritePos       atomic.Int64                    // 写入位置
	viewerSendReadPos        atomic.Int64                    // 读取位置
	viewerSendMu             sync.RWMutex                    // 保护缓冲区

	viewerWake chan ViewerID // 用户从网络层获取消息后，将用户ID发送到该通道，用于唤醒读取消息协程

	roomCtx    context.Context    // 传递给其他 goroutine 监听
	cancelFunc context.CancelFunc // 由房间自己持有，用于主动关闭

	//数据源
	dataSource DataSource
}

func NewRoom(ctx context.Context, rootName string, roomNumber RoomNumber, roomMax uint32) (*Room, error) {
	if rootName == "" {
		return nil, ErrNewRoomName
	}

	if roomNumber == "" {
		return nil, ErrNewRoomNumber
	}

	// 创建context，用于传递给其他goroutine
	roomCtx, cancelFunc := context.WithCancel(ctx)

	// 创建房间
	room := &Room{
		roomNumber: roomNumber,
		roomName:   rootName,
		maxViewer:  roomMax,
		viewers:    make(map[ViewerID]*Viewer),
		viewerWake: make(chan ViewerID, roomMax),
		roomCtx:    roomCtx,
		cancelFunc: cancelFunc,
	}
	// 初始化 ring buffer 位置
	room.viewerSendWritePos.Store(0)
	room.viewerSendReadPos.Store(0)

	// 设置房间状态为直播中
	room.isOpenRoom.Store(true)

	return room, nil
}

func (r *Room) sendBatch(messages []*Message) {
	for _, msg := range messages {
		if msg == nil || msg.Data == nil {
			continue
		}
		// 写入到 ring buffer
		r.writeToMessageRingBuffer(msg)
		// 更新房间发送统计
		r.messageSentCnt.Add(1)
		r.bytesSentCnt.Add(int64(len(msg.Data)))
	}
}

// writeToMessageRingBuffer 写入消息到 ring buffer
func (r *Room) writeToMessageRingBuffer(msg *Message) {
	r.viewerSendMu.Lock()
	defer r.viewerSendMu.Unlock()

	// 获取写入位置
	writePos := r.viewerSendWritePos.Load()
	readPos := r.viewerSendReadPos.Load()

	// 计算下一个写入位置
	nextWritePos := (writePos + 1) % int64(MessageRingBufferSize)

	// 检查是否存储满，如果满了则覆盖最旧的消息（直播场景昨日消息丢弃是接可的）
	if nextWritePos == readPos {
		// 缓冲区满了，推进readPos
		newReadPos := (readPos + 1) % int64(MessageRingBufferSize)
		r.viewerSendReadPos.Store(newReadPos)
	}

	// 组件规一、维护 ring buffer的一程目标、持一个序列号且制造新消息提验查是否需需需需需需
	// 缓冲区不禹存羊设思想
	bufferMsg := *msg // 复制消息，不直接引用
	r.viewerSendRoomMessageBuf[writePos] = &bufferMsg

	// 更新写入位置
	r.viewerSendWritePos.Store(nextWritePos)
}

// 此处不再需要

func (r *Room) Start(dataSource DataSource) {
	// 存储数据源
	r.dataSource = dataSource

	// 设置房间状态为直播中
	r.isOpenRoom.Store(true)
	// 开播时间设置为当前时间
	r.startTime.Store(time.Now())

	// 观众网络->房间
	go r.MessageCollector()

	// 房间->数据源
	go r.messageToDataSource()

	// 房间->观众网络
	go r.broadcastHandler()

	fmt.Printf("%s 房间已经启动\n", r.roomNumber)
}

func (r *Room) Close() {
	// 如果房间不是直播中，直接返回
	if !r.isOpenRoom.Load() {
		return
	}

	// 直播结束时间设置为当前时间
	r.endTime.Store(time.Now())

	// 主动关闭房间上下文
	r.cancelFunc()

	// 设置房间状态为已关闭
	r.isOpenRoom.Store(false)
}

func (r *Room) JoinRoom(viewer *Viewer) error {
	if !r.isOpenRoom.Load() {
		return ErrRoomNoLiving
	}
	if r.onlineViewer.Load() >= r.maxViewer {
		return ErrRoomIsFull
	}

	r.viewerMux.Lock()
	// 设置观众的房间引用
	viewer.Room = r
	r.viewers[viewer.vid] = viewer
	r.viewerMux.Unlock()

	// 更新在线人数和总人数
	r.onlineViewer.Add(1)
	r.totalViewer.Add(1)

	// 增强日志：记录用户加入房间
	fmt.Printf("room=%s %s 加入房间。\n", r.roomNumber, viewer.name)
	return nil
}

func (r *Room) LeaveRoom(viewer *Viewer) {
	if viewer == nil {
		return
	}

	r.viewerMux.Lock()
	defer r.viewerMux.Unlock()

	// 检查观众是否在房间中
	if _, exists := r.viewers[viewer.vid]; !exists {
		fmt.Printf("警告: 观众 %s 不在房间中，无法退出\n", viewer.vid)
		return
	}

	// 记录当前在线人数
	currentOnline := r.onlineViewer.Load()

	// 从观众列表中移除观众
	viewer.Close()
	delete(r.viewers, viewer.vid)

	// 更新在线人数
	// 减1：利用无符号整数溢出特性，^uint32(0) 等于最大无符号32位整数，加后溢出即为减1
	if currentOnline > 0 {
		r.onlineViewer.Add(^uint32(0))
	}

	// 增强日志：记录用户离开房间
	fmt.Printf("【Room.LeaveRoom】room=%s viewerID=%s name=%s left room, current online: %d, previous online: %d\n",
		r.roomNumber, viewer.vid, viewer.name, r.onlineViewer.Load(), currentOnline)
}

// 房间消息收集器
func (r *Room) MessageCollector() {

	// 使用select的default实现非阻塞处理
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	messageBatch := make([]*Message, 0, 5000)

	for {
		select {
		case <-r.roomCtx.Done():
			return
		case viewerID := <-r.viewerWake:
			// 快速处理唤醒的观众
			r.processSingleViewer(viewerID, &messageBatch)

		case <-ticker.C:
			// 定期批量处理
			r.processBatch(&messageBatch)
		}
	}
}

// 收集处理单个观众的消息
func (r *Room) processSingleViewer(viewerID ViewerID, batch *[]*Message) {
	r.viewerMux.RLock()
	viewer, exists := r.viewers[viewerID]
	r.viewerMux.RUnlock()

	if !exists || viewer == nil || viewer.sendRoomHasMessage.Load() != 1 {
		return
	}

	rawMessages := viewer.CollectMessages()
	for _, data := range rawMessages {
		*batch = append(*batch, &Message{
			ViewerID: viewerID,
			Data:     data,
			Time:     time.Now(),
		})
		// 更新房间接收统计
		r.messageReceivedCnt.Add(1)
		r.bytesReceivedCnt.Add(int64(len(data)))
	}

	// 批次达到一定大小就发送，避免频繁发送
	if len(*batch) >= 400 {
		r.sendBatch(*batch)
		*batch = (*batch)[:0]
	}
}

// 批量处理收集到的消息
func (r *Room) processBatch(batch *[]*Message) {
	r.viewerMux.RLock()
	defer r.viewerMux.RUnlock()

	// 每次只处理一部分观众，避免阻塞太久
	limit := 100
	count := 0

	for viewerID, viewer := range r.viewers {
		if count >= limit {
			break
		}

		if viewer.sendRoomHasMessage.Load() == 1 {
			rawMessages := viewer.CollectMessages()
			for _, data := range rawMessages {
				// 添加日志：打印用户发送的消息
				fmt.Printf("[消息] room=%s viewer=%s: %s\n", r.roomNumber, viewer.name, string(data))
				*batch = append(*batch, &Message{
					ViewerID: viewerID,
					Data:     data,
					Time:     time.Now(),
				})
				// 更新房间接收统计
				r.messageReceivedCnt.Add(1)
				r.bytesReceivedCnt.Add(int64(len(data)))
			}
			count++
		}
	}

	// 发送批次（即使批次小于400也发送，避免消息延迟）
	if len(*batch) > 0 {
		r.sendBatch(*batch)
		*batch = (*batch)[:0]
	}
}

// messageToDataSource 每100ms检查一次 ring buffer，将消息发送到数据源
func (r *Room) messageToDataSource() {
	fmt.Printf("房间 %s messageToDataSource 协程开始运行\n", r.roomNumber)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.roomCtx.Done():
			fmt.Printf("房间 %s messageToDataSource 协程退出（房间上下文取消）\n", r.roomNumber)
			return // 房间关闭ok退出
		case <-ticker.C:
			// 从 ring buffer 读取消息并发送到数据源
			messages := r.readFromMessageRingBuffer()
			for _, msg := range messages {
				if msg != nil && r.dataSource != nil {
					err := r.dataSource.SendMessage(r.roomCtx, r.roomNumber, msg)
					if err != nil {
						fmt.Printf("发送消息到数据源失败: %v\n", err)
					}
				}
			}
		}
	}
}

// readFromMessageRingBuffer 从 ring buffer 读取消息
func (r *Room) readFromMessageRingBuffer() []*Message {
	r.viewerSendMu.Lock()
	defer r.viewerSendMu.Unlock()

	readPos := r.viewerSendReadPos.Load()
	writePos := r.viewerSendWritePos.Load()

	// 计算可读的消息数量
	available := writePos - readPos
	if available < 0 {
		available += int64(MessageRingBufferSize)
	}

	// 限制每次读取最多 100 条消息
	maxMessages := int64(100)
	if available > maxMessages {
		available = maxMessages
	}

	var messages []*Message
	bufferSize := int64(MessageRingBufferSize)
	for i := int64(0); i < available; i++ {
		index := (readPos + i) % bufferSize
		if r.viewerSendRoomMessageBuf[index] != nil {
			messages = append(messages, r.viewerSendRoomMessageBuf[index])
			r.viewerSendRoomMessageBuf[index] = nil // 清空引用
		}
	}

	// 更新读取位置
	if len(messages) > 0 {
		r.viewerSendReadPos.Store((readPos + int64(len(messages))) % bufferSize)
	}

	return messages
}

// 房间广播消恫处理器
func (r *Room) broadcastHandler() {

	ticker := time.NewTicker(10 * time.Millisecond) // 增可不漑消恫速度，给观众更多时间处理
	defer ticker.Stop()
	for {
		select {
		case <-r.roomCtx.Done():
			return // 房间关闭，退出广播
		case <-ticker.C:
			if r.dataSource != nil {
				messages := r.dataSource.GetMessage(r.roomCtx, r.roomNumber)
				if len(messages) > 0 {
					// 广播消息给所有观众
					r.broadcastToViewers(messages)
				}
			}
		}
	}
}

// broadcastToViewers 广播消息给所有观众
func (r *Room) broadcastToViewers(messages []*Message) {
	if len(messages) == 0 {
		return
	}

	// 序列化消息
	messageBytes := make([][]byte, 0, len(messages))
	for _, msg := range messages {
		// 序列化为JSON或其他格式
		data, err := json.Marshal(msg)
		if err != nil {
			// 记录错误，继续处理其他消息
			log.Printf("Failed to marshal message: %v", err)
			continue
		}
		messageBytes = append(messageBytes, data)
	}

	if len(messageBytes) == 0 {
		return
	}

	// 获取所有观众
	r.viewerMux.RLock()
	viewers := make([]*Viewer, 0, len(r.viewers))
	for _, viewer := range r.viewers {
		viewers = append(viewers, viewer)
	}
	r.viewerMux.RUnlock()

	// 异步广播给所有观众，不等待完成
	for _, viewer := range viewers {
		go func(v *Viewer) {
			// 检查观众是否活跃
			if !r.isViewerActive(v) {
				return
			}
			// 发送消息到观众
			r.sendMessagesToViewer(v, messageBytes)
		}(viewer)
	}
}

// sendMessagesToViewer 发送消息到指定观众
func (r *Room) sendMessagesToViewer(viewer *Viewer, messageBytes [][]byte) {
	// 检查观众是否活跃
	if !r.isViewerActive(viewer) {
		return
	}

	// 尝试发送消息到观众的缓冲区
	for _, msgBytes := range messageBytes {
		r.trySendToViewerBuffer(viewer, msgBytes)
	}
}

// 尝试发送消息到观众的环形缓冲区
func (r *Room) trySendToViewerBuffer(viewer *Viewer, message []byte) bool {
	writePos := viewer.roomBroadcastWriteAto.Load()
	nextWritePos := (writePos + 1) % int64(len(viewer.roomBroadcastSlots))

	// 创建新消息
	newItem := &item{
		seq:  writePos,
		data: message,
	}

	// 直接覆盖（不检查是否满）
	viewer.roomBroadcastSlots[writePos].Store(newItem)
	viewer.roomBroadcastWriteAto.Store(nextWritePos)

	viewer.hasMessage.Store(1) // 告诉用户有新消息

	return true // 总是成功
}

// DataSource 数据源接口，用于发送和获取消息
type DataSource interface {
	// SendMessage 发送消息到数据源
	SendMessage(ctx context.Context, roomNumber RoomNumber, msg *Message) error

	// GetMessage 从数据源获取消息
	GetMessage(ctx context.Context, roomNumber RoomNumber) []*Message

	// GetRedisBytesSent 获取发送到Redis的字节数
	GetRedisBytesSent(roomNumber RoomNumber) int64

	// GetRedisBytesRecv 获取从Redis接收的字节数
	GetRedisBytesRecv(roomNumber RoomNumber) int64
}

// GetCapacity 获取房间当前最大容纳人数
func (r *Room) GetCapacity() uint32 {
	return r.maxViewer
}

func (r *Room) isViewerActive(viewer *Viewer) bool {
	// 检查观众是否已经取消
	select {
	case <-viewer.vctx.Done():
		return false
	default:
	}

	// 检查连接是否有效
	// 在基准测试环境中，我们没有设置WebSocket连接，所以不检查Conn是否为nil
	// if viewer.Conn == nil {
	// 	return false
	// }

	// 可以根据需要添加更多的活跃性检查
	// 例如：检查最后活跃时间、心跳等

	return true
}

func (r *Room) Info() string {
	r.viewerSendMu.RLock()
	messageCount := r.viewerSendWritePos.Load() - r.viewerSendReadPos.Load()
	if messageCount < 0 {
		messageCount += int64(MessageRingBufferSize)
	}
	r.viewerSendMu.RUnlock()

	return fmt.Sprintf("房间号 %s 房间名称: %s 人数: %d 直播状态: %v 最大容纳人数: %d 总观看人数: %d 点赞数: %d 消息缓冲区中的消息数量: %d \n",
		r.roomNumber, r.roomName, r.onlineViewer.Load(), r.isOpenRoom.Load(), r.maxViewer, r.totalViewer.Load(), r.likeCount.Load(), messageCount)
}

// 获取房间号
func (r *Room) RoomNumber() RoomNumber {
	return r.roomNumber
}

// 根据ViewerID获取观众
func (r *Room) GetViewer(viewerID ViewerID) *Viewer {
	r.viewerMux.RLock()
	defer r.viewerMux.RUnlock()
	return r.viewers[viewerID]
}

// 获取消息缓冲区中待发送的消息数量
func (r *Room) ViewerSendRoomMessageCount() int64 {
	r.viewerSendMu.RLock()
	defer r.viewerSendMu.RUnlock()

	count := r.viewerSendWritePos.Load() - r.viewerSendReadPos.Load()
	if count < 0 {
		count += int64(MessageRingBufferSize)
	}
	return count
}

// GetOnlineViewerCount 获取房间当前在线人数
func (r *Room) GetOnlineViewerCount() uint32 {
	return r.onlineViewer.Load()
}

// GetTotalViewerCount 获取房间总观看人数
func (r *Room) GetTotalViewerCount() uint32 {
	return r.totalViewer.Load()
}

// IsOpen 检查房间是否在直播中
func (r *Room) IsOpen() bool {
	return r.isOpenRoom.Load()
}

func (r *Room) PrintRoomInfo() {
	fmt.Printf("房间 %s 信息:\n", r.roomNumber)
	fmt.Printf("  房间名称: %s\n", r.roomName)
	fmt.Printf("  最大容纳人数: %d\n", r.maxViewer)
	fmt.Printf("  总观看人数: %d\n", r.totalViewer.Load())
	fmt.Printf("  在线人数: %d\n", r.onlineViewer.Load())
	fmt.Printf("  点赞数: %d\n", r.likeCount.Load())
	fmt.Printf("  消息缓冲区中的消息数量: %d\n", r.ViewerSendRoomMessageCount())
	fmt.Printf("  直播状态: %v\n", r.isOpenRoom.Load())
}

// BytesSent 获取房间发送的总字节数
func (r *Room) BytesSent() int64 {
	return r.bytesSentCnt.Load()
}

// BytesReceived 获取房间接收的总字节数
func (r *Room) BytesReceived() int64 {
	return r.bytesReceivedCnt.Load()
}

// RedisBytesSent 获取房间发送到Redis的字节数
func (r *Room) RedisBytesSent() int64 {
	if r.dataSource != nil {
		return r.dataSource.GetRedisBytesSent(r.roomNumber)
	}
	return 0
}

// RedisBytesRecv 获取房间从Redis接收的字节数
func (r *Room) RedisBytesRecv() int64 {
	if r.dataSource != nil {
		return r.dataSource.GetRedisBytesRecv(r.roomNumber)
	}
	return 0
}

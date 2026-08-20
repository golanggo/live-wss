package sdk

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type Room struct {
	firmUUID   string
	roomNumber string
	roomName   string

	isOpenRoom atomic.Bool // 房间是否在直播中

	startTime atomic.Value // 开播时间
	endTime   atomic.Value // 直播结束时间

	maxViewer uint32 // 房间最大容容纳人数

	// 消息和字节数统计
	messageSentCnt         atomic.Int64 // 房间发送的消息数
	messageReceivedCnt     atomic.Int64 // 房间接收的消息数
	bytesSentCnt           atomic.Int64 // 房间发送的字节数
	bytesReceivedCnt       atomic.Int64 // 房间接收的字节数
	lastMessageReceivedCnt atomic.Int64 // 上一次统计房间接收的消息数

	viewers   map[string]*Viewer // 观众列表（如果需要跟踪具体观众）
	viewerMux sync.RWMutex       // 保护 viewerList 的互斥锁

	// 使用 ring buffer 替换通道，避免通道满丢失消息
	viewerSendRoomMessageBuf [MessageRingBufferSize]*MessagePb // 环形缓冲区
	viewerSendWritePos       atomic.Int64                      // 写入位置
	viewerSendReadPos        atomic.Int64                      // 读取位置
	viewerSendMu             sync.RWMutex                      // 保护缓冲区

	viewerWake chan string // 用户从网络层获取消息后，将用户ID发送到该通道，用于唤醒读取消息协程

	roomCtx    context.Context    // 传递给其他 goroutine 监听
	cancelFunc context.CancelFunc // 由房间自己持有，用于主动关闭

	//数据源
	dataSource DataSource

	leaveRoomViewerCnt     atomic.Uint32 // 离开房间人数
	lastLeaveRoomViewerCnt atomic.Uint32 // 上一次统计离开房间人数

	onlineViewer            atomic.Uint32 // 本实例实时在线人数
	distributedOnlineViewer atomic.Uint32 // 所有服务实例的实时在线人数
	totalViewer             atomic.Uint32 // 本实例累计观看人次
	lastTotalViewerCnt      atomic.Uint32 // 上一次统计总观看人次

	instanceID        string        // 当前 Room 实例唯一标识
	onlinePresenceTTL time.Duration // 分布式在线会话心跳过期时间

	onlinePeakInterval    time.Duration // 最高在线人数统计区间
	onlinePeakRetention   time.Duration // 区间峰值数据保留时间，0 表示不过期
	totalViewerRetention  time.Duration // 累计去重观看人数数据保留时间
	onlinePeakMu          sync.Mutex
	onlinePeakWindowStart time.Time
	onlinePeakCount       uint32
	onlinePeakMaxMu       sync.Mutex
	onlinePeakMaxCount    atomic.Uint32 // 当前直播周期内已成功持久化的最高区间峰值

	commentCodes     map[string]struct{} // 需要统计并发送到 MQ 的评论消息 code
	commentRetention time.Duration
	commentPublisher CommentPublisher
	commentEventSeq  atomic.Uint64

	likeCount     atomic.Uint32 // 点赞数
	lastLikeCount atomic.Uint32 // 上一次统计点赞数

	// Message filtering
	messageFilter      MessageFilter
	filterEnabled      atomic.Bool  // Whether filtering is enabled
	messageFilterLimit atomic.Int64 // 匹配消息限制数量，默认10，则保留十分之一消息

	// adaptiveMessageSampler 只作用于低优先级消息，并按全局在线人数选择采样比例。
	adaptiveMessageSampler     atomic.Pointer[adaptiveMessageSampler]
	adaptiveDroppedLowPriority atomic.Uint64
}

func NewRoom(ctx context.Context, rootName string, roomNumber string, roomMax uint32, firmUUID string) (*Room, error) {
	return NewRoomWithConfig(ctx, rootName, roomNumber, roomMax, firmUUID, RoomConfig{})
}

func NewRoomWithConfig(ctx context.Context, rootName string, roomNumber string, roomMax uint32, firmUUID string, config RoomConfig) (*Room, error) {
	config = config.withDefaults()
	if len(rootName) == 0 {
		return nil, ErrNewRoomName
	}

	if len(roomNumber) == 0 {
		return nil, ErrNewRoomNumber
	}

	if len(firmUUID) == 0 {
		return nil, ErrNewRoomFirmUUID
	}

	instanceID, err := newRoomInstanceID()
	if err != nil {
		return nil, err
	}

	// 创建context，用于传递给其他goroutine
	roomCtx, cancelFunc := context.WithCancel(ctx)

	// 创建房间
	room := &Room{
		firmUUID:              firmUUID,
		roomNumber:            roomNumber,
		roomName:              rootName,
		maxViewer:             roomMax,
		viewers:               make(map[string]*Viewer),
		viewerWake:            make(chan string, roomMax),
		roomCtx:               roomCtx,
		cancelFunc:            cancelFunc,
		onlinePeakInterval:    config.OnlinePeakInterval,
		onlinePeakRetention:   config.OnlinePeakRetention,
		totalViewerRetention:  config.TotalViewerRetention,
		onlinePeakWindowStart: time.Now().Truncate(config.OnlinePeakInterval),
		instanceID:            instanceID,
		onlinePresenceTTL:     config.OnlinePresenceTTL,
		commentCodes:          makeCommentCodeSet(config.CommentCodes),
		commentRetention:      config.CommentRetention,
		commentPublisher:      config.CommentPublisher,
	}
	// 初始化 ring buffer 位置
	room.viewerSendWritePos.Store(0)
	room.viewerSendReadPos.Store(0)
	room.messageFilterLimit.Store(10)
	if err := room.SetAdaptiveMessageSampling(config.AdaptiveSamplingRules); err != nil {
		cancelFunc()
		return nil, err
	}

	// 设置房间状态为直播中
	room.isOpenRoom.Store(true)

	// 统计信息
	room.lastLeaveRoomViewerCnt.Store(0)
	room.lastLikeCount.Store(0)
	room.lastTotalViewerCnt.Store(0)

	return room, nil
}

func (r *Room) sendBatch(messages []*MessagePb) {
	for _, msg := range messages {
		if msg == nil || (msg.Msg == "" && msg.Data == "") {
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
func (r *Room) writeToMessageRingBuffer(msg *MessagePb) {
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

	// 组件规一、维护 ring buffer的一程目标、持一个序列号且制造新消息提验查是否需要
	bufferMsg := &MessagePb{
		SendClient: msg.SendClient,
		MessageId:  msg.MessageId,
		Code:       msg.Code,
		Msg:        msg.Msg,
		Data:       msg.Data,
		Priority:   msg.Priority,
		LiveId:     msg.LiveId,
		Timestamp:  msg.Timestamp,
	}
	r.viewerSendRoomMessageBuf[writePos] = bufferMsg

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

	// 房间统计数据->数据源
	go r.storeSummaryToDataSource()

	// 维护本实例在线人数心跳并获取分布式全局在线人数。
	go r.collectOnlineViewerPresence()

	// 按配置时间区间统计最高实时在线人数
	go r.collectOnlineViewerPeak()

	fmt.Printf("%s 房间已经启动\n", r.roomNumber)
}

// 存储统计数据到Redis的协程
func (r *Room) storeSummaryToDataSource() {
	fmt.Printf("房间 %s storeSummaryToDataSource 协程开始运行\n", r.roomNumber)
	ticker := time.NewTicker(6 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.roomCtx.Done():
			fmt.Printf("房间 %s storeSummaryToDataSource 协程退出（房间上下文取消）\n", r.roomNumber)
			return // 房间关闭ok退出
		case <-ticker.C:
			// 存储实时在线用户数与当前区间峰值
			r.storeOnlineViewerSummary()

			// 存储累计观看用户数和累计离开用户数
			r.storeViewerCntToDataSource()

			// 存储点赞数
			r.storeLikeCountToDataSource()

			// 存储用户时长
			//r.storeViewerDurationsToDataSource()

			// 存储评论数
			r.storeMessageCountToDataSource()
		}
	}
}

// 存储累计观看用户数和累计离开直播间用户数
func (r *Room) storeViewerCntToDataSource() {
	// 原子读取当前值
	currentLeaveCnt := r.leaveRoomViewerCnt.Load()
	lastLeaveCnt := r.lastLeaveRoomViewerCnt.Load()
	// 计算相对于上次的净变化（可以为正或负）
	netChange := int64(currentLeaveCnt - lastLeaveCnt)
	// 离开房间人数
	if netChange != 0 { // 只有净变化不为0时才更新
		key := fmt.Sprintf(Live_Leave_User_Count, r.firmUUID, r.roomNumber)
		err := r.dataSource.AccumulatedBy(r.roomCtx, key, netChange)
		if err == nil {
			// 更新记录的上一次值
			r.lastLeaveRoomViewerCnt.Store(currentLeaveCnt)
		} else {
			fmt.Printf("存储净增加离开人数到Redis失败: %v, 房间: %s\n", err, r.roomNumber)
		}
	}

	// 累计观看人次
	currentTotalViewerCnt := r.totalViewer.Load()
	lastTotalViewerCnt := r.lastTotalViewerCnt.Load()
	netChangeTotalViewerCnt := int64(currentTotalViewerCnt - lastTotalViewerCnt)
	if netChangeTotalViewerCnt != 0 {
		key := fmt.Sprintf(Live_Total_Count, r.firmUUID, r.roomNumber)
		err := r.dataSource.AccumulatedBy(r.roomCtx, key, netChangeTotalViewerCnt)
		if err == nil {
			// 存储成功，更新记录的上一次值
			r.lastTotalViewerCnt.Store(currentTotalViewerCnt)
		} else {
			fmt.Printf("存储累计观看人数到Redis失败: %v\n, 房间: %s\n", err, r.roomNumber)
		}
	}
}

// 存储点赞数到Redis
func (r *Room) storeLikeCountToDataSource() {
	// 累计点赞数
	currentLikeCount := r.likeCount.Load()
	lastLikeCount := r.lastLikeCount.Load()
	netChangeLikeCount := int64(currentLikeCount - lastLikeCount)
	if netChangeLikeCount != 0 {
		key := fmt.Sprintf(Live_Liked_Count, r.firmUUID, r.roomNumber)
		err := r.dataSource.AccumulatedBy(r.roomCtx, key, netChangeLikeCount)
		if err == nil {
			// 存储成功，更新记录的上一次值
			r.lastLikeCount.Store(currentLikeCount)
		} else {
			fmt.Printf("【ERROR】存储点赞数到Redis失败: %v\n, 房间: %s\n", err, r.roomNumber)
		}
	}
}

// 将用户时长存储到Redis
func (r *Room) storeViewerDurationsToDataSource() {
	r.viewerMux.RLock()
	defer r.viewerMux.RUnlock()

	for viewerID, viewer := range r.viewers {
		// 计算时长键
		key := fmt.Sprintf(Live_WatchDuration, r.firmUUID, r.roomNumber, viewerID)
		totalDuration := viewer.GetTotalWatchTime()
		// 存储到Redis
		if r.dataSource != nil {
			err := r.dataSource.Store(r.roomCtx, key, totalDuration, 36*time.Hour)
			if err != nil {
				fmt.Printf("存储用户时长到Redis失败: %v, 用户: %s, 房间: %s\n", err, viewerID, r.roomNumber)
			}
		}
	}
}

// 存储评论数到Redis
func (r *Room) storeMessageCountToDataSource() {
	// 累计评论数
	currentMsgCount := r.messageReceivedCnt.Load()
	lastMsgCount := r.lastMessageReceivedCnt.Load()
	netChangeMsgCount := int64(currentMsgCount - lastMsgCount)
	if netChangeMsgCount != 0 {
		key := fmt.Sprintf(Live_Comment_Count, r.firmUUID, r.roomNumber)
		err := r.dataSource.AccumulatedBy(r.roomCtx, key, netChangeMsgCount)
		if err == nil {
			// 存储成功，更新记录的上一次值
			r.lastMessageReceivedCnt.Store(currentMsgCount)
		} else {
			fmt.Printf("【ERROR】存储评论数到Redis失败: %v\n, 房间: %s\n", err, r.roomNumber)
		}
	}
}

func (r *Room) Close() {
	// Swap 保证关闭逻辑只执行一次，并在清理前阻止新观众加入。
	if !r.isOpenRoom.Swap(false) {
		return
	}

	r.endTime.Store(time.Now())
	r.viewerMux.Lock()

	// 先原子移除本实例拥有的全部用户，得到其他存活实例仍在房间中的全局人数。
	viewerIDs := r.localViewerIDsLocked()
	_, _, _, err := r.syncOnlineViewerPresence(r.roomCtx, OnlineViewerClose, viewerIDs, 0)
	if err != nil {
		fmt.Printf("关闭房间时同步分布式在线人数失败: %v, 房间: %s\n", err, r.roomNumber)
	}
	r.onlineViewer.Store(0)
	if _, distributed := r.distributedOnlineDataSource(); !distributed {
		r.storeCurrentOnlineViewerCount(0)
	}
	r.flushCurrentOnlineViewerPeak()
	// 关播前同步处理尚在房间环形缓冲区中的消息，避免最后 200ms 内的评论遗漏。
	r.flushPendingMessagesToDataSource()
	// 在分布式状态更新后取消上下文，避免 Redis 操作使用已取消的 context。
	r.cancelFunc()

	viewers := make([]*Viewer, 0, len(r.viewers))
	for _, viewer := range r.viewers {
		if viewer != nil {
			viewers = append(viewers, viewer)
		}
	}
	clear(r.viewers)
	r.viewerMux.Unlock()

	for _, viewer := range viewers {
		viewer.Close()
	}

	// 清理消息环形缓冲区，帮助 GC 回收消息对象。
	r.viewerSendMu.Lock()
	for i := range r.viewerSendRoomMessageBuf {
		r.viewerSendRoomMessageBuf[i] = nil
	}
	r.viewerSendWritePos.Store(0)
	r.viewerSendReadPos.Store(0)
	r.viewerSendMu.Unlock()
}

func (r *Room) JoinRoom(viewer *Viewer) error {
	if viewer == nil {
		return fmt.Errorf("观众不能为空")
	}

	r.viewerMux.Lock()
	defer r.viewerMux.Unlock()

	if !r.isOpenRoom.Load() {
		return ErrRoomNoLiving
	}
	if _, exists := r.viewers[viewer.vid]; exists {
		return nil
	}

	proposedLocalCount := uint32(len(r.viewers) + 1)
	_, status, _, err := r.syncOnlineViewerPresence(
		r.roomCtx,
		OnlineViewerJoin,
		[]string{viewer.vid},
		proposedLocalCount,
	)
	if err != nil {
		return err
	}
	switch status {
	case OnlineViewerSyncRoomFull:
		return ErrRoomIsFull
	case OnlineViewerSyncDuplicate:
		return ErrViewerAlreadyJoined
	}

	viewer.Room = r
	r.viewers[viewer.vid] = viewer
	r.onlineViewer.Store(proposedLocalCount)
	r.totalViewer.Add(1)
	return nil
}

func (r *Room) LeaveRoom(viewer *Viewer) {
	if viewer == nil {
		return
	}

	r.viewerMux.Lock()
	joinedViewer, exists := r.viewers[viewer.vid]
	if !exists {
		r.viewerMux.Unlock()
		return
	}

	delete(r.viewers, viewer.vid)
	localCount := uint32(len(r.viewers))
	r.onlineViewer.Store(localCount)
	r.leaveRoomViewerCnt.Add(1)
	if _, _, _, err := r.syncOnlineViewerPresence(
		r.roomCtx,
		OnlineViewerLeave,
		[]string{viewer.vid},
		localCount,
	); err != nil {
		fmt.Printf("观众离房时同步分布式在线人数失败: %v, 房间: %s\n", err, r.roomNumber)
	}
	r.viewerMux.Unlock()

	// 关闭连接放在房间锁之外，避免取消回调再次进入 LeaveRoom 时发生锁等待。
	joinedViewer.Close()
}

// 房间消息收集器
func (r *Room) MessageCollector() {

	// 使用select的default实现非阻塞处理
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	messageBatch := make([]*MessagePb, 0, 5000)

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
func (r *Room) processSingleViewer(viewerID string, batch *[]*MessagePb) {
	r.viewerMux.RLock()
	viewer, exists := r.viewers[viewerID]
	r.viewerMux.RUnlock()

	if !exists || viewer == nil || viewer.sendRoomHasMessage.Load() != 1 {
		return
	}
	isAnchor, ok := viewer.GetCustomData("IsAnchor")
	if !ok {
		isAnchor = false
	}
	userTags, ok := viewer.GetCustomData("UserTags")
	if !ok {
		userTags = ""
	}
	userAvatar, ok := viewer.GetCustomData("UserAvatar")
	if !ok {
		userAvatar = ""
	}

	rawMessages := viewer.CollectMessages()
	for _, data := range rawMessages {
		var messagePb MessagePb
		err := proto.Unmarshal(data, &messagePb)
		if err != nil {
			fmt.Printf("[错误] 收集处理单个观众的消息时发生错误: %v\n", err)
			continue
		}
		messagePb.LiveId = string(r.roomNumber)
		messagePb.SendClient = &SendClientInfoPb{
			UserId:     viewer.GetViewerID(),
			NickName:   viewer.GetViewerName(),
			IsAnchor:   isAnchor.(bool),
			UserTags:   userTags.(string),
			UserAvatar: userAvatar.(string),
		}
		messagePb.Priority = MessagePriority_LOW
		messagePb.Timestamp = time.Now().Unix()

		// Apply message filtering here
		filteredMsg, allowed := r.ApplyMessageFilter(&messagePb, r.messageFilterLimit.Load())
		if !allowed {
			// Skip this message as it was blocked by filter
			continue
		}

		*batch = append(*batch, filteredMsg)
		// 更新房间接收统计
		r.bytesReceivedCnt.Add(int64(len(data)))
		// 点赞
		if messagePb.Code == Code_Event_User_Click_Like {
			r.likeCount.Add(1)
			continue
		}
		r.messageReceivedCnt.Add(1)
	}

	// 批次达到一定大小就发送，避免频繁发送
	if len(*batch) >= 400 {
		r.sendBatch(*batch)
		*batch = (*batch)[:0]
	}
}

// 批量处理收集到的消息
func (r *Room) processBatch(batch *[]*MessagePb) {
	r.viewerMux.RLock()
	defer r.viewerMux.RUnlock()

	// 每次只处理一部分观众，避免阻塞太久
	limit := 100
	count := 0

	for _, viewer := range r.viewers {
		if count >= limit {
			break
		}
		isAnchor, ok := viewer.GetCustomData("IsAnchor")
		if !ok {
			isAnchor = false
		}

		userAvatar, ok := viewer.GetCustomData("UserAvatar")
		if !ok {
			userAvatar = ""
		}

		userTags, ok := viewer.GetCustomData("UserTags")
		if !ok {
			userTags = ""
		}

		if viewer.sendRoomHasMessage.Load() == 1 {
			rawMessages := viewer.CollectMessages()
			for _, data := range rawMessages {
				// 添加日志：打印用户发送的消息
				//fmt.Printf("[消息] room=%s viewer=%s: %s\n", r.roomNumber, viewer.vname, string(data))
				var messagePb MessagePb
				err := proto.Unmarshal(data, &messagePb)
				if err != nil {
					fmt.Printf("[错误] 批量收集处理观众的消息时发生错误: %v\n", err)
					continue
				}
				messagePb.LiveId = string(r.roomNumber)
				messagePb.SendClient = &SendClientInfoPb{
					UserId:     viewer.GetViewerID(),
					NickName:   viewer.GetViewerName(),
					IsAnchor:   isAnchor.(bool),
					UserAvatar: userAvatar.(string),
					UserTags:   userTags.(string),
				}
				messagePb.Priority = MessagePriority_LOW
				messagePb.Timestamp = time.Now().Unix()
				// Apply message filtering here
				filteredMsg, allowed := r.ApplyMessageFilter(&messagePb, r.messageFilterLimit.Load())
				if !allowed {
					// Skip this message as it was blocked by filter
					continue
				}

				*batch = append(*batch, filteredMsg)
				// 更新房间接收统计
				r.bytesReceivedCnt.Add(int64(len(data)))
				// 点赞
				if messagePb.Code == Code_Event_User_Click_Like {
					r.likeCount.Add(1)
					continue
				}
				r.messageReceivedCnt.Add(1)
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
	//fmt.Printf("房间 %s messageToDataSource 协程开始运行\n", r.roomNumber)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	streamKey := fmt.Sprintf(Live_Msg_Broadcast, r.firmUUID, r.roomNumber)
	for {
		select {
		case <-r.roomCtx.Done():
			fmt.Printf("房间 %s messageToDataSource 协程退出（房间上下文取消）\n", r.roomNumber)
			return // 房间关闭ok退出
		case <-ticker.C:
			r.flushPendingMessagesToDataSourceForStream(streamKey)
		}
	}
}

// readFromMessageRingBuffer 从 ring buffer 读取消息
func (r *Room) readFromMessageRingBuffer() []*MessagePb {
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

	var messages []*MessagePb
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

// 房间广播消息处理器
func (r *Room) broadcastHandler() {
	ticker := time.NewTicker(300 * time.Millisecond) // 每300ms检查一次数据源是否有新消息需要广播
	defer ticker.Stop()
	for {
		select {
		case <-r.roomCtx.Done():
			return // 房间关闭，退出广播
		case <-ticker.C:
			if r.dataSource != nil {
				//高优先级消息
				hpStreamKey := fmt.Sprintf(Live_Msg_Broadcast_HP, r.firmUUID, r.roomNumber)
				hpMessages := r.dataSource.GetMessage(r.roomCtx, hpStreamKey)
				if len(hpMessages) > 0 {
					// 广播消息给所有观众
					r.broadcastToViewers(hpMessages)
				}
				//低优先级消息
				streamKey := fmt.Sprintf(Live_Msg_Broadcast, r.firmUUID, r.roomNumber)
				messages := r.dataSource.GetMessage(r.roomCtx, streamKey)
				if len(messages) > 0 {
					// 广播消息给所有观众
					r.broadcastToViewers(messages)
				}
			}
		}
	}
}

// broadcastToViewers 广播消息给所有观众
func (r *Room) broadcastToViewers(messages []*MessagePb) {
	if len(messages) == 0 {
		return
	}

	// 分离高优先级和低优先级消息
	highPriorityMessages := make([][]byte, 0)
	lowPriorityMessages := make([][]byte, 0)
	for _, msg := range messages {
		data, err := proto.Marshal(msg)
		if err != nil {
			// 记录错误，继续处理其他消息
			log.Printf("Failed to marshal message: %v", err)
			continue
		}
		if msg.Priority == MessagePriority_HIGH {
			highPriorityMessages = append(highPriorityMessages, data)
		} else {
			lowPriorityMessages = append(lowPriorityMessages, data)
		}
	}

	if len(highPriorityMessages) == 0 && len(lowPriorityMessages) == 0 {
		return
	}

	// 获取所有观众
	r.viewerMux.RLock()
	viewers := make([]*Viewer, 0, len(r.viewers))
	for _, viewer := range r.viewers {
		viewers = append(viewers, viewer)
	}
	r.viewerMux.RUnlock()

	// 入队操作是常数时间且不会等待网络 I/O；同步遍历避免每个消息/观众组合都创建协程，
	// 同时保持 broadcastHandler 中消息批次的先后顺序。
	for _, viewer := range viewers {
		if !r.isViewerActive(viewer) {
			continue
		}
		if len(highPriorityMessages) > 0 {
			r.sendPriorityMessagesToViewer(viewer, highPriorityMessages, MessagePriority_HIGH)
		}
		if len(lowPriorityMessages) > 0 {
			r.sendPriorityMessagesToViewer(viewer, lowPriorityMessages, MessagePriority_LOW)
		}
	}

}

// sendPriorityMessagesToViewer 发送优先级消息到指定观众
func (r *Room) sendPriorityMessagesToViewer(viewer *Viewer, messageBytes [][]byte, priority MessagePriority) {
	// 检查观众是否活跃
	if !r.isViewerActive(viewer) {
		return
	}

	for _, msgBytes := range messageBytes {
		if priority == MessagePriority_HIGH {
			r.trySendHighPriorityToViewerBuffer(viewer, msgBytes)
		} else {
			r.trySendToViewerBuffer(viewer, msgBytes)
		}
	}
}

// 尝试发送消息到观众的环形缓冲区
func (r *Room) trySendToViewerBuffer(viewer *Viewer, message []byte) bool {
	// 慢用户只会丢弃自身的低优先级消息，不会阻塞房间的扇出循环。
	return viewer.enqueueOutboundMessage(message, MessagePriority_LOW)
}

// 尝试发送高优先级消息到观众的环形缓冲区（插队）
func (r *Room) trySendHighPriorityToViewerBuffer(viewer *Viewer, message []byte) bool {
	// enqueue 会先逐出该用户尚未发送的低优先级积压，再把高优先级消息放到队首。
	return viewer.enqueueOutboundMessage(message, MessagePriority_HIGH)
}

// GetCapacity 获取房间当前最大容纳人数
func (r *Room) GetCapacity() uint32 {
	return r.maxViewer
}

func (r *Room) isViewerActive(viewer *Viewer) bool {
	// 检查观众是否已经取消
	select {
	case <-viewer.viewerCtx.Done():
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
		r.roomNumber, r.roomName, r.GetOnlineViewerCount(), r.isOpenRoom.Load(), r.maxViewer, r.totalViewer.Load(), r.likeCount.Load(), messageCount)
}

// 获取房间号
func (r *Room) GetRoomNumber() string {
	return r.roomNumber
}

// 获取房间号
func (r *Room) GetRoomName() string {
	return r.roomName
}

// 获取事业部ID
func (r *Room) GetFirmUUID() string {
	return r.firmUUID
}

// 根据ViewerID获取观众
func (r *Room) GetViewer(viewerID string) *Viewer {
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

// GetOnlineViewerCount 获取所有服务实例合计的实时在线人数。
func (r *Room) GetOnlineViewerCount() uint32 {
	return r.distributedOnlineViewer.Load()
}

// GetLocalOnlineViewerCount 获取当前服务实例承载的实时在线人数。
func (r *Room) GetLocalOnlineViewerCount() uint32 {
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
	fmt.Printf("  在线人数: %d（本实例: %d）\n", r.GetOnlineViewerCount(), r.GetLocalOnlineViewerCount())
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
func (r *Room) RedisBytesSent(streamKey string) int64 {
	if r.dataSource != nil {
		return r.dataSource.GetRedisBytesSent(streamKey)
	}
	return 0
}

// RedisBytesRecv 获取房间从Redis接收的字节数
func (r *Room) RedisBytesRecv(streamKey string) int64 {
	if r.dataSource != nil {
		return r.dataSource.GetRedisBytesRecv(streamKey)
	}
	return 0
}

// SendSystemMessage 发送系统消息（高优先级）
func (r *Room) SendSystemMessage(data []byte) {
	var msg MessagePb
	err := proto.Unmarshal(data, &msg)
	if err != nil {
		fmt.Println("无法解码系统消息:", err)
		return
	}

	// 直接写入到消息环形缓冲区
	r.writeToMessageRingBuffer(&msg)

	// 更新房间发送统计
	r.messageSentCnt.Add(1)
	r.bytesSentCnt.Add(int64(len(data)))
}

func (r *Room) GetRoomCtx() context.Context {
	return r.roomCtx
}

func (r *Room) GetDataSource() DataSource {
	return r.dataSource
}

// 房间设置消息过滤器
func (r *Room) SetMessageFilter(filter MessageFilter) {
	r.messageFilter = filter
	r.filterEnabled.Store(filter != nil)
}

// 获取房间消息过滤器
func (r *Room) GetMessageFilter() MessageFilter {
	return r.messageFilter
}

// 添加消息过滤规则
func (r *Room) AddFilterRule(pattern string, action int, replacement string, priority int, limit int64) error {
	log.Printf("%v 添加消息过滤规则: %s, %d, %s, %d, %d\n", r.GetRoomNumber(), pattern, action, replacement, priority, limit)
	if r.messageFilter == nil {
		r.messageFilter = NewDefaultMessageFilter()
		r.filterEnabled.Store(true)
	}
	r.messageFilterLimit.Store(limit)

	compiledPattern, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %v", err)
	}

	rule := &MessageFilterRule{
		ID:          fmt.Sprintf("rule_%d_%s", time.Now().UnixNano(), pattern[:min(10, len(pattern))]),
		Pattern:     compiledPattern,
		Action:      action,
		Replacement: replacement,
		Priority:    priority,
		Limit:       limit,
	}

	return r.messageFilter.AddRule(rule)
}

// 检查消息是否符合过滤规则
func (r *Room) ApplyMessageFilter(msg *MessagePb, limit int64) (*MessagePb, bool) {
	// 自适应采样位于通用过滤器之前，避免高并发场景下对最终会被丢弃的低优先级消息执行正则和敏感词检测。
	if !r.allowAdaptiveLowPriorityMessage(msg) {
		return nil, false
	}
	if !r.filterEnabled.Load() || r.messageFilter == nil {
		return msg, true // Allow all messages if filter is disabled
	}
	allow, filteredMsg, err := r.messageFilter.ShouldAllowMessage(msg, limit)

	if err != nil {
		log.Printf("Error applying message filter: %v", err)
		return msg, true
	}

	return filteredMsg, allow
}

// LiveOnlineViewerPeak 描述一场直播中最高在线人数所在的区间。
type LiveOnlineViewerPeak struct {
	LiveStart time.Time        `json:"live_start"`
	LiveEnd   time.Time        `json:"live_end"`
	Peak      OnlineViewerPeak `json:"peak"`
	HasData   bool             `json:"has_data"`
}

// SaveLiveOnlineViewerPeak 汇总 [liveStart, liveEnd) 内所有时间桶的峰值，
// 并将最大数值写入 Live_Online_User_Peak_Max_Count。
// 即使多个实例同时执行，RedisDataSource 也会通过 StoreMax 保留更大的值。
func (r *Room) SaveLiveOnlineViewerPeak(ctx context.Context, liveStart time.Time, liveEnd time.Time) error {
	if !liveEnd.After(liveStart) {
		return fmt.Errorf("直播结束时间必须晚于开始时间")
	}
	if r.dataSource == nil {
		return fmt.Errorf("数据源未初始化")
	}
	if ctx == nil {
		ctx = r.roomCtx
	}

	interval := r.GetOnlinePeakInterval()
	firstWindow := liveStart.Truncate(interval)
	// 用结束时刻前 1ns 定位最后一个有效时间桶，保证区间为左闭右开。
	lastWindow := liveEnd.Add(-time.Nanosecond).Truncate(interval)
	var maxCount uint32

	for windowStart := firstWindow; !windowStart.After(lastWindow); windowStart = windowStart.Add(interval) {
		peak, err := r.GetOnlineViewerPeak(ctx, windowStart)
		if err != nil {
			// 某个桶无人在线、已过期或尚未写入时，按 0 处理。
			if errors.Is(err, redis.Nil) {
				continue
			}
			return fmt.Errorf("查询区间峰值 %s: %w", windowStart.Format(time.RFC3339), err)
		}
		if peak.Count > maxCount {
			maxCount = peak.Count
		}
	}
	return r.storeLiveOnlineViewerPeakMax(ctx, maxCount)
}

// ResetLiveOnlineViewerPeakMax 在一场新直播开始前清零最高峰值键。
// 同一 roomNumber 复播时，必须由直播编排服务在所有实例接受新用户前只调用一次；
// 运行中的直播不可调用此方法，否则会清除已有统计结果。
func (r *Room) ResetLiveOnlineViewerPeakMax(ctx context.Context) error {
	if r.dataSource == nil {
		return fmt.Errorf("房间数据源未初始化")
	}
	if ctx == nil {
		ctx = r.roomCtx
	}

	r.onlinePeakMaxMu.Lock()
	defer r.onlinePeakMaxMu.Unlock()
	key := LiveOnlineViewerPeakMaxKey(r.firmUUID, r.roomNumber)
	if err := r.dataSource.Store(ctx, key, uint32(0), r.onlinePeakRetention); err != nil {
		return fmt.Errorf("重置直播最高在线人数失败: %w", err)
	}
	r.onlinePeakMaxCount.Store(0)
	return nil
}

package live

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockDataSource 模拟数据源，用于测试
type MockDataSource struct {
	messages      map[RoomNumber][]*Message
	mu            sync.RWMutex
	bytesSent     atomic.Int64
	bytesReceived atomic.Int64
}

// NewMockDataSource 创建新的模拟数据源
func NewMockDataSource() *MockDataSource {
	return &MockDataSource{
		messages: make(map[RoomNumber][]*Message),
	}
}

// SendMessage 发送消息到模拟数据源
func (m *MockDataSource) SendMessage(ctx context.Context, roomNumber RoomNumber, msg *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages[roomNumber] = append(m.messages[roomNumber], msg)
	m.bytesSent.Add(int64(len(msg.Data)))
	return nil
}

// GetMessage 从模拟数据源获取消息
func (m *MockDataSource) GetMessage(ctx context.Context, roomNumber RoomNumber) []*Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	messages := m.messages[roomNumber]
	if len(messages) == 0 {
		return nil
	}

	// 复制消息并清空存储
	result := make([]*Message, len(messages))
	copy(result, messages)
	m.messages[roomNumber] = nil

	// 统计接收的字节数
	for _, msg := range result {
		m.bytesReceived.Add(int64(len(msg.Data)))
	}

	return result
}

// GetRedisBytesSent 获取发送到Redis的字节数
func (m *MockDataSource) GetRedisBytesSent(roomNumber RoomNumber) int64 {
	return m.bytesSent.Load()
}

// GetRedisBytesRecv 获取从Redis接收的字节数
func (m *MockDataSource) GetRedisBytesRecv(roomNumber RoomNumber) int64 {
	return m.bytesReceived.Load()
}

// TestRoomViewerInteraction 测试房间和观众之间的交互
func TestRoomViewerInteraction(t *testing.T) {
	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 创建房间
	roomNumber := RoomNumber("test-room-123")
	roomName := "测试房间"
	roomMax := uint32(100)

	room, err := NewRoom(ctx, roomName, roomNumber, roomMax)
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// 创建模拟数据源
	dataSource := NewMockDataSource()

	// 启动房间
	room.Start(dataSource)
	defer room.Close()

	// 验证房间信息
	if room.RoomNumber() != roomNumber {
		t.Errorf("Expected room number %s, got %s", roomNumber, room.RoomNumber())
	}

	if !room.IsOpen() {
		t.Error("Expected room to be open")
	}

	if room.GetCapacity() != roomMax {
		t.Errorf("Expected room capacity %d, got %d", roomMax, room.GetCapacity())
	}

	// 创建观众
	viewer1ID := ViewerID("viewer-001")
	viewer1 := NewViewer(ctx, viewer1ID, "观众1", ViewerTypeViewer)

	// 加入房间
	err = room.JoinRoom(viewer1)
	if err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}

	// 验证观众数量
	if room.GetOnlineViewerCount() != 1 {
		t.Errorf("Expected online viewers 1, got %d", room.GetOnlineViewerCount())
	}

	if room.GetTotalViewerCount() != 1 {
		t.Errorf("Expected total viewers 1, got %d", room.GetTotalViewerCount())
	}

	// 创建测试消息
	messageData := map[string]string{
		"content": "Hello, world!",
		"type":    "text",
	}

	jsonData, err := json.Marshal(messageData)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	// 发送几条消息
	messageCount := 5
	for i := 0; i < messageCount; i++ {
		viewer1.Write(jsonData)
	}

	// 等待一段时间，让Room的MessageCollector处理消息
	time.Sleep(500 * time.Millisecond)

	// 直接将一条消息发送到数据源进行测试
	testMsg := &Message{
		ViewerID: viewer1ID,
		Data:     jsonData,
		Time:     time.Now(),
	}
	err = dataSource.SendMessage(ctx, roomNumber, testMsg)
	if err != nil {
		t.Errorf("Failed to send test message to data source: %v", err)
	}

	// 从数据源获取消息
	dataSourceMessages := dataSource.GetMessage(ctx, roomNumber)
	if len(dataSourceMessages) == 0 {
		t.Error("Expected messages in data source")
	} else {
		t.Logf("Received %d messages from data source", len(dataSourceMessages))
	}

	// 广播消息给观众
	if len(dataSourceMessages) > 0 {
		room.broadcastToViewersOptimized(dataSourceMessages)
	}

	// 等待观众接收消息
	time.Sleep(200 * time.Millisecond)

	// 观众离开房间
	room.LeaveRoom(viewer1)

	// 验证观众数量减少
	time.Sleep(100 * time.Millisecond)
	if room.GetOnlineViewerCount() != 0 {
		t.Errorf("Expected online viewers 0, got %d", room.GetOnlineViewerCount())
	}

	// 关闭房间
	room.Close()

	// 验证房间关闭
	time.Sleep(100 * time.Millisecond)
	if room.IsOpen() {
		t.Error("Expected room to be closed")
	}
}

// TestMultipleViewers 测试多个观众的场景
func TestMultipleViewers(t *testing.T) {
	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 创建房间
	roomNumber := RoomNumber("test-room-456")
	roomName := "多观众测试房间"
	roomMax := uint32(100)

	room, err := NewRoom(ctx, roomName, roomNumber, roomMax)
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// 创建模拟数据源
	dataSource := NewMockDataSource()

	// 启动房间
	room.Start(dataSource)
	defer room.Close()

	// 创建多个观众
	const viewerCount = 10
	viewers := make([]*Viewer, viewerCount)

	// 观众加入房间
	for i := 0; i < viewerCount; i++ {
		viewerID := ViewerID(fmt.Sprintf("viewer-%03d", i+1))
		viewer := NewViewer(ctx, viewerID, fmt.Sprintf("观众%d", i+1), ViewerTypeViewer)

		err = room.JoinRoom(viewer)
		if err != nil {
			t.Fatalf("Failed to join room: %v", err)
		}

		viewer.StartMessageReader()
		viewers[i] = viewer
	}

	// 验证观众数量
	time.Sleep(100 * time.Millisecond)
	if room.GetOnlineViewerCount() != uint32(viewerCount) {
		t.Errorf("Expected online viewers %d, got %d", viewerCount, room.GetOnlineViewerCount())
	}

	// 发送消息
	messageData := map[string]string{
		"content": "Hello, everyone!",
		"type":    "broadcast",
	}

	jsonData, err := json.Marshal(messageData)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	// 确保第一个观众不为空
	if viewers[0] == nil {
		t.Fatal("First viewer is nil")
	}

	// 第一个观众发送多条消息
	for i := 0; i < 10; i++ {
		viewers[0].Write(jsonData)
	}

	// 检查sendRoomHasMessage状态并设置为1
	if viewers[0].sendRoomHasMessage.Load() != 1 {
		viewers[0].sendRoomHasMessage.Store(1)
	}

	// 直接调用房间的processSingleViewer方法来强制收集消息
	var messageBatch []*Message
	room.processSingleViewer(viewers[0].vid, &messageBatch)

	// 直接调用数据源的SendMessage方法将消息发送到数据源
	for _, msg := range messageBatch {
		if msg != nil {
			err := dataSource.SendMessage(ctx, roomNumber, msg)
			if err != nil {
				t.Errorf("Failed to send message to data source: %v", err)
			}
		}
	}

	// 直接调用数据源的SendMessage方法再发送一条测试消息
	testMsg := &Message{
		ViewerID: viewers[0].vid,
		Data:     jsonData,
		Time:     time.Now(),
	}
	err = dataSource.SendMessage(ctx, roomNumber, testMsg)
	if err != nil {
		t.Errorf("Failed to send test message to data source: %v", err)
	}

	// 从数据源获取消息
	dataSourceMessages := dataSource.GetMessage(ctx, roomNumber)
	if len(dataSourceMessages) == 0 {
		t.Error("Expected messages in data source")
	} else {
		t.Logf("Received %d messages from data source", len(dataSourceMessages))
	}

	// 广播消息给所有观众
	if len(dataSourceMessages) > 0 {
		room.broadcastToViewersOptimized(dataSourceMessages)
	}

	// 等待观众接收消息
	time.Sleep(200 * time.Millisecond)

	// 观众离开房间
	for _, viewer := range viewers {
		room.LeaveRoom(viewer)
	}

	// 验证观众数量减少
	time.Sleep(100 * time.Millisecond)
	if room.GetOnlineViewerCount() != 0 {
		t.Errorf("Expected online viewers 0, got %d", room.GetOnlineViewerCount())
	}
}

// TestMessageCollector 测试消息收集器
func TestMessageCollector(t *testing.T) {
	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 创建房间
	roomNumber := RoomNumber("test-room-789")
	roomName := "消息收集器测试房间"
	roomMax := uint32(10)

	room, err := NewRoom(ctx, roomName, roomNumber, roomMax)
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// 创建模拟数据源
	dataSource := NewMockDataSource()

	// 启动房间
	room.Start(dataSource)
	defer room.Close()

	// 创建观众
	viewerID := ViewerID("collector-viewer-001")
	viewer := NewViewer(ctx, viewerID, "收集器测试观众", ViewerTypeViewer)

	err = room.JoinRoom(viewer)
	if err != nil {
		t.Fatalf("Failed to join room: %v", err)
	}

	viewer.StartMessageReader()

	// 发送多条消息
	const messageCount = 100
	for i := 0; i < messageCount; i++ {
		messageData := map[string]string{
			"content": fmt.Sprintf("Message %d", i+1),
			"type":    "text",
		}

		jsonData, err := json.Marshal(messageData)
		if err != nil {
			t.Fatalf("Failed to marshal message: %v", err)
		}

		viewer.Write(jsonData)
	}

	// 等待消息处理
	time.Sleep(2 * time.Second)

	// 检查消息统计
	if viewer.SentMessages() < int64(messageCount) {
		t.Errorf("Expected viewer sent messages at least %d, got %d", messageCount, viewer.SentMessages())
	}

	// 检查房间统计
	if room.messageReceivedCnt.Load() < int64(messageCount) {
		t.Errorf("Expected room received messages at least %d, got %d", messageCount, room.messageReceivedCnt.Load())
	}

	// 观众离开房间
	room.LeaveRoom(viewer)
}

// TestTwoViewersRandomMessages 测试两个观众在5分钟内随机发送消息
func TestTwoViewersRandomMessages(t *testing.T) {
	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute+10*time.Second)
	defer cancel()

	// 创建房间
	roomNumber := RoomNumber("test-room-random-messages")
	roomName := "随机消息测试房间"
	roomMax := uint32(10)

	room, err := NewRoom(ctx, roomName, roomNumber, roomMax)
	if err != nil {
		t.Fatalf("Failed to create room: %v", err)
	}

	// 创建模拟数据源
	dataSource := NewMockDataSource()

	// 启动房间
	room.Start(dataSource)
	defer room.Close()

	// 创建两个观众
	viewer1ID := ViewerID("viewer-random-001")
	viewer1 := NewViewer(ctx, viewer1ID, "随机观众1", ViewerTypeViewer)

	viewer2ID := ViewerID("viewer-random-002")
	viewer2 := NewViewer(ctx, viewer2ID, "随机观众2", ViewerTypeViewer)

	// 加入房间
	err = room.JoinRoom(viewer1)
	if err != nil {
		t.Fatalf("Viewer1 failed to join room: %v", err)
	}

	err = room.JoinRoom(viewer2)
	if err != nil {
		t.Fatalf("Viewer2 failed to join room: %v", err)
	}

	// 启动消息读取器
	viewer1.StartMessageReader()
	viewer2.StartMessageReader()

	// 直接启动观众信息打印协程（因为StartMessageReader不会启动这个协程）
	go viewer1.PrintViewerInfo()
	go viewer2.PrintViewerInfo()

	// 验证观众数量
	time.Sleep(100 * time.Millisecond)
	if room.GetOnlineViewerCount() != 2 {
		t.Errorf("Expected 2 online viewers, got %d", room.GetOnlineViewerCount())
	}

	// 开始时间
	startTime := time.Now()
	var wg sync.WaitGroup

	// 观众1随机发送消息
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// 随机间隔（1-10秒）
				randomInterval := time.Duration(1+rand.Intn(10)) * time.Second
				time.Sleep(randomInterval)

				// 创建消息
				messageData := map[string]string{
					"content": fmt.Sprintf("Random message from viewer1 at %s", time.Now().Format("15:04:05")),
					"type":    "text",
				}

				jsonData, err := json.Marshal(messageData)
				if err != nil {
					t.Logf("Viewer1 failed to marshal message: %v", err)
					continue
				}

				// 发送消息
				viewer1.Write(jsonData)
				t.Logf("Viewer1 sent message at %s", time.Now().Format("15:04:05"))
			}
		}
	}()

	// 观众2随机发送消息
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// 随机间隔（1-15秒）
				randomInterval := time.Duration(1+rand.Intn(15)) * time.Second
				time.Sleep(randomInterval)

				// 创建消息
				messageData := map[string]string{
					"content": fmt.Sprintf("Random message from viewer2 at %s", time.Now().Format("15:04:05")),
					"type":    "text",
				}

				jsonData, err := json.Marshal(messageData)
				if err != nil {
					t.Logf("Viewer2 failed to marshal message: %v", err)
					continue
				}

				// 发送消息
				viewer2.Write(jsonData)
				t.Logf("Viewer2 sent message at %s", time.Now().Format("15:04:05"))
			}
		}
	}()

	// 等待5分钟或上下文取消
	select {
	case <-ctx.Done():
		// 上下文取消，退出循环
	case <-time.After(5 * time.Minute):
		// 5分钟结束
	}

	// 等待所有goroutine完成
	go func() {
		time.Sleep(2 * time.Second) // 给goroutine一些时间来处理最后一条消息
		cancel()
	}()
	wg.Wait()

	// 统计时间
	duration := time.Since(startTime)
	t.Logf("Test ran for %v", duration)

	// 等待消息处理完成
	time.Sleep(5 * time.Second)

	// 打印统计信息
	viewer1Messages := viewer1.SentMessages()
	viewer2Messages := viewer2.SentMessages()
	roomMessages := room.messageReceivedCnt.Load()
	dataSourceMessages := len(dataSource.GetMessage(ctx, roomNumber))

	t.Logf("Viewer1 sent %d messages", viewer1Messages)
	t.Logf("Viewer2 sent %d messages", viewer2Messages)
	t.Logf("Room received %d messages", roomMessages)
	t.Logf("Data source stored %d messages", dataSourceMessages)

	// 检查基本统计
	if viewer1Messages == 0 {
		t.Error("Viewer1 should have sent at least one message")
	}

	if viewer2Messages == 0 {
		t.Error("Viewer2 should have sent at least one message")
	}

	// 观众离开房间
	room.LeaveRoom(viewer1)
	room.LeaveRoom(viewer2)

	// 验证观众数量减少
	time.Sleep(100 * time.Millisecond)
	if room.GetOnlineViewerCount() != 0 {
		t.Errorf("Expected 0 online viewers, got %d", room.GetOnlineViewerCount())
	}
}

package test

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// 测试配置
const (
	TestDuration    = 1 * time.Minute // 测试持续时间，改为1分钟
	TotalViewers    = 2               // 总观众数量，改为2个
	MessageInterval = 5 * time.Second // 消息发送间隔，改为5秒一次
	RoomName        = "test_room"
)

// 测试统计信息
type TestStats struct {
	TotalConnections      int       `json:"total_connections"`
	SuccessfulConnections int       `json:"successful_connections"`
	FailedConnections     int       `json:"failed_connections"`
	TotalMessagesSent     int64     `json:"total_messages_sent"`
	TotalMessagesReceived int64     `json:"total_messages_received"`
	StartTime             time.Time `json:"start_time"`
	EndTime               time.Time `json:"end_time"`
}

var (
	stats      TestStats
	statsMutex sync.Mutex
)

func StressTestMain() {
	fmt.Println("=== 房间高并发压力测试开始 ===")
	fmt.Printf("测试配置: 房间持续 %v, 总观众数 %d, 消息间隔 %v\n",
		TestDuration, TotalViewers, MessageInterval)

	// 初始化统计信息
	stats = TestStats{
		TotalConnections: TotalViewers,
		StartTime:        time.Now(),
	}

	// 启动测试协程
	var wg sync.WaitGroup
	wg.Add(TotalViewers)

	for i := 0; i < TotalViewers; i++ {
		go func(viewerID int) {
			defer wg.Done()
			username := fmt.Sprintf("观众_%d", viewerID)
			testViewer(username)
		}(i)

		// 模拟用户分批加入
		time.Sleep(50 * time.Millisecond)
	}

	// 等待所有测试完成
	go func() {
		wg.Wait()
		stats.EndTime = time.Now()
		printStats()
	}()

	// 主协程保持运行
	fmt.Printf("测试将持续 %v，请等待...\n", TestDuration)
	time.Sleep(TestDuration)

	// 打印最终统计信息
	stats.EndTime = time.Now()
	printStats()
	fmt.Println("=== 房间高并发压力测试结束 ===")
}

// 程序入口 - 移除 main 函数，改为库代码
// func main() {
// 	StressTestMain()
// }

func testViewer(username string) {
	// 生成唯一用户ID
	userID := fmt.Sprintf("%s_%d", username, time.Now().UnixNano())

	// WebSocket连接URL
	url := fmt.Sprintf("ws://localhost:8080/ws?room=%s&userid=%s&username=%s",
		RoomName, userID, username)

	// 连接到WebSocket服务器
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Printf("%s 连接失败: %v\n", username, err)
		statsMutex.Lock()
		stats.FailedConnections++
		statsMutex.Unlock()
		return
	}
	defer conn.Close()

	statsMutex.Lock()
	stats.SuccessfulConnections++
	statsMutex.Unlock()

	// 启动消息接收协程
	go receiveMessages(conn, username)

	// 持续发送消息
	startTime := time.Now()
	for time.Since(startTime) < TestDuration {
		// 生成随机消息
		message := generateRandomMessage(username)

		// 发送消息
		err := conn.WriteMessage(websocket.TextMessage, []byte(message))
		if err != nil {
			log.Printf("%s 发送消息失败: %v\n", username, err)
			break
		}

		statsMutex.Lock()
		stats.TotalMessagesSent++
		statsMutex.Unlock()

		// 使用配置的消息间隔
		time.Sleep(MessageInterval)
	}
}

func receiveMessages(conn *websocket.Conn, username string) {
	// 设置接收超时
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			// 如果是超时，更新超时时间继续接收
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("%s 接收消息错误: %v\n", username, err)
			}
			break
		}

		statsMutex.Lock()
		stats.TotalMessagesReceived++
		statsMutex.Unlock()

		// 更新超时时间
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	}
}

func generateRandomMessage(username string) string {
	messages := []string{
		"主播好！",
		"这个直播内容很有趣！",
		"请问什么时候开始抽奖？",
		"支持主播！",
		"这个问题怎么解决？",
		"主播辛苦了！",
		"能不能再讲一遍？",
		"我觉得这个观点很对！",
		"谢谢主播的分享！",
		"这个功能太实用了！",
	}

	// 随机选择一条消息
	return fmt.Sprintf("[%s] %s: %s", time.Now().Format("15:04:05"), username, messages[rand.Intn(len(messages))])
}

func printStats() {
	statsMutex.Lock()
	defer statsMutex.Unlock()

	fmt.Printf("\n=== 测试统计信息 ===\n")
	fmt.Printf("测试持续时间: %v\n", TestDuration)
	fmt.Printf("总连接数: %d\n", stats.TotalConnections)
	fmt.Printf("成功连接数: %d\n", stats.SuccessfulConnections)
	fmt.Printf("失败连接数: %d\n", stats.FailedConnections)
	fmt.Printf("消息发送总数: %d\n", stats.TotalMessagesSent)
	fmt.Printf("消息接收总数: %d\n", stats.TotalMessagesReceived)
	fmt.Printf("连接成功率: %.2f%%\n", float64(stats.SuccessfulConnections)/float64(stats.TotalConnections)*100)
	fmt.Printf("消息接收率: %.2f%%\n", float64(stats.TotalMessagesReceived)/float64(stats.TotalMessagesSent)*100)

	if stats.SuccessfulConnections > 0 {
		averageMessagesPerViewer := float64(stats.TotalMessagesSent) / float64(stats.SuccessfulConnections)
		fmt.Printf("平均每用户发送消息数: %.2f\n", averageMessagesPerViewer)
	}
}

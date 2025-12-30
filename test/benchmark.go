package test

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golanggo/live-wss/redis"
	"github.com/golanggo/live-wss/sdk"
)

// 压测配置
type BenchmarkConfig struct {
	TotalViewers     int           // 总观众数
	MessagePerViewer int           // 每个观众发送的消息数
	MessageInterval  time.Duration // 消息发送间隔
	TestDuration     time.Duration // 测试持续时间
	RoomNumber       string        // 房间号
	RoomName         string        // 房间名
	UseRedis         bool          // 是否使用Redis数据源
	FirmUUID         int64
}

// 压测结果
type BenchmarkResult struct {
	TotalViewers           int64         // 总观众数
	ActiveViewers          int64         // 活跃观众数
	TotalMessagesSent      int64         // 总发送消息数
	TotalMessagesRecv      int64         // 总接收消息数
	TotalBytesSent         int64         // 总发送字节数
	TotalBytesRecv         int64         // 总接收字节数
	RedisBytesSent         int64         // Redis层发送字节数
	RedisBytesRecv         int64         // Redis层接收字节数
	WebSocketBytesSent     int64         // WebSocket层发送字节数
	WebSocketBytesRecv     int64         // WebSocket层接收字节数
	AvgMessageSize         float64       // 平均消息大小
	SendBandwidth          float64       // 总发送带宽（字节/秒）
	RecvBandwidth          float64       // 总接收带宽（字节/秒）
	RedisSendBandwidth     float64       // Redis层发送带宽（字节/秒）
	RedisRecvBandwidth     float64       // Redis层接收带宽（字节/秒）
	WebSocketSendBandwidth float64       // WebSocket层发送带宽（字节/秒）
	WebSocketRecvBandwidth float64       // WebSocket层接收带宽（字节/秒）
	AvgLatency             time.Duration // 平均延迟
	MaxLatency             time.Duration // 最大延迟
	MinLatency             time.Duration // 最小延迟
	Throughput             float64       // 吞吐量（消息/秒）
	Duration               time.Duration // 测试持续时间
	MemoryUsed             uint64        // 内存使用量
	Goroutines             int           // Goroutine数量
	JoinSuccessCount       int64         // 加入成功数
	JoinFailCount          int64         // 加入失败数
	SendSuccessCount       int64         // 发送成功数
	SendFailCount          int64         // 发送失败数
}

// 压测器
type Benchmark struct {
	config  BenchmarkConfig
	result  BenchmarkResult
	room    *sdk.Room
	viewers []*sdk.Viewer
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex

	// 原子计数器
	messagesSent atomic.Int64
	messagesRecv atomic.Int64
	joinSuccess  atomic.Int64
	joinFail     atomic.Int64
	sendSuccess  atomic.Int64
	sendFail     atomic.Int64

	// 延迟统计
	latencies   []time.Duration
	latenciesMu sync.Mutex
	startTime   time.Time
	dataSource  sdk.DataSource
}

// NewBenchmark 创建压测器
func NewBenchmark(config BenchmarkConfig) *Benchmark {
	ctx, cancel := context.WithCancel(context.Background())
	return &Benchmark{
		config:    config,
		viewers:   make([]*sdk.Viewer, 0, config.TotalViewers),
		ctx:       ctx,
		cancel:    cancel,
		latencies: make([]time.Duration, 0, config.TotalViewers*config.MessagePerViewer),
	}
}

// Run 运行压测
func (b *Benchmark) Run() *BenchmarkResult {
	fmt.Println("========================================")
	fmt.Println("      直播间系统压力测试开始")
	fmt.Println("========================================")
	fmt.Printf("配置信息:\n")
	fmt.Printf("  - 观众数量: %d\n", b.config.TotalViewers)
	fmt.Printf("  - 每人发送消息数: %d\n", b.config.MessagePerViewer)
	fmt.Printf("  - 消息发送间隔: %v\n", b.config.MessageInterval)
	fmt.Printf("  - 测试持续时间: %v\n", b.config.TestDuration)
	fmt.Printf("  - 使用Redis: %v\n", b.config.UseRedis)
	fmt.Println("----------------------------------------")

	b.startTime = time.Now()

	redis.InitRedis()

	// 1. 初始化数据源
	b.initDataSource()

	// 2. 创建房间
	b.createRoom()

	// 3. 批量创建观众并加入房间
	b.createAndJoinViewers()

	// 4. 运行消息压测
	b.runMessageBenchmark()

	// 5. 等待测试完成
	b.waitForCompletion()

	// 6. 收集结果
	b.collectResults()

	// 7. 清理资源
	b.cleanup()

	return &b.result
}

// initDataSource 初始化数据源
func (b *Benchmark) initDataSource() {
	fmt.Println("[1/6] 初始化数据源...")
	if !b.config.UseRedis {
		panic("仅支持Redis数据源，请设置 UseRedis: true")
	}
	// 使用Redis数据源
	redisStream := sdk.NewRedisDataSource(redis.RDB)
	stream := fmt.Sprintf(sdk.Live_Msg_Broadcast, b.config.FirmUUID)
	redisStream.CreateStreamHandler(b.ctx, b.config.RoomNumber, stream)
	b.dataSource = &RedisDataSourceAdapter{stream: redisStream, roomNumber: b.config.RoomNumber}
	fmt.Println("  使用 Redis Stream 数据源")
}

// RedisDataSourceAdapter Redis数据源适配器
type RedisDataSourceAdapter struct {
	stream     *sdk.RedisDataSource
	roomNumber string
}

// Get implements sdk.DataSource.
func (r *RedisDataSourceAdapter) Get(ctx context.Context, key string) (string, error) {
	panic("unimplemented")
}

// Store implements sdk.DataSource.
func (r *RedisDataSourceAdapter) Store(ctx context.Context, key string, value any, duration time.Duration) error {
	panic("unimplemented")
}
func (r *RedisDataSourceAdapter) AccumulatedBy(ctx context.Context, key string, value int64) error {
	panic("unimplemented")
}

func (r *RedisDataSourceAdapter) SendMessage(ctx context.Context, stream string, msg *sdk.MessagePb) error {
	return r.stream.SendMessage(ctx, stream, msg)
}

func (r *RedisDataSourceAdapter) GetMessage(ctx context.Context, stream string) []*sdk.MessagePb {
	return r.stream.GetMessage(ctx, stream)
}

// GetRedisBytesSent 获取发送到Redis的字节数
func (r *RedisDataSourceAdapter) GetRedisBytesSent(stream string) int64 {
	return r.stream.GetRedisBytesSent(stream)
}

// GetRedisBytesRecv 获取从Redis接收的字节数
func (r *RedisDataSourceAdapter) GetRedisBytesRecv(stream string) int64 {
	return r.stream.GetRedisBytesRecv(stream)
}

// createRoom 创建房间
func (b *Benchmark) createRoom() {
	fmt.Println("[2/6] 创建直播房间...")
	var err error
	b.room, err = sdk.NewRoom(b.ctx, b.config.RoomName, b.config.RoomNumber, uint32(b.config.TotalViewers+1000), fmt.Sprintf("%s", b.config.FirmUUID))
	if err != nil {
		panic(fmt.Sprintf("创建房间失败: %v", err))
	}
	b.room.Start(b.dataSource)
	fmt.Printf("  房间 %s 创建成功，最大容量: %d\n", b.config.RoomNumber, b.config.TotalViewers+1000)
}

// createAndJoinViewers 批量创建观众并加入房间
func (b *Benchmark) createAndJoinViewers() {
	fmt.Printf("[3/6] 创建 %d 个观众并加入房间...\n", b.config.TotalViewers)

	var joinWg sync.WaitGroup
	batchSize := 1000 // 每批处理1000个
	if batchSize > b.config.TotalViewers {
		batchSize = b.config.TotalViewers
	}

	for i := 0; i < b.config.TotalViewers; i += batchSize {
		end := i + batchSize
		if end > b.config.TotalViewers {
			end = b.config.TotalViewers
		}

		for j := i; j < end; j++ {
			joinWg.Add(1)
			go func(idx int) {
				defer joinWg.Done()
				b.createAndJoinViewer(idx)
			}(j)
		}

		joinWg.Wait()

		// 打印进度
		if (i+batchSize)%5000 == 0 || end == b.config.TotalViewers {
			fmt.Printf("  已加入 %d/%d 观众 (成功: %d, 失败: %d)\n",
				end, b.config.TotalViewers, b.joinSuccess.Load(), b.joinFail.Load())
		}
	}

	fmt.Printf("  观众加入完成: 成功 %d, 失败 %d\n", b.joinSuccess.Load(), b.joinFail.Load())
}

// createAndJoinViewer 创建单个观众并加入房间
func (b *Benchmark) createAndJoinViewer(idx int) {
	viewerID := fmt.Sprintf("viewer_%d", idx)
	viewerName := fmt.Sprintf("观众%d", idx)

	viewer := sdk.NewViewer(b.ctx, viewerID, viewerName, nil)

	err := b.room.JoinRoom(viewer)
	if err != nil {
		b.joinFail.Add(1)
		return
	}

	// 启动消息读取器，用于接收房间广播的消息
	viewer.StartMessageReader()

	b.mu.Lock()
	b.viewers = append(b.viewers, viewer)
	b.mu.Unlock()

	b.joinSuccess.Add(1)
}

// runMessageBenchmark 运行消息压测
func (b *Benchmark) runMessageBenchmark() {
	fmt.Println("[4/6] 开始消息压测...")

	// 为每个观众启动消息发送协程
	for _, viewer := range b.viewers {
		b.wg.Add(1)
		go b.viewerSendMessages(viewer)
	}

	// 启动统计协程
	go b.printProgress()
}

// viewerSendMessages 观众发送消息
func (b *Benchmark) viewerSendMessages(viewer *sdk.Viewer) {
	defer b.wg.Done()

	for i := 0; i < b.config.MessagePerViewer; i++ {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		// 生成随机消息
		msg := generateBenchmarkMessage(viewer.GetViewerID(), i)

		// 记录发送时间
		sendTime := time.Now()

		// 发送消息到环形缓冲区
		viewer.Write(msg)
		b.messagesSent.Add(1)
		b.sendSuccess.Add(1)

		// 计算延迟（模拟）
		latency := time.Since(sendTime)
		b.recordLatency(latency)

		// 等待发送间隔
		if b.config.MessageInterval > 0 {
			time.Sleep(b.config.MessageInterval)
		}
	}
}

// recordLatency 记录延迟
func (b *Benchmark) recordLatency(latency time.Duration) {
	b.latenciesMu.Lock()
	b.latencies = append(b.latencies, latency)
	b.latenciesMu.Unlock()
}

// printProgress 打印进度
func (b *Benchmark) printProgress() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(b.startTime)
			sent := b.messagesSent.Load()
			throughput := float64(sent) / elapsed.Seconds()

			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			fmt.Printf("  [进度] 耗时: %v | 发送: %d | 吞吐量: %.2f msg/s | 内存: %.2f MB | Goroutines: %d\n",
				elapsed.Truncate(time.Second), sent, throughput,
				float64(memStats.Alloc)/1024/1024, runtime.NumGoroutine())
		}
	}
}

// waitForCompletion 等待测试完成
func (b *Benchmark) waitForCompletion() {
	fmt.Println("[5/6] 等待消息发送完成...")

	// 使用超时机制
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("  所有消息发送完成")
	case <-time.After(b.config.TestDuration):
		fmt.Println("  测试时间到，停止发送")
	}

	// 等待一段时间让消息处理完成
	fmt.Println("  等待消息处理完成...")
	if b.config.UseRedis {
		fmt.Println("  等待Redis消息处理完成...")
		time.Sleep(30 * time.Second) // Redis需要更长时间处理和广播
	} else {
		time.Sleep(5 * time.Second)
	}

	// 再等待一段时间确保观众处理完所有消息
	fmt.Println("  确保观众处理完所有消息...")
	time.Sleep(2 * time.Second)
}

// collectResults 收集结果
func (b *Benchmark) collectResults() {
	fmt.Println("[6/6] 收集测试结果...")

	b.result.Duration = time.Since(b.startTime)
	b.result.TotalViewers = int64(b.config.TotalViewers)
	b.result.ActiveViewers = int64(len(b.viewers))
	b.result.TotalMessagesSent = b.messagesSent.Load()
	b.result.JoinSuccessCount = b.joinSuccess.Load()
	b.result.JoinFailCount = b.joinFail.Load()
	b.result.SendSuccessCount = b.sendSuccess.Load()
	b.result.SendFailCount = b.sendFail.Load()

	// 计算吞吐量
	b.result.Throughput = float64(b.result.TotalMessagesSent) / b.result.Duration.Seconds()

	// 计算延迟统计
	b.calculateLatencyStats()

	// 收集内存信息
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	b.result.MemoryUsed = memStats.Alloc
	b.result.Goroutines = runtime.NumGoroutine()

	// 统计消息接收数和字节数
	fmt.Printf("开始统计消息接收数和字节数，观众数量: %d\n", len(b.viewers))
	totalReceived := int64(0)
	totalBytesSent := int64(0)
	totalBytesReceived := int64(0)

	for i, v := range b.viewers {
		received := v.ReceivedMessages()
		totalReceived += int64(received)

		// 收集字节数
		bytesSent := v.SentBytes()
		bytesReceived := v.ReceivedBytes()
		totalBytesSent += bytesSent
		totalBytesReceived += bytesReceived

		if i < 10 { // 只打印前10个观众的接收消息数
			fmt.Printf("  观众 %s 接收消息数: %d, 发送字节: %d, 接收字节: %d\n", v.GetViewerName(), received, bytesSent, bytesReceived)
		}
	}

	// 收集房间层面的字节统计（WebSocket层）
	websocketBytesSent := b.room.BytesSent()
	websocketBytesReceived := b.room.BytesReceived()

	// 收集Redis层带宽统计
	//redisBytesSent := b.room.RedisBytesSent(b.room)
	//redisBytesReceived := b.room.RedisBytesRecv()

	// 计算总字节数
	//totalBytesSent = websocketBytesSent + redisBytesSent
	//totalBytesReceived = websocketBytesReceived + redisBytesReceived

	b.result.TotalMessagesRecv = totalReceived
	b.result.TotalBytesSent = totalBytesSent
	b.result.TotalBytesRecv = totalBytesReceived
	//b.result.RedisBytesSent = redisBytesSent
	//b.result.RedisBytesRecv = redisBytesReceived
	b.result.WebSocketBytesSent = websocketBytesSent
	b.result.WebSocketBytesRecv = websocketBytesReceived

	fmt.Printf("总计接收消息数: %d\n", totalReceived)
	fmt.Printf("总计发送字节数: %d\n", totalBytesSent)
	fmt.Printf("总计接收字节数: %d\n", totalBytesReceived)

	// 计算平均消息大小
	if b.result.TotalMessagesSent > 0 {
		b.result.AvgMessageSize = float64(totalBytesSent) / float64(b.result.TotalMessagesSent)
	}

	// 计算带宽
	testSeconds := b.result.Duration.Seconds()
	if testSeconds > 0 {
		// 总带宽
		b.result.SendBandwidth = float64(totalBytesSent) / testSeconds
		b.result.RecvBandwidth = float64(totalBytesReceived) / testSeconds

		// Redis层带宽
		b.result.RedisSendBandwidth = float64(b.result.RedisBytesSent) / testSeconds
		b.result.RedisRecvBandwidth = float64(b.result.RedisBytesRecv) / testSeconds

		// WebSocket层带宽
		b.result.WebSocketSendBandwidth = float64(b.result.WebSocketBytesSent) / testSeconds
		b.result.WebSocketRecvBandwidth = float64(b.result.WebSocketBytesRecv) / testSeconds
	}
}

// calculateLatencyStats 计算延迟统计
func (b *Benchmark) calculateLatencyStats() {
	b.latenciesMu.Lock()
	defer b.latenciesMu.Unlock()

	if len(b.latencies) == 0 {
		return
	}

	var total time.Duration
	b.result.MinLatency = b.latencies[0]
	b.result.MaxLatency = b.latencies[0]

	for _, lat := range b.latencies {
		total += lat
		if lat < b.result.MinLatency {
			b.result.MinLatency = lat
		}
		if lat > b.result.MaxLatency {
			b.result.MaxLatency = lat
		}
	}

	b.result.AvgLatency = total / time.Duration(len(b.latencies))
}

// cleanup 清理资源
func (b *Benchmark) cleanup() {
	b.cancel()
	if b.room != nil {
		b.room.Close()
	}
}

// PrintResult 打印结果
func (b *Benchmark) PrintResult() {
	r := &b.result

	fmt.Println("")
	fmt.Println("========================================")
	fmt.Println("           压测结果报告")
	fmt.Println("========================================")
	fmt.Println("")

	fmt.Println("【观众统计】")
	fmt.Printf("  总观众数:     %d\n", r.TotalViewers)
	fmt.Printf("  活跃观众数:   %d\n", r.ActiveViewers)
	fmt.Printf("  加入成功:     %d\n", r.JoinSuccessCount)
	fmt.Printf("  加入失败:     %d\n", r.JoinFailCount)
	fmt.Printf("  加入成功率:   %.2f%%\n", float64(r.JoinSuccessCount)/float64(r.TotalViewers)*100)
	fmt.Println("")

	fmt.Println("【消息统计】")
	fmt.Printf("  总发送消息:   %d\n", r.TotalMessagesSent)
	fmt.Printf("  总接收消息:   %d\n", r.TotalMessagesRecv)
	if r.TotalMessagesSent > 0 {
		fmt.Printf("  发送接收比:   %.2f:1\n", float64(r.TotalMessagesRecv)/float64(r.TotalMessagesSent))
	}
	fmt.Printf("  发送成功:     %d\n", r.SendSuccessCount)
	fmt.Printf("  发送失败:     %d\n", r.SendFailCount)
	fmt.Printf("  发送成功率:   %.2f%%\n", float64(r.SendSuccessCount)/float64(r.TotalMessagesSent)*100)
	fmt.Println("")

	fmt.Println("【带宽统计】")
	fmt.Printf("  总发送字节:   %d 字节 (%.2f MB)\n", r.TotalBytesSent, float64(r.TotalBytesSent)/1024/1024)
	fmt.Printf("  总接收字节:   %d 字节 (%.2f MB)\n", r.TotalBytesRecv, float64(r.TotalBytesRecv)/1024/1024)
	fmt.Printf("  平均消息大小: %.2f 字节\n", r.AvgMessageSize)
	fmt.Printf("  发送带宽:     %.2f KB/s\n", r.SendBandwidth/1024)
	fmt.Printf("  接收带宽:     %.2f KB/s\n", r.RecvBandwidth/1024)
	fmt.Printf("  总带宽:       %.2f KB/s\n", (r.SendBandwidth+r.RecvBandwidth)/1024)
	fmt.Println("  ")
	fmt.Println("  【Redis层带宽】")
	fmt.Printf("    Redis发送字节: %d 字节 (%.2f MB)\n", r.RedisBytesSent, float64(r.RedisBytesSent)/1024/1024)
	fmt.Printf("    Redis接收字节: %d 字节 (%.2f MB)\n", r.RedisBytesRecv, float64(r.RedisBytesRecv)/1024/1024)
	fmt.Printf("    Redis发送带宽: %.2f KB/s\n", r.RedisSendBandwidth/1024)
	fmt.Printf("    Redis接收带宽: %.2f KB/s\n", r.RedisRecvBandwidth/1024)
	fmt.Println("  ")
	fmt.Println("  【WebSocket层带宽】")
	fmt.Printf("    WebSocket发送字节: %d 字节 (%.2f MB)\n", r.WebSocketBytesSent, float64(r.WebSocketBytesSent)/1024/1024)
	fmt.Printf("    WebSocket接收字节: %d 字节 (%.2f MB)\n", r.WebSocketBytesRecv, float64(r.WebSocketBytesRecv)/1024/1024)
	fmt.Printf("    WebSocket发送带宽: %.2f KB/s\n", r.WebSocketSendBandwidth/1024)
	fmt.Printf("    WebSocket接收带宽: %.2f KB/s\n", r.WebSocketRecvBandwidth/1024)
	fmt.Println("")

	fmt.Println("【性能指标】")
	fmt.Printf("  测试耗时:     %v\n", r.Duration.Truncate(time.Millisecond))
	fmt.Printf("  吞吐量:       %.2f msg/s\n", r.Throughput)
	fmt.Printf("  平均延迟:     %v\n", r.AvgLatency)
	fmt.Printf("  最小延迟:     %v\n", r.MinLatency)
	fmt.Printf("  最大延迟:     %v\n", r.MaxLatency)
	fmt.Println("")

	fmt.Println("【系统资源】")
	fmt.Printf("  内存使用:     %.2f MB\n", float64(r.MemoryUsed)/1024/1024)
	fmt.Printf("  Goroutines:   %d\n", r.Goroutines)
	fmt.Println("")

	fmt.Println("========================================")
	fmt.Println("           压测完成")
	fmt.Println("========================================")
}

// generateBenchmarkMessage 生成压测消息
func generateBenchmarkMessage(viewerID string, seq int) []byte {
	messages := []string{
		"主播好！",
		"这个直播太精彩了！",
		"支持主播！",
		"666",
		"哈哈哈",
		"主播辛苦了",
		"学到了",
		"感谢分享",
	}
	return []byte(fmt.Sprintf("[%s][%d] %s", viewerID, seq, messages[rand.Intn(len(messages))]))
}

// RunBenchmark 运行压测
func RunBenchmark() {
	panic("内存数据源测试已移除，请使用 RunSimpleRedisBenchmark() 或 RunMultiRoomBenchmarkWithRedis()")
}

// RunBenchmarkWithRedis 使用Redis运行压测
func RunBenchmarkWithRedis() {
	// 对接 RunSimpleRedisBenchmark()
	RunSimpleRedisBenchmark()
}

// RunQuickBenchmark 快速压测（用于验证）
func RunQuickBenchmark() {
	panic("内存数据源测试已移除，请使用 RunSimpleRedisBenchmark()")
}

// RunHighLoadBenchmark 高负载压测（4万人）
// RunHighLoadBenchmark 高负载Redis压测（40000观众，5条消息/人）
func RunHighLoadBenchmark() {
	fmt.Println("========================================")
	fmt.Println("          高负载Redis压力测试开始")
	fmt.Println("========================================")
	fmt.Println("配置信息:")
	fmt.Println("  - 房间数量: 1")
	fmt.Println("  - 观众数: 40000")
	fmt.Println("  - 每人发送消息数: 5")
	fmt.Println("  - 消息发送间隔: 8秒")
	fmt.Println("  - 数据源: Redis Stream")
	fmt.Println("----------------------------------------")

	config := BenchmarkConfig{
		TotalViewers:     40000,            // 40000人
		MessagePerViewer: 5,                // 每人5条
		MessageInterval:  8 * time.Second,  // 8秒间隔
		TestDuration:     30 * time.Minute, // 最长30分钟
		RoomNumber:       "high_load_redis_room",
		RoomName:         "高负载Redis压测间",
		UseRedis:         true, // 使用Redis数据源
	}

	benchmark := NewBenchmark(config)
	benchmark.Run()
	benchmark.PrintResult()
}

// RunMultiRoomBenchmark 多房间压测（10个房间，每个房间500人）
// RunMultiRoomBenchmark 多房间压测
func RunMultiRoomBenchmark() {
	panic("内存数据源测试已移除，请使用 RunMultiRoomBenchmarkWithRedis()")
}

// RunSimpleRedisBenchmark 简单Redis数据源压测（单个房间，500人，50条消息）
func RunSimpleRedisBenchmark() {
	fmt.Println("========================================")
	fmt.Println("        简单Redis压力测试开始")
	fmt.Println("========================================")
	fmt.Println("配置信息:")
	fmt.Println("  - 房间数量: 1")
	fmt.Println("  - 观众数: 500")
	fmt.Println("  - 每人发送消息数: 50")
	fmt.Println("  - 消息发送间隔: 3秒")
	fmt.Println("  - 数据源: Redis Stream")
	fmt.Println("----------------------------------------")

	config := BenchmarkConfig{
		TotalViewers:     500,              // 500人
		MessagePerViewer: 50,               // 每人50条
		MessageInterval:  3 * time.Second,  // 3秒间隔
		TestDuration:     10 * time.Minute, // 最长10分钟
		RoomNumber:       "simple_redis_room",
		RoomName:         "简单Redis压测间",
		UseRedis:         true, // 使用Redis数据源
	}

	benchmark := NewBenchmark(config)
	benchmark.Run()
	benchmark.PrintResult()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RunMultiRoomBenchmarkWithRedis Redis技模型压测（10个房间，每个房间500人）
func RunMultiRoomBenchmarkWithRedis() {
	fmt.Println("========================================")
	fmt.Println("       Redis多房间压力测试开始")
	fmt.Println("========================================")
	fmt.Println("配置信息:")
	fmt.Println("  - 房间数量: 10")
	fmt.Println("  - 每个房间观众数: 500")
	fmt.Println("  - 每人发送消息数: 5")
	fmt.Println("  - 消息发送间隔: 2秒/条")
	fmt.Println("  - 数据源: Redis Stream")
	fmt.Println("----------------------------------------")

	startTime := time.Now()
	roomCount := 10
	viewerPerRoom := 500
	messagePerViewer := 5
	messageInterval := 2 * time.Second

	// 并发运行多个房间的压测
	var wg sync.WaitGroup
	totalResults := make([]*BenchmarkResult, 0, roomCount)
	var resultsMu sync.Mutex

	fmt.Printf("\n正在启动 %d 个房间的Redis压测...\n\n", roomCount)

	for roomIdx := 0; roomIdx < roomCount; roomIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			roomNumber := fmt.Sprintf("redis_multi_room_%d", idx)
			roomName := fmt.Sprintf("Redis多房间压测间_%d", idx)

			config := BenchmarkConfig{
				TotalViewers:     viewerPerRoom,
				MessagePerViewer: messagePerViewer,
				MessageInterval:  messageInterval,
				TestDuration:     3 * time.Minute,
				RoomNumber:       roomNumber,
				RoomName:         roomName,
				UseRedis:         true, // 使用Redis数据源
			}

			benchmark := NewBenchmark(config)
			result := benchmark.Run()

			resultsMu.Lock()
			totalResults = append(totalResults, result)
			resultsMu.Unlock()

			// 每个房间完成后打印结果
			fmt.Printf("\n[Redis房间 %2d] 完成 - 发送: %d, 接收: %.2fk, 比率: %.2f:1\n",
				idx,
				result.TotalMessagesSent,
				float64(result.TotalMessagesRecv)/1000,
				float64(result.TotalMessagesRecv)/float64(result.TotalMessagesSent))
		}(roomIdx)
	}

	wg.Wait()

	// 汇总统计
	fmt.Println("\n" + strings.Repeat("=", 42))
	fmt.Println("        Redis多房间压测汇总报告")
	fmt.Println(strings.Repeat("=", 42))

	var totalSent, totalRecv int64
	var maxLatency time.Duration
	var maxMemory uint64
	var maxGoroutines int

	for _, result := range totalResults {
		totalSent += result.TotalMessagesSent
		totalRecv += result.TotalMessagesRecv
		if result.MaxLatency > maxLatency {
			maxLatency = result.MaxLatency
		}
		if result.MemoryUsed > maxMemory {
			maxMemory = result.MemoryUsed
		}
		if result.Goroutines > maxGoroutines {
			maxGoroutines = result.Goroutines
		}
	}

	totalDuration := time.Since(startTime)

	fmt.Println("\n【总体统计】")
	fmt.Printf("  房间数量:       %d\n", roomCount)
	fmt.Printf("  总观众数:       %d\n", roomCount*viewerPerRoom)
	fmt.Printf("  总发送消息:     %d\n", totalSent)
	fmt.Printf("  总接收消息:     %d\n", totalRecv)
	if totalSent > 0 {
		fmt.Printf("  整体发送接收比: %.2f:1\n", float64(totalRecv)/float64(totalSent))
	}
	fmt.Println("")

	fmt.Println("【性能指标】")
	fmt.Printf("  总耗时:         %v\n", totalDuration.Truncate(time.Millisecond))
	fmt.Printf("  平均吞吐量:     %.2f msg/s\n", float64(totalSent)/totalDuration.Seconds())
	fmt.Printf("  最大延迟:       %v\n", maxLatency)
	fmt.Printf("  峰值内存:       %.2f MB\n", float64(maxMemory)/1024/1024)
	fmt.Printf("  最多Goroutines: %d\n", maxGoroutines)
	fmt.Println("")

	fmt.Println("【房间级别统计】")
	for i, result := range totalResults {
		if i%10 == 0 {
			fmt.Printf("房间%3d-%3d: ", i, minInt(i+9, roomCount-1))
		}
		sendRecvRatio := 0.0
		if result.TotalMessagesSent > 0 {
			sendRecvRatio = float64(result.TotalMessagesRecv) / float64(result.TotalMessagesSent)
		}
		fmt.Printf("[%d:%.1f] ", result.TotalMessagesSent, sendRecvRatio)
		if (i+1)%10 == 0 || i == roomCount-1 {
			fmt.Println("")
		}
	}

	fmt.Println("\n" + strings.Repeat("=", 42))
	fmt.Println("        Redis多房间压测完成")
	fmt.Println(strings.Repeat("=", 42))
}

// RunRedisBenchmarkWith10KViewers Redis压力测试（1万人）
func RunRedisBenchmarkWith10KViewers() {
	fmt.Println("========================================")
	fmt.Println("       Redis压力测试（1万人）开始")
	fmt.Println("========================================")
	fmt.Println("配置信息:")
	fmt.Println("  - 房间数量: 1")
	fmt.Println("  - 观众数: 10000")
	fmt.Println("  - 每人发送消息数: 10")
	fmt.Println("  - 消息发送间隔: 5秒")
	fmt.Println("  - 数据源: Redis Stream")
	fmt.Println("----------------------------------------")

	config := BenchmarkConfig{
		TotalViewers:     10000,            // 10000人
		MessagePerViewer: 10,               // 每人10条
		MessageInterval:  5 * time.Second,  // 5秒间隔
		TestDuration:     20 * time.Minute, // 最长20分钟
		RoomNumber:       "redis_10k_room",
		RoomName:         "Redis 10K压测间",
		UseRedis:         true, // 使用Redis数据源
	}

	benchmark := NewBenchmark(config)
	benchmark.Run()
	benchmark.PrintResult()
}

// RunCustomRedisBenchmark 自定义Redis压测（10000观众，20条消息/人，3分钟）
func RunCustomRedisBenchmark() {
	fmt.Println("========================================")
	fmt.Println("          自定义Redis压力测试开始")
	fmt.Println("========================================")
	fmt.Println("配置信息:")
	fmt.Println("  - 房间数量: 1")
	fmt.Println("  - 观众数: 10000")
	fmt.Println("  - 每人发送消息数: 20")
	fmt.Println("  - 消息发送间隔: 1秒")
	fmt.Println("  - 测试持续时间: 3分钟")
	fmt.Println("  - 数据源: Redis Stream")
	fmt.Println("----------------------------------------")

	config := BenchmarkConfig{
		TotalViewers:     10000,           // 10000人
		MessagePerViewer: 20,              // 每人20条
		MessageInterval:  1 * time.Second, // 1秒间隔
		TestDuration:     3 * time.Minute, // 3分钟
		RoomNumber:       "custom_redis_room",
		RoomName:         "自定义Redis压测间",
		UseRedis:         true, // 使用Redis数据源
	}

	benchmark := NewBenchmark(config)
	benchmark.Run()
	benchmark.PrintResult()
}

// Run10KViewer3SecIntervalBenchmark 测试10000观众每人每3秒发一条消息
func Run10KViewer3SecIntervalBenchmark() {
	fmt.Println("========================================")
	fmt.Println("        10000观众3秒消息间隔测试开始")
	fmt.Println("========================================")
	fmt.Println("配置信息:")
	fmt.Println("  - 房间数量: 1")
	fmt.Println("  - 观众数: 10000")
	fmt.Println("  - 每人发送消息数: 100") // 足够多的消息，确保测试期间能持续发送
	fmt.Println("  - 消息发送间隔: 3秒")
	fmt.Println("  - 测试持续时间: 3分钟")
	fmt.Println("  - 数据源: Redis Stream")
	fmt.Println("----------------------------------------")

	config := BenchmarkConfig{
		TotalViewers:     10000,           // 10000人
		MessagePerViewer: 100,             // 每人100条（足够多）
		MessageInterval:  3 * time.Second, // 3秒间隔
		TestDuration:     3 * time.Minute, // 3分钟测试时长
		RoomNumber:       "10k_3sec_room",
		RoomName:         "10K观众3秒间隔测试间",
		UseRedis:         true, // 使用Redis数据源
	}

	benchmark := NewBenchmark(config)
	benchmark.Run()
	benchmark.PrintResult()
}

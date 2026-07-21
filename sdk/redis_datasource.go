package sdk

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const RedisDataSourceRingBuffer = 8192

var storeMaxScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if not current or tonumber(ARGV[1]) > tonumber(current) then
	redis.call("SET", KEYS[1], ARGV[1])
end
local ttl = tonumber(ARGV[2])
if ttl and ttl > 0 then
	redis.call("PEXPIRE", KEYS[1], ttl)
end
return 1
`)

var syncOnlineViewerPresenceScript = redis.NewScript(`
local redisTime = redis.call("TIME")
local now = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
local operation = ARGV[1]
local instanceID = ARGV[2]
local maxViewer = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local expiresAt = now + ttl
local peakRetention = tonumber(ARGV[5])
local viewerCount = tonumber(ARGV[6])

local viewerIDs = {}
for i = 1, viewerCount do
	viewerIDs[i] = ARGV[6 + i]
end

local function storePeak(value)
	local currentPeak = redis.call("GET", KEYS[5])
	if not currentPeak or value > tonumber(currentPeak) then
		redis.call("SET", KEYS[5], value)
	end
	if peakRetention > 0 then
		redis.call("PEXPIRE", KEYS[5], peakRetention)
	end
end

local expiredViewerIDs = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", now)
for _, expiredViewerID in ipairs(expiredViewerIDs) do
	redis.call("HDEL", KEYS[1], expiredViewerID)
end
if #expiredViewerIDs > 0 then
	redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now)
end

local currentGlobalCount = redis.call("HLEN", KEYS[1])
local configuredMaxViewer = redis.call("GET", KEYS[4])
if currentGlobalCount == 0 then
	configuredMaxViewer = tostring(maxViewer)
	redis.call("SET", KEYS[4], configuredMaxViewer)
elseif not configuredMaxViewer then
	configuredMaxViewer = tostring(maxViewer)
	redis.call("SET", KEYS[4], configuredMaxViewer)
elseif operation == "join" and tonumber(configuredMaxViewer) ~= maxViewer then
	storePeak(currentGlobalCount)
			return {-1, currentGlobalCount, 0}

end
local effectiveMaxViewer = tonumber(configuredMaxViewer or maxViewer)

local rejected = {}
if operation == "join" then
	local viewerID = viewerIDs[1]
	local owner = redis.call("HGET", KEYS[1], viewerID)
	if owner then
		if owner == instanceID then
			redis.call("ZADD", KEYS[2], expiresAt, viewerID)
		end
		redis.call("SET", KEYS[3], currentGlobalCount)
		storePeak(currentGlobalCount)
		return {2, currentGlobalCount, 0}
	end
	if currentGlobalCount >= effectiveMaxViewer then
		redis.call("SET", KEYS[3], currentGlobalCount)
		storePeak(currentGlobalCount)
		return {1, currentGlobalCount, 0}
	end
	redis.call("HSET", KEYS[1], viewerID, instanceID)
	redis.call("ZADD", KEYS[2], expiresAt, viewerID)
elseif operation == "leave" then
	local viewerID = viewerIDs[1]
	if redis.call("HGET", KEYS[1], viewerID) == instanceID then
		redis.call("HDEL", KEYS[1], viewerID)
		redis.call("ZREM", KEYS[2], viewerID)
	end
elseif operation == "heartbeat" then
	for _, viewerID in ipairs(viewerIDs) do
		local owner = redis.call("HGET", KEYS[1], viewerID)
		if owner and owner ~= instanceID then
			table.insert(rejected, viewerID)
		elseif not owner then
			local count = redis.call("HLEN", KEYS[1])
			if count >= effectiveMaxViewer then
				table.insert(rejected, viewerID)
			else
				redis.call("HSET", KEYS[1], viewerID, instanceID)
				redis.call("ZADD", KEYS[2], expiresAt, viewerID)
			end
		else
			redis.call("ZADD", KEYS[2], expiresAt, viewerID)
		end
	end
elseif operation == "close" then
	for _, viewerID in ipairs(viewerIDs) do
		if redis.call("HGET", KEYS[1], viewerID) == instanceID then
			redis.call("HDEL", KEYS[1], viewerID)
			redis.call("ZREM", KEYS[2], viewerID)
		end
	end
end

local globalCount = redis.call("HLEN", KEYS[1])
redis.call("SET", KEYS[3], globalCount)
if globalCount == 0 then
	redis.call("DEL", KEYS[1])
	redis.call("DEL", KEYS[2])
	redis.call("DEL", KEYS[4])
elseif ttl > 0 then
	redis.call("PEXPIRE", KEYS[1], ttl * 2)
	redis.call("PEXPIRE", KEYS[2], ttl * 2)
	redis.call("PEXPIRE", KEYS[4], ttl * 2)
end
storePeak(globalCount)
local result = {0, globalCount, #rejected}
for _, viewerID in ipairs(rejected) do
	table.insert(result, viewerID)
end
return result
`)

// 简化的Redis Stream数据源
type RedisDataSource struct {
	rdbClient *redis.ClusterClient
	streams   map[string]*StreamHandler // StreamKey -> Stream处理器
}

// 创建简单数据源
// 创建新的RedisDataSource实例
func NewRedisDataSource(rdbClient *redis.ClusterClient) *RedisDataSource {
	fmt.Printf("NewRedisDataSource被调用\n") // 添加明显的日志
	// 返回RedisDataSource实例
	return &RedisDataSource{
		rdbClient: rdbClient,
		streams:   make(map[string]*StreamHandler),
	}
}

// Client 获取Redis客户端（用于测试和诊断）
func (s *RedisDataSource) Client() *redis.ClusterClient {
	return s.rdbClient
}

// CreateStreamHandler - 为指定房间创建Stream处理器
func (s *RedisDataSource) CreateStreamHandler(ctx context.Context, roomNumber string, streamKey string) {
	// 创建Stream处理器
	handler := &StreamHandler{
		roomNumber:  roomNumber,
		streamKey:   streamKey,
		rdbClient:   s.rdbClient,                   // 设置client引用
		messageChan: make(chan *MessagePb, 100000), // 保留通道用于向房间传递消息
	}
	// 初始化原子计数器
	handler.messageWriteAt.Store(0)
	handler.messageReadAt.Store(0)
	handler.sendWriteAt.Store(0)
	handler.sendReadAt.Store(0)
	// 创建上下文
	handler.ctx, handler.cancel = context.WithCancel(ctx)

	// 保存Stream处理器到映射表
	s.streams[streamKey] = handler

	// 启动发送协程,发送消息到Redis Stream
	go func() {
		fmt.Printf("runSender协程开始执行\n") // 添加日志
		handler.runSender()
	}()

	// 启动接收协程,从Redis Stream接收消息
	go func() {
		fmt.Printf("runReceiver协程开始执行\n") // 添加日志
		handler.runReceiver()
	}()
}

// SendMessage - 发送消息到Stream
func (s *RedisDataSource) SendMessage(ctx context.Context, steamKey string, msg *MessagePb) error {
	handler, ok := s.streams[steamKey]
	if !ok {
		return fmt.Errorf("stream handler not found for stream key %s", steamKey)
	}

	// 使用环形缓冲区发送消息
	return handler.sendToRingBuffer(msg)
}

// GetMessage - 从Stream获取消息
func (s *RedisDataSource) GetMessage(ctx context.Context, streamKey string) []*MessagePb {
	handler, ok := s.streams[streamKey]
	if !ok {
		return nil
	}

	// 从环形缓冲区读取消息
	return handler.readFromRingBuffer()
}

// GetRedisBytesSent 获取发送到Redis的字节数
func (s *RedisDataSource) GetRedisBytesSent(streamKey string) int64 {
	h, ok := s.streams[streamKey]
	if !ok {
		return 0
	}
	return h.redisBytesSent.Load()
}

// GetRedisBytesRecv 获取从Redis接收的字节数
func (s *RedisDataSource) GetRedisBytesRecv(streamKey string) int64 {
	h, ok := s.streams[streamKey]
	if !ok {
		return 0
	}
	return h.redisBytesRecv.Load()
}

// Store 存储键值对到Redis
func (s *RedisDataSource) Store(ctx context.Context, key string, value any, duration time.Duration) error {
	return s.rdbClient.Set(ctx, key, value, duration).Err()
}

// Get 从Redis获取值
func (s *RedisDataSource) Get(ctx context.Context, key string) (string, error) {
	return s.rdbClient.Get(ctx, key).Result()
}

// Accumulated 累计键值对到Redis
func (s *RedisDataSource) AccumulatedBy(ctx context.Context, key string, value int64) error {
	return s.rdbClient.IncrBy(ctx, key, value).Err()
}

// StoreMax 通过 Lua 脚本原子地保留更大的值，避免多实例并发覆盖峰值。
func (s *RedisDataSource) StoreMax(ctx context.Context, key string, value uint32, duration time.Duration) error {
	return storeMaxScript.Run(ctx, s.rdbClient, []string{key}, value, duration.Milliseconds()).Err()
}

// SyncOnlineViewerPresence 原子维护 viewerID 到服务实例的归属、实例租约、全局人数和区间峰值。
func (s *RedisDataSource) SyncOnlineViewerPresence(
	ctx context.Context,
	userOwnerKey string,
	userExpiryKey string,
	totalCountKey string,
	roomMaxKey string,
	peakCountKey string,
	instanceID string,
	operation OnlineViewerOperation,
	viewerIDs []string,
	maxViewer uint32,
	ttl time.Duration,
	peakRetention time.Duration,
) (uint32, OnlineViewerSyncStatus, []string, error) {
	args := make([]any, 0, 6+len(viewerIDs))
	args = append(args,
		string(operation),
		instanceID,
		maxViewer,
		ttl.Milliseconds(),
		peakRetention.Milliseconds(),
		len(viewerIDs),
	)
	for _, viewerID := range viewerIDs {
		args = append(args, viewerID)
	}

	result, err := syncOnlineViewerPresenceScript.Run(
		ctx,
		s.rdbClient,
		[]string{userOwnerKey, userExpiryKey, totalCountKey, roomMaxKey, peakCountKey},
		args...,
	).Slice()
	if err != nil {
		return 0, OnlineViewerSyncAccepted, nil, err
	}
	if len(result) < 3 {
		return 0, OnlineViewerSyncAccepted, nil, fmt.Errorf("unexpected online viewer sync result: %v", result)
	}

	statusValue, ok := result[0].(int64)
	if !ok {
		return 0, OnlineViewerSyncAccepted, nil, fmt.Errorf("unexpected online viewer status value: %T", result[0])
	}
	globalCount, ok := result[1].(int64)
	if !ok || globalCount < 0 || uint64(globalCount) > uint64(^uint32(0)) {
		return 0, OnlineViewerSyncAccepted, nil, fmt.Errorf("unexpected online viewer count value: %v", result[1])
	}
	if statusValue == -1 {
		return uint32(globalCount), OnlineViewerSyncAccepted, nil, ErrRoomMaxMismatch
	}
	status := OnlineViewerSyncStatus(statusValue)
	if status < OnlineViewerSyncAccepted || status > OnlineViewerSyncDuplicate {
		return 0, OnlineViewerSyncAccepted, nil, fmt.Errorf("unexpected online viewer status: %d", statusValue)
	}

	rejectedCount, ok := result[2].(int64)
	if !ok || rejectedCount < 0 || int64(len(result)-3) != rejectedCount {
		return 0, OnlineViewerSyncAccepted, nil, fmt.Errorf("unexpected rejected viewer count: %v", result)
	}
	rejectedViewerIDs := make([]string, 0, rejectedCount)
	for _, item := range result[3:] {
		viewerID, ok := item.(string)
		if !ok {
			return 0, OnlineViewerSyncAccepted, nil, fmt.Errorf("unexpected rejected viewer ID value: %T", item)
		}
		rejectedViewerIDs = append(rejectedViewerIDs, viewerID)
	}
	return uint32(globalCount), status, rejectedViewerIDs, nil
}

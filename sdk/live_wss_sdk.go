package sdk

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type LiveWssSDKConfig struct {
	// Redis配置
	RedisClient *redis.ClusterClient

	// 房间管理器配置
	MaxRooms uint16

	// 默认房间配置
	DefaultMaxViewers uint32

	// OnlinePeakInterval 最高实时在线人数的统计区间，例如 1*time.Minute。
	// 小于等于 0 时默认使用 1 分钟。
	OnlinePeakInterval time.Duration

	// OnlinePeakRetention 每个区间峰值的保留时间，0 表示不过期。
	OnlinePeakRetention time.Duration

	// OnlinePresenceTTL 分布式在线用户租约过期时间，默认 30 秒。
	OnlinePresenceTTL time.Duration

	// CommentCodes 指定应统计和保存的评论消息 code；空值使用默认评论事件码。
	CommentCodes []string

	// CommentRetention 指定评论人数与去重用户集合的 Redis 保留时间；未设置时默认 7 天。
	CommentRetention time.Duration

	// RabbitMQComment 配置评论内容直发 RabbitMQ。URL 为空时不启用 MQ 发布。
	RabbitMQComment RabbitMQCommentPublisherConfig

	// CommentPublisher 允许注入自定义评论 MQ 发布器；非空时优先于 RabbitMQComment。
	CommentPublisher CommentPublisher
}

// LiveWssSDK 初始化器
type LiveWssSDK struct {
	config               *LiveWssSDKConfig
	roomManager          *RoomManager
	dataSource           DataSource
	commentPublisher     CommentPublisher
	ownsCommentPublisher bool
}

// NewSDK 创建新的SDK实例
func NewLiveWssSDK(config *LiveWssSDKConfig) (*LiveWssSDK, error) {
	if config == nil {
		return nil, fmt.Errorf("LiveWssSDK配置不能为空")
	}

	if config.RedisClient == nil {
		return nil, fmt.Errorf("Redis客户端不能为空")
	}

	if config.MaxRooms == 0 {
		config.MaxRooms = 100 // 默认100个房间
	}

	if config.DefaultMaxViewers == 0 {
		config.DefaultMaxViewers = 100000 // 默认10万人
	}
	roomConfig := RoomConfig{
		OnlinePeakInterval:  config.OnlinePeakInterval,
		OnlinePeakRetention: config.OnlinePeakRetention,
		OnlinePresenceTTL:   config.OnlinePresenceTTL,
		CommentCodes:        config.CommentCodes,
		CommentRetention:    config.CommentRetention,
	}.withDefaults()
	config.OnlinePeakInterval = roomConfig.OnlinePeakInterval
	config.OnlinePeakRetention = roomConfig.OnlinePeakRetention
	config.OnlinePresenceTTL = roomConfig.OnlinePresenceTTL
	config.CommentCodes = append([]string(nil), roomConfig.CommentCodes...)
	config.CommentRetention = roomConfig.CommentRetention

	commentPublisher := config.CommentPublisher
	ownsCommentPublisher := false
	if commentPublisher == nil && config.RabbitMQComment.URL != "" {
		publisher, err := NewRabbitMQCommentPublisher(config.RabbitMQComment)
		if err != nil {
			fmt.Printf("创建RabbitMQ评论发布器失败: %v", err)
			return nil, err
		}
		commentPublisher = publisher
		ownsCommentPublisher = true
	}

	// 创建Redis数据源
	redisDataSource := NewRedisDataSource(config.RedisClient)

	// 创建房间管理器
	roomManager := NewRoomManager(config.MaxRooms)

	liveWssSDK := &LiveWssSDK{
		config:               config,
		roomManager:          roomManager,
		dataSource:           redisDataSource,
		commentPublisher:     commentPublisher,
		ownsCommentPublisher: ownsCommentPublisher,
	}

	return liveWssSDK, nil
}

// GetRoomManager 获取房间管理器
func (s *LiveWssSDK) GetRoomManager() *RoomManager {
	return s.roomManager
}

// GetDataSource 获取数据源
func (s *LiveWssSDK) GetDataSource() DataSource {
	return s.dataSource
}

// CreateRoom 创建房间
func (s *LiveWssSDK) CreateRoom(ctx context.Context, roomNumber string, roomName string, maxViewers uint32, firmUUID string) error {
	if maxViewers == 0 {
		maxViewers = s.config.DefaultMaxViewers
	}

	// 创建Redis Stream处理器
	streamKey := fmt.Sprintf(Live_Msg_Broadcast, firmUUID, roomNumber)
	if redisDS, ok := s.dataSource.(*RedisDataSource); ok {
		redisDS.CreateStreamHandler(ctx, roomNumber, streamKey)
	}

	// 创建Redis Stream处理器,高优先级
	HpStreamKey := fmt.Sprintf(Live_Msg_Broadcast+":hp", firmUUID, roomNumber)
	if redisDS, ok := s.dataSource.(*RedisDataSource); ok {
		redisDS.CreateStreamHandler(ctx, roomNumber, HpStreamKey)
	}

	// 创建房间
	room, err := NewRoomWithConfig(ctx, roomName, roomNumber, maxViewers, firmUUID, RoomConfig{
		OnlinePeakInterval:  s.config.OnlinePeakInterval,
		OnlinePeakRetention: s.config.OnlinePeakRetention,
		OnlinePresenceTTL:   s.config.OnlinePresenceTTL,
		CommentCodes:        s.config.CommentCodes,
		CommentRetention:    s.config.CommentRetention,
		CommentPublisher:    s.commentPublisher,
	})
	if err != nil {
		return err
	}

	// 启动房间
	room.Start(s.dataSource)

	// 添加到房间管理器
	return s.roomManager.SetRoom(room)
}

// GetRoom 获取房间
func (s *LiveWssSDK) GetRoom(roomNumber string) *Room {
	return s.roomManager.GetRoom(roomNumber)
}

// RemoveRoom 删除房间
func (s *LiveWssSDK) RemoveRoom(roomNumber string) {
	s.roomManager.RemoveRoom(roomNumber)
}

// GetRoomManagerInfo 获取房间管理器信息
func (s *LiveWssSDK) GetRoomManagerInfo() string {
	return s.roomManager.Info()
}

// Close 关闭SDK
func (s *LiveWssSDK) Close() {
	if s.ownsCommentPublisher && s.commentPublisher != nil {
		_ = s.commentPublisher.Close()
	}
}

func (s *LiveWssSDK) GetRooms() map[string]*Room {
	return s.roomManager.GetRooms()
}

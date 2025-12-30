package sdk

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type LiveWssSDKConfig struct {
	// Redis配置
	RedisClient *redis.ClusterClient

	// 房间管理器配置
	MaxRooms uint16

	// 默认房间配置
	DefaultMaxViewers uint32
}

// LiveWssSDK 初始化器
type LiveWssSDK struct {
	config      *LiveWssSDKConfig
	roomManager *RoomManager
	dataSource  DataSource
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

	// 创建Redis数据源
	redisDataSource := NewRedisDataSource(config.RedisClient)

	// 创建房间管理器
	roomManager := NewRoomManager(config.MaxRooms)

	liveWssSDK := &LiveWssSDK{
		config:      config,
		roomManager: roomManager,
		dataSource:  redisDataSource,
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
	room, err := NewRoom(ctx, roomName, roomNumber, maxViewers, firmUUID)
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
	// 这里可以添加清理逻辑，如关闭Redis连接等
	// 目前主要依赖上下文取消机制
}

func (s *LiveWssSDK) GetRooms() map[string]*Room {
	return s.roomManager.GetRooms()
}

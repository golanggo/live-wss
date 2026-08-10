package sdk

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// recordTotalViewerScript 原子记录一名观看用户，并同步累计去重观看人数。
var recordTotalViewerScript = redis.NewScript(`
redis.call("SADD", KEYS[1], ARGV[1])
local totalViewerCount = redis.call("SCARD", KEYS[1])
redis.call("SET", KEYS[2], totalViewerCount)
local ttl = tonumber(ARGV[2])
if ttl and ttl > 0 then
  redis.call("PEXPIRE", KEYS[1], ttl)
  redis.call("PEXPIRE", KEYS[2], ttl)
end
return totalViewerCount
`)

// LiveTotalViewerCountKey 返回本场直播累计去重观看人数键。
func LiveTotalViewerCountKey(firmUUID, roomNumber string) string {
	return fmt.Sprintf(Live_Total_User_Count, firmUUID, roomNumber)
}

// LiveTotalViewerSetKey 返回本场直播累计去重观看用户集合键。
func LiveTotalViewerSetKey(firmUUID, roomNumber string) string {
	return fmt.Sprintf(Live_Total_User_Set, firmUUID, roomNumber)
}

// RecordTotalViewer 按 viewerID 原子去重统计本场直播累计观看人数。
func (s *RedisDataSource) RecordTotalViewer(ctx context.Context, userSetKey, userCountKey, viewerID string, retention time.Duration) (uint32, error) {
	if s == nil || s.rdbClient == nil {
		return 0, fmt.Errorf("Redis 数据源未初始化")
	}
	if viewerID == "" {
		return 0, fmt.Errorf("观看用户 ID 不能为空")
	}
	result, err := recordTotalViewerScript.Run(ctx, s.rdbClient,
		[]string{userSetKey, userCountKey}, viewerID, retention.Milliseconds()).Result()
	if err != nil {
		return 0, err
	}
	count, err := strconv.ParseUint(redisValueString(result), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("解析直播总人数失败: %w", err)
	}
	return uint32(count), nil
}

// ResetTotalViewers 清空新场次开始前的累计去重观看用户集合和人数。
func (s *RedisDataSource) ResetTotalViewers(ctx context.Context, userSetKey, userCountKey string) error {
	if s == nil || s.rdbClient == nil {
		return fmt.Errorf("Redis 数据源未初始化")
	}
	return s.rdbClient.Del(ctx, userSetKey, userCountKey).Err()
}

func (r *Room) totalViewerDataSource() (TotalViewerDataSource, bool) {
	dataSource, ok := r.dataSource.(TotalViewerDataSource)
	return dataSource, ok
}

func (r *Room) recordTotalViewer(ctx context.Context, viewerID string) error {
	dataSource, ok := r.totalViewerDataSource()
	if !ok {
		return nil
	}
	_, err := dataSource.RecordTotalViewer(
		ctx,
		LiveTotalViewerSetKey(r.firmUUID, r.roomNumber),
		LiveTotalViewerCountKey(r.firmUUID, r.roomNumber),
		viewerID,
		r.totalViewerRetention,
	)
	return err
}

// GetLiveTotalViewerCount 查询本场直播累计去重观看人数。
func (r *Room) GetLiveTotalViewerCount(ctx context.Context) (uint32, error) {
	if r.dataSource == nil {
		return 0, fmt.Errorf("房间数据源未初始化")
	}
	if ctx == nil {
		ctx = r.roomCtx
	}
	value, err := r.dataSource.Get(ctx, LiveTotalViewerCountKey(r.firmUUID, r.roomNumber))
	if err != nil {
		return 0, err
	}
	count, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("解析直播总人数失败: %w", err)
	}
	return uint32(count), nil
}

// ResetLiveTotalViewers 清空新场次开始前的累计去重观看人数。
// 同一 roomNumber 复播前应仅由直播编排层调用一次。
func (r *Room) ResetLiveTotalViewers(ctx context.Context) error {
	dataSource, ok := r.totalViewerDataSource()
	if !ok {
		return fmt.Errorf("当前数据源未实现直播总人数重置能力")
	}
	if ctx == nil {
		ctx = r.roomCtx
	}
	return dataSource.ResetTotalViewers(
		ctx,
		LiveTotalViewerSetKey(r.firmUUID, r.roomNumber),
		LiveTotalViewerCountKey(r.firmUUID, r.roomNumber),
	)
}

var _ TotalViewerDataSource = (*RedisDataSource)(nil)

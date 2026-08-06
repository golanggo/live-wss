package sdk

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// recordCommentUserScript 在同一 Redis Cluster 槽内原子更新评论用户去重集合和人数。
var recordCommentUserScript = redis.NewScript(`
redis.call("SADD", KEYS[1], ARGV[1])
local userCount = redis.call("SCARD", KEYS[1])
redis.call("SET", KEYS[2], userCount)

local ttl = tonumber(ARGV[2])
if ttl and ttl > 0 then
  redis.call("PEXPIRE", KEYS[1], ttl)
  redis.call("PEXPIRE", KEYS[2], ttl)
end
return userCount
`)

// RecordCommentUser 按 viewerID 去重统计本场直播的评论人数。
func (s *RedisDataSource) RecordCommentUser(ctx context.Context, userSetKey, userCountKey, viewerID string, retention time.Duration) (uint32, error) {
	if s == nil || s.rdbClient == nil {
		return 0, fmt.Errorf("Redis 数据源未初始化")
	}
	if viewerID == "" {
		return 0, fmt.Errorf("评论用户 ID 不能为空")
	}

	result, err := recordCommentUserScript.Run(ctx, s.rdbClient,
		[]string{userSetKey, userCountKey}, viewerID, retention.Milliseconds()).Result()
	if err != nil {
		return 0, err
	}
	count, err := strconv.ParseUint(redisValueString(result), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("解析评论人数失败: %w", err)
	}
	return uint32(count), nil
}

// ResetCommentUsers 清空新场次开始前的评论去重集合与人数。
func (s *RedisDataSource) ResetCommentUsers(ctx context.Context, userSetKey, userCountKey string) error {
	if s == nil || s.rdbClient == nil {
		return fmt.Errorf("Redis 数据源未初始化")
	}
	return s.rdbClient.Del(ctx, userSetKey, userCountKey).Err()
}

func redisValueString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

var _ CommentStatisticsDataSource = (*RedisDataSource)(nil)

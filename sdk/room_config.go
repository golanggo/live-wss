package sdk

import "time"

const (
	DefaultOnlinePeakInterval   = 1 * time.Minute
	DefaultOnlinePresenceTTL    = 30 * time.Second
	DefaultCommentRetention     = 7 * 24 * time.Hour
	DefaultTotalViewerRetention = 7 * 24 * time.Hour
)

// RoomConfig 控制单个直播间的运行参数。
type RoomConfig struct {
	// OnlinePeakInterval 指定最高实时在线人数的统计区间。
	// 小于等于 0 时使用 DefaultOnlinePeakInterval。
	OnlinePeakInterval time.Duration

	// OnlinePeakRetention 指定每个区间峰值在数据源中的保留时间。
	// 0 表示不过期，负数会被归一化为 0。
	OnlinePeakRetention time.Duration

	// OnlinePresenceTTL 指定分布式在线用户租约的过期时间。
	// 实例异常退出后，其持有的用户最迟会在该时间后从全局在线人数中剔除。
	OnlinePresenceTTL time.Duration

	// TotalViewerRetention 指定直播累计去重观看人数与用户集合的 Redis 保留时间。
	// 小于等于 0 时使用 DefaultTotalViewerRetention；应覆盖直播时长和查询窗口。
	TotalViewerRetention time.Duration

	// CommentCodes 指定应统计评论人数并发送到 MQ 的消息 code。
	// 空值时使用 Code_Event_User_Send_Comment。
	CommentCodes []string

	// CommentRetention 指定评论人数与去重用户集合的 Redis 保留时间。
	// 小于等于 0 时使用 DefaultCommentRetention。
	CommentRetention time.Duration

	// CommentPublisher 是可选的评论 MQ 发布器。配置后，评论内容将直接发送到 MQ，
	// 不会额外保存到独立的 Redis 评论内容键。
	CommentPublisher CommentPublisher

	// AdaptiveSamplingRules 按全局在线人数分层随机保留低优先级消息。
	// 空值时关闭自适应采样；高优先级消息始终绕过该策略。
	AdaptiveSamplingRules []AdaptiveSamplingRule
}

func (c RoomConfig) withDefaults() RoomConfig {
	if c.OnlinePeakInterval <= 0 {
		c.OnlinePeakInterval = DefaultOnlinePeakInterval
	}
	if c.OnlinePeakRetention < 0 {
		c.OnlinePeakRetention = 0
	}
	if c.OnlinePresenceTTL <= 0 {
		c.OnlinePresenceTTL = DefaultOnlinePresenceTTL
	}
	if c.TotalViewerRetention <= 0 {
		c.TotalViewerRetention = DefaultTotalViewerRetention
	}
	if len(c.CommentCodes) == 0 {
		c.CommentCodes = []string{Code_Event_Send_Msg}
	} else {
		c.CommentCodes = append([]string(nil), c.CommentCodes...)
	}
	if c.CommentRetention <= 0 {
		c.CommentRetention = DefaultCommentRetention
	}
	c.AdaptiveSamplingRules = append([]AdaptiveSamplingRule(nil), c.AdaptiveSamplingRules...)
	return c
}

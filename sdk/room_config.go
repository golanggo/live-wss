package sdk

import "time"

const (
	DefaultOnlinePeakInterval = 1 * time.Minute
	DefaultOnlinePresenceTTL  = 30 * time.Second
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
	return c
}

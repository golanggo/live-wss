package sdk

import (
	"fmt"
	"sort"
	"sync/atomic"
)

// AdaptiveSamplingRule 定义一个在线人数区间内低优先级消息的保留比例。
// MinOnlineViewers 为该区间的下界；例如 1000 / 0.25 表示在线人数达到 1000 时，
// 随机保留约 25% 的低优先级消息。高优先级消息永不经过该采样器。
type AdaptiveSamplingRule struct {
	MinOnlineViewers uint32
	KeepRatio        float64
}

// AdaptiveSamplingStats 返回该房间自适应采样的观测数据。
type AdaptiveSamplingStats struct {
	Enabled            bool
	OnlineViewers      uint32
	KeepRatio          float64
	DroppedLowPriority uint64
}

type adaptiveMessageSampler struct {
	rules    []AdaptiveSamplingRule
	sequence atomic.Uint64
}

func newAdaptiveMessageSampler(rules []AdaptiveSamplingRule) (*adaptiveMessageSampler, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	copied := append([]AdaptiveSamplingRule(nil), rules...)
	sort.Slice(copied, func(i, j int) bool {
		return copied[i].MinOnlineViewers < copied[j].MinOnlineViewers
	})
	for i, rule := range copied {
		if rule.KeepRatio < 0 || rule.KeepRatio > 1 {
			return nil, fmt.Errorf("adaptive sampling rule %d has invalid keep ratio %f; it must be in [0, 1]", i, rule.KeepRatio)
		}
		if i > 0 && rule.MinOnlineViewers == copied[i-1].MinOnlineViewers {
			return nil, fmt.Errorf("adaptive sampling rules must not repeat min online viewers %d", rule.MinOnlineViewers)
		}
	}
	return &adaptiveMessageSampler{rules: copied}, nil
}

func (s *adaptiveMessageSampler) keepRatio(onlineViewers uint32) float64 {
	if s == nil || len(s.rules) == 0 {
		return 1
	}
	selected := s.rules[0]
	for _, rule := range s.rules[1:] {
		if rule.MinOnlineViewers > onlineViewers {
			break
		}
		selected = rule
	}
	return selected.KeepRatio
}

func (s *adaptiveMessageSampler) allow(onlineViewers uint32) bool {
	if s == nil {
		return true
	}
	ratio := s.keepRatio(onlineViewers)
	if ratio <= 0 {
		return false
	}
	if ratio >= 1 {
		return true
	}
	// SplitMix64 提供无锁、均匀的伪随机采样；比共享 math/rand 状态更适合高并发入站消息。
	x := s.sequence.Add(1) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x = x ^ (x >> 31)
	return x <= uint64(ratio*float64(^uint64(0)))
}

// SetAdaptiveMessageSampling 设置按全局在线人数分层的低优先级消息采样规则。
// 传入空切片会关闭该策略。规则会被复制，调用方后续修改其切片不会影响房间行为。
func (r *Room) SetAdaptiveMessageSampling(rules []AdaptiveSamplingRule) error {
	if r == nil {
		return fmt.Errorf("room is nil")
	}
	sampler, err := newAdaptiveMessageSampler(rules)
	if err != nil {
		return err
	}
	r.adaptiveMessageSampler.Store(sampler)
	r.adaptiveDroppedLowPriority.Store(0)
	return nil
}

// GetAdaptiveMessageSamplingStats 返回当前生效比例和累计被采样丢弃的低优先级消息数。
func (r *Room) GetAdaptiveMessageSamplingStats() AdaptiveSamplingStats {
	if r == nil {
		return AdaptiveSamplingStats{}
	}
	sampler := r.adaptiveMessageSampler.Load()
	online := r.GetOnlineViewerCount()
	if online == 0 {
		// 加入、离开同步的传播存在短暂滞后，冷启动时使用本地承载人数避免错误地按零人数档位放行。
		online = r.GetLocalOnlineViewerCount()
	}
	stats := AdaptiveSamplingStats{
		Enabled:            sampler != nil,
		OnlineViewers:      online,
		KeepRatio:          1,
		DroppedLowPriority: r.adaptiveDroppedLowPriority.Load(),
	}
	if sampler != nil {
		stats.KeepRatio = sampler.keepRatio(online)
	}
	return stats
}

func (r *Room) allowAdaptiveLowPriorityMessage(msg *MessagePb) bool {
	if r == nil || msg == nil || msg.Priority == MessagePriority_HIGH {
		return true
	}
	sampler := r.adaptiveMessageSampler.Load()
	if sampler == nil {
		return true
	}
	online := r.GetOnlineViewerCount()
	if online == 0 {
		online = r.GetLocalOnlineViewerCount()
	}
	if sampler.allow(online) {
		return true
	}
	r.adaptiveDroppedLowPriority.Add(1)
	return false
}

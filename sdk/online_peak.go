package sdk

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

// OnlineViewerPeak 描述一个统计区间内的最高实时在线人数。
type OnlineViewerPeak struct {
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Count       uint32    `json:"count"`
}

// collectOnlineViewerPeak 在自然时间边界滚动统计区间。
func (r *Room) collectOnlineViewerPeak() {
	for {
		now := time.Now()
		nextWindow := now.Truncate(r.onlinePeakInterval).Add(r.onlinePeakInterval)
		timer := time.NewTimer(time.Until(nextWindow))

		select {
		case <-r.roomCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case firedAt := <-timer.C:
			r.observeOnlineViewerCount(firedAt, r.GetOnlineViewerCount())
		}
	}
}

// observeOnlineViewerCount 记录实时在线人数，并在进入新时间区间时落库上一段峰值。
func (r *Room) observeOnlineViewerCount(observedAt time.Time, count uint32) {
	windowStart := observedAt.Truncate(r.onlinePeakInterval)
	var completedStart time.Time
	var completedPeak uint32

	r.onlinePeakMu.Lock()
	if r.onlinePeakWindowStart.IsZero() {
		r.onlinePeakWindowStart = windowStart
	}

	if windowStart.Before(r.onlinePeakWindowStart) {
		r.onlinePeakMu.Unlock()
		return
	}
	if windowStart.After(r.onlinePeakWindowStart) {
		completedStart = r.onlinePeakWindowStart
		completedPeak = r.onlinePeakCount
		r.onlinePeakWindowStart = windowStart
		r.onlinePeakCount = count
	} else if count > r.onlinePeakCount {
		r.onlinePeakCount = count
	}
	r.onlinePeakMu.Unlock()

	if !completedStart.IsZero() {
		r.persistOnlineViewerPeak(completedStart, completedPeak)
	}
}

func (r *Room) persistOnlineViewerPeak(windowStart time.Time, peak uint32) {
	if r.dataSource == nil {
		return
	}

	key := OnlineViewerPeakKey(r.firmUUID, r.roomNumber, windowStart)
	var err error
	if maxStore, ok := r.dataSource.(MaxValueDataSource); ok {
		err = maxStore.StoreMax(r.roomCtx, key, peak, r.onlinePeakRetention)
	} else {
		err = r.dataSource.Store(r.roomCtx, key, peak, r.onlinePeakRetention)
	}
	if err != nil {
		log.Printf("存储区间最高在线人数失败: %v, 房间: %s, 区间开始: %s", err, r.roomNumber, windowStart.Format(time.RFC3339))
	}

	// 该键保存本场直播内所有时间桶的最大值，而非当前桶的值。
	// Redis 数据源使用 Lua 原子最大值写入，多个服务实例不会以较小值覆盖较大值。
	r.persistLiveOnlineViewerPeakMax(peak)
}

// persistLiveOnlineViewerPeakMax 更新本场直播已持久化的最大区间峰值。
func (r *Room) persistLiveOnlineViewerPeakMax(peak uint32) {
	if err := r.storeLiveOnlineViewerPeakMax(r.roomCtx, peak); err != nil {
		log.Printf("存储直播最高在线人数失败: %v, 房间: %s", err, r.roomNumber)
	}
}

// storeLiveOnlineViewerPeakMax 原子保留本场直播各时间桶中的最大在线人数。
// MaxValueDataSource 会在每次写入时续期，避免长直播在高峰后因不再创新高而让键提前过期。
func (r *Room) storeLiveOnlineViewerPeakMax(ctx context.Context, peak uint32) error {
	if r.dataSource == nil {
		return fmt.Errorf("房间数据源未初始化")
	}
	if ctx == nil {
		ctx = r.roomCtx
	}

	key := LiveOnlineViewerPeakMaxKey(r.firmUUID, r.roomNumber)
	if maxStore, ok := r.dataSource.(MaxValueDataSource); ok {
		if err := maxStore.StoreMax(ctx, key, peak, r.onlinePeakRetention); err != nil {
			return err
		}
		r.recordLiveOnlineViewerPeakMax(peak)
		return nil
	}

	// 兼容旧的自定义 DataSource：同一 Room 进程内只允许更大值覆盖。
	// 生产分布式部署必须实现 MaxValueDataSource，才能获得跨实例原子性。
	r.onlinePeakMaxMu.Lock()
	defer r.onlinePeakMaxMu.Unlock()
	if peak <= r.onlinePeakMaxCount.Load() {
		return nil
	}
	if err := r.dataSource.Store(ctx, key, peak, r.onlinePeakRetention); err != nil {
		return err
	}
	r.onlinePeakMaxCount.Store(peak)
	return nil
}

func (r *Room) recordLiveOnlineViewerPeakMax(peak uint32) {
	for {
		current := r.onlinePeakMaxCount.Load()
		if peak <= current || r.onlinePeakMaxCount.CompareAndSwap(current, peak) {
			return
		}
	}
}

// storeOnlineViewerSummary 持久化当前在线人数，并刷新当前区间峰值。
func (r *Room) storeOnlineViewerSummary() {
	if r.dataSource == nil {
		return
	}

	current := r.refreshDistributedOnlineViewerCount()
	if _, distributed := r.distributedOnlineDataSource(); !distributed {
		r.storeCurrentOnlineViewerCount(current)
	}
	peak := r.GetCurrentOnlineViewerPeak()
	r.persistOnlineViewerPeak(peak.WindowStart, peak.Count)
}

func (r *Room) storeCurrentOnlineViewerCount(count uint32) {
	if r.dataSource == nil {
		return
	}
	key := fmt.Sprintf(Live_Online_User_Count, r.firmUUID, r.roomNumber)
	if err := r.dataSource.Store(r.roomCtx, key, count, 0); err != nil {
		log.Printf("存储实时在线人数失败: %v, 房间: %s", err, r.roomNumber)
	}
}

// flushCurrentOnlineViewerPeak 在房间关闭前持久化尚未结束的当前区间。
func (r *Room) flushCurrentOnlineViewerPeak() {
	peak := r.GetCurrentOnlineViewerPeak()
	r.persistOnlineViewerPeak(peak.WindowStart, peak.Count)
}

// GetCurrentOnlineViewerPeak 获取当前统计区间及其最高实时在线人数快照。
func (r *Room) GetCurrentOnlineViewerPeak() OnlineViewerPeak {
	r.onlinePeakMu.Lock()
	defer r.onlinePeakMu.Unlock()

	return OnlineViewerPeak{
		WindowStart: r.onlinePeakWindowStart,
		WindowEnd:   r.onlinePeakWindowStart.Add(r.onlinePeakInterval),
		Count:       r.onlinePeakCount,
	}
}

// GetOnlineViewerPeak 查询给定时间所在区间的最高实时在线人数。
func (r *Room) GetOnlineViewerPeak(ctx context.Context, at time.Time) (OnlineViewerPeak, error) {
	windowStart := at.Truncate(r.onlinePeakInterval)
	result := OnlineViewerPeak{
		WindowStart: windowStart,
		WindowEnd:   windowStart.Add(r.onlinePeakInterval),
	}
	if r.dataSource == nil {
		return result, fmt.Errorf("房间数据源未初始化")
	}

	value, err := r.dataSource.Get(ctx, OnlineViewerPeakKey(r.firmUUID, r.roomNumber, windowStart))
	if err != nil {
		return result, err
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return result, fmt.Errorf("解析区间最高在线人数失败: %w", err)
	}
	result.Count = uint32(parsed)
	return result, nil
}

// OnlineViewerPeakKey 返回指定时间桶对应的数据源键。
func OnlineViewerPeakKey(firmUUID, roomNumber string, windowStart time.Time) string {
	return fmt.Sprintf(Live_Online_User_Peak_Count, firmUUID, roomNumber, windowStart.UnixMilli())
}

// LiveOnlineViewerPeakMaxKey 返回本场直播所有时间桶中的最高在线人数键。
func LiveOnlineViewerPeakMaxKey(firmUUID, roomNumber string) string {
	return fmt.Sprintf(Live_Online_User_Peak_Max_Count, firmUUID, roomNumber)
}

// GetLiveOnlineViewerPeakMax 查询本场直播所有时间桶的最高在线人数。
func (r *Room) GetLiveOnlineViewerPeakMax(ctx context.Context) (uint32, error) {
	if r.dataSource == nil {
		return 0, fmt.Errorf("房间数据源未初始化")
	}
	if ctx == nil {
		ctx = r.roomCtx
	}

	value, err := r.dataSource.Get(ctx, LiveOnlineViewerPeakMaxKey(r.firmUUID, r.roomNumber))
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("解析直播最高在线人数失败: %w", err)
	}
	return uint32(parsed), nil
}

// GetOnlinePeakInterval 获取房间当前使用的峰值统计区间。
func (r *Room) GetOnlinePeakInterval() time.Duration {
	return r.onlinePeakInterval
}

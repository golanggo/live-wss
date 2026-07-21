package sdk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"
)

func newRoomInstanceID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("生成房间实例 ID 失败: %w", err)
	}
	return hex.EncodeToString(random[:]), nil
}

func (r *Room) distributedOnlineDataSource() (DistributedOnlineDataSource, bool) {
	dataSource, ok := r.dataSource.(DistributedOnlineDataSource)
	return dataSource, ok
}

func (r *Room) onlinePresenceKeys() (userOwnerKey, userExpiryKey, totalCountKey, roomMaxKey string) {
	return fmt.Sprintf(Live_Online_User_Owner, r.firmUUID, r.roomNumber),
		fmt.Sprintf(Live_Online_User_Expiry, r.firmUUID, r.roomNumber),
		fmt.Sprintf(Live_Online_User_Count, r.firmUUID, r.roomNumber),
		fmt.Sprintf(Live_Online_Room_Max, r.firmUUID, r.roomNumber)
}

// syncOnlineViewerPresence 原子更新分布式用户会话。调用方必须持有 viewerMux 的读锁或写锁。
func (r *Room) syncOnlineViewerPresence(
	ctx context.Context,
	operation OnlineViewerOperation,
	viewerIDs []string,
	fallbackLocalCount uint32,
) (uint32, OnlineViewerSyncStatus, []string, error) {
	dataSource, distributed := r.distributedOnlineDataSource()
	if !distributed {
		if operation == OnlineViewerJoin && fallbackLocalCount > r.maxViewer {
			return r.distributedOnlineViewer.Load(), OnlineViewerSyncRoomFull, nil, nil
		}
		r.distributedOnlineViewer.Store(fallbackLocalCount)
		r.observeOnlineViewerCount(time.Now(), fallbackLocalCount)
		return fallbackLocalCount, OnlineViewerSyncAccepted, nil, nil
	}

	userOwnerKey, userExpiryKey, totalCountKey, roomMaxKey := r.onlinePresenceKeys()
	peakWindowStart := time.Now().Truncate(r.onlinePeakInterval)
	peakCountKey := OnlineViewerPeakKey(r.firmUUID, r.roomNumber, peakWindowStart)
	globalCount, status, rejectedViewerIDs, err := dataSource.SyncOnlineViewerPresence(
		ctx,
		userOwnerKey,
		userExpiryKey,
		totalCountKey,
		roomMaxKey,
		peakCountKey,
		r.instanceID,
		operation,
		viewerIDs,
		r.maxViewer,
		r.onlinePresenceTTL,
		r.onlinePeakRetention,
	)
	if err != nil {
		return r.distributedOnlineViewer.Load(), OnlineViewerSyncAccepted, nil, fmt.Errorf("%w: %w", ErrOnlinePresenceSync, err)
	}

	r.distributedOnlineViewer.Store(globalCount)
	r.observeOnlineViewerCount(time.Now(), globalCount)
	return globalCount, status, rejectedViewerIDs, nil
}

// localViewerIDsLocked 返回本实例观众快照。调用方必须持有 viewerMux 的读锁或写锁。
func (r *Room) localViewerIDsLocked() []string {
	viewerIDs := make([]string, 0, len(r.viewers))
	for viewerID := range r.viewers {
		viewerIDs = append(viewerIDs, viewerID)
	}
	return viewerIDs
}

// RefreshOnlineViewerCount 主动同步本实例用户租约，并返回清理过期租约后的最新全局在线人数。
// GetOnlineViewerCount 只读取本地原子缓存；需要强一致读取时调用本方法。
func (r *Room) RefreshOnlineViewerCount(ctx context.Context) (uint32, error) {
	if ctx == nil {
		ctx = r.roomCtx
	}
	r.viewerMux.RLock()
	viewerIDs := r.localViewerIDsLocked()
	globalCount, _, rejectedViewerIDs, err := r.syncOnlineViewerPresence(
		ctx,
		OnlineViewerHeartbeat,
		viewerIDs,
		uint32(len(viewerIDs)),
	)
	r.viewerMux.RUnlock()
	if err != nil {
		return globalCount, err
	}
	if len(rejectedViewerIDs) > 0 {
		r.removeRejectedLocalViewers(rejectedViewerIDs)
	}
	return globalCount, nil
}

func (r *Room) refreshDistributedOnlineViewerCount() uint32 {
	globalCount, err := r.RefreshOnlineViewerCount(r.roomCtx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("刷新分布式在线人数失败: %v, 房间: %s", err, r.roomNumber)
	}
	return globalCount
}

// removeRejectedLocalViewers 移除已归属于其他实例或因全局容量不足无法恢复租约的本地连接。
func (r *Room) removeRejectedLocalViewers(viewerIDs []string) {
	r.viewerMux.Lock()
	viewers := make([]*Viewer, 0, len(viewerIDs))
	for _, viewerID := range viewerIDs {
		viewer, exists := r.viewers[viewerID]
		if !exists {
			continue
		}
		delete(r.viewers, viewerID)
		r.leaveRoomViewerCnt.Add(1)
		viewers = append(viewers, viewer)
	}
	r.onlineViewer.Store(uint32(len(r.viewers)))
	r.viewerMux.Unlock()

	for _, viewer := range viewers {
		viewer.Close()
	}
}

// collectOnlineViewerPresence 定期刷新实例租约；实例异常退出后，其用户会在 TTL 后被原子清理。
func (r *Room) collectOnlineViewerPresence() {
	if _, distributed := r.distributedOnlineDataSource(); !distributed {
		return
	}

	refreshInterval := r.onlinePresenceTTL / 3
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	r.refreshDistributedOnlineViewerCount()
	for {
		select {
		case <-r.roomCtx.Done():
			return
		case <-ticker.C:
			r.refreshDistributedOnlineViewerCount()
		}
	}
}

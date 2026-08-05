package sdk

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type distributedOwner struct {
	instanceID string
	expiresAt  time.Time
}

type distributedMemoryDataSource struct {
	*memoryDataSource

	presenceMu sync.Mutex
	owners     map[string]map[string]distributedOwner
	roomMax    map[string]uint32
	now        func() time.Time
	syncErr    error
}

func newDistributedMemoryDataSource() *distributedMemoryDataSource {
	return &distributedMemoryDataSource{
		memoryDataSource: newMemoryDataSource(),
		owners:           make(map[string]map[string]distributedOwner),
		roomMax:          make(map[string]uint32),
		now:              time.Now,
	}
}

func (d *distributedMemoryDataSource) SyncOnlineViewerPresence(
	_ context.Context,
	userOwnerKey string,
	_ string,
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
	d.presenceMu.Lock()
	defer d.presenceMu.Unlock()

	if d.syncErr != nil {
		return 0, OnlineViewerSyncAccepted, nil, d.syncErr
	}

	now := d.now()
	owners := d.owners[userOwnerKey]
	if owners == nil {
		owners = make(map[string]distributedOwner)
		d.owners[userOwnerKey] = owners
	}
	for viewerID, owner := range owners {
		if !owner.expiresAt.After(now) {
			delete(owners, viewerID)
		}
	}

	if len(owners) == 0 {
		d.roomMax[roomMaxKey] = maxViewer
	} else if operation == OnlineViewerJoin && d.roomMax[roomMaxKey] != maxViewer {
		return uint32(len(owners)), OnlineViewerSyncAccepted, nil, ErrRoomMaxMismatch
	}
	effectiveMax := d.roomMax[roomMaxKey]

	status := OnlineViewerSyncAccepted
	var rejected []string
	switch operation {
	case OnlineViewerJoin:
		viewerID := viewerIDs[0]
		if _, exists := owners[viewerID]; exists {
			status = OnlineViewerSyncDuplicate
		} else if uint32(len(owners)) >= effectiveMax {
			status = OnlineViewerSyncRoomFull
		} else {
			owners[viewerID] = distributedOwner{instanceID: instanceID, expiresAt: now.Add(ttl)}
		}
	case OnlineViewerLeave:
		viewerID := viewerIDs[0]
		if owner, exists := owners[viewerID]; exists && owner.instanceID == instanceID {
			delete(owners, viewerID)
		}
	case OnlineViewerHeartbeat:
		desired := make(map[string]struct{}, len(viewerIDs))
		for _, viewerID := range viewerIDs {
			desired[viewerID] = struct{}{}
		}
		for viewerID, owner := range owners {
			if owner.instanceID == instanceID {
				if _, exists := desired[viewerID]; !exists {
					delete(owners, viewerID)
				}
			}
		}
		for _, viewerID := range viewerIDs {
			owner, exists := owners[viewerID]
			switch {
			case exists && owner.instanceID != instanceID:
				rejected = append(rejected, viewerID)
			case !exists && uint32(len(owners)) >= effectiveMax:
				rejected = append(rejected, viewerID)
			default:
				owners[viewerID] = distributedOwner{instanceID: instanceID, expiresAt: now.Add(ttl)}
			}
		}
	case OnlineViewerClose:
		for viewerID, owner := range owners {
			if owner.instanceID == instanceID {
				delete(owners, viewerID)
			}
		}
	default:
		return 0, OnlineViewerSyncAccepted, nil, fmt.Errorf("unexpected operation %q", operation)
	}

	globalCount := uint32(len(owners))
	d.setTotalCount(totalCountKey, globalCount)
	d.setPeakCount(peakCountKey, globalCount, peakRetention)
	return globalCount, status, rejected, nil
}

// StoreMax 模拟 Redis Lua 的原子最大值语义，供跨实例最高峰值测试使用。
func (d *distributedMemoryDataSource) StoreMax(_ context.Context, key string, value uint32, duration time.Duration) error {
	d.memoryDataSource.mu.Lock()
	defer d.memoryDataSource.mu.Unlock()
	current, _ := strconv.ParseUint(d.memoryDataSource.values[key], 10, 32)
	if uint64(value) > current {
		d.memoryDataSource.values[key] = strconv.FormatUint(uint64(value), 10)
	}
	d.memoryDataSource.retention[key] = duration
	return nil
}

func (d *distributedMemoryDataSource) setTotalCount(key string, count uint32) {
	d.memoryDataSource.mu.Lock()
	d.memoryDataSource.values[key] = strconv.FormatUint(uint64(count), 10)
	d.memoryDataSource.mu.Unlock()
}

func (d *distributedMemoryDataSource) setPeakCount(key string, count uint32, retention time.Duration) {
	d.memoryDataSource.mu.Lock()
	current, _ := strconv.ParseUint(d.memoryDataSource.values[key], 10, 32)
	if uint64(count) > current {
		d.memoryDataSource.values[key] = strconv.FormatUint(uint64(count), 10)
	}
	d.memoryDataSource.retention[key] = retention
	d.memoryDataSource.mu.Unlock()
}

func newDistributedTestRoom(t *testing.T, dataSource DataSource, roomNumber string, maxViewer uint32, ttl time.Duration) *Room {
	t.Helper()
	room, err := NewRoomWithConfig(context.Background(), "分布式测试房间", roomNumber, maxViewer, "firm-distributed", RoomConfig{
		OnlinePeakInterval: time.Minute,
		OnlinePresenceTTL:  ttl,
	})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	room.dataSource = dataSource
	return room
}

func TestDistributedJoinCountsAcrossInstancesAndEnforcesCapacity(t *testing.T) {
	dataSource := newDistributedMemoryDataSource()
	room1 := newDistributedTestRoom(t, dataSource, "shared-room", 3, time.Minute)
	room2 := newDistributedTestRoom(t, dataSource, "shared-room", 3, time.Minute)
	defer room1.Close()
	defer room2.Close()

	viewer1 := NewViewer(room1.GetRoomCtx(), "room1-viewer1", "观众1", nil)
	viewer2 := NewViewer(room1.GetRoomCtx(), "room1-viewer2", "观众2", nil)
	viewer3 := NewViewer(room2.GetRoomCtx(), "room2-viewer1", "观众3", nil)
	viewer4 := NewViewer(room2.GetRoomCtx(), "room2-viewer2", "观众4", nil)

	for _, item := range []struct {
		room   *Room
		viewer *Viewer
	}{{room1, viewer1}, {room1, viewer2}, {room2, viewer3}} {
		if err := item.room.JoinRoom(item.viewer); err != nil {
			t.Fatalf("JoinRoom() error = %v", err)
		}
	}
	if err := room2.JoinRoom(viewer4); !errors.Is(err, ErrRoomIsFull) {
		t.Fatalf("超过全局容量时 JoinRoom() error = %v, want ErrRoomIsFull", err)
	}

	if got := room2.GetOnlineViewerCount(); got != 3 {
		t.Fatalf("第二实例看到的全局在线人数 = %d, want 3", got)
	}
	if got := room1.refreshDistributedOnlineViewerCount(); got != 3 {
		t.Fatalf("第一实例刷新后的全局在线人数 = %d, want 3", got)
	}
	if got := room1.GetLocalOnlineViewerCount(); got != 2 {
		t.Fatalf("第一实例本地在线人数 = %d, want 2", got)
	}

	room1.LeaveRoom(viewer1)
	if got := room1.GetOnlineViewerCount(); got != 2 {
		t.Fatalf("离房后的全局在线人数 = %d, want 2", got)
	}
	peak, err := room1.GetOnlineViewerPeak(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("GetOnlineViewerPeak() error = %v", err)
	}
	if peak.Count != 3 {
		t.Fatalf("分布式区间峰值 = %d, want 3", peak.Count)
	}
}

func TestDistributedConcurrentJoinNeverExceedsGlobalCapacity(t *testing.T) {
	const maxViewer = 10
	const attempts = 40

	dataSource := newDistributedMemoryDataSource()
	room1 := newDistributedTestRoom(t, dataSource, "concurrent-room", maxViewer, time.Minute)
	room2 := newDistributedTestRoom(t, dataSource, "concurrent-room", maxViewer, time.Minute)
	defer room1.Close()
	defer room2.Close()

	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			room := room1
			if index%2 == 1 {
				room = room2
			}
			viewer := NewViewer(room.GetRoomCtx(), fmt.Sprintf("distributed-viewer-%d", index), "观众", nil)
			err := room.JoinRoom(viewer)
			if err == nil {
				success.Add(1)
				return
			}
			if !errors.Is(err, ErrRoomIsFull) {
				t.Errorf("JoinRoom() error = %v, want ErrRoomIsFull", err)
			}
		}(i)
	}
	wg.Wait()

	if got := success.Load(); got != maxViewer {
		t.Fatalf("全局成功加入人数 = %d, want %d", got, maxViewer)
	}
	if got := room1.GetLocalOnlineViewerCount() + room2.GetLocalOnlineViewerCount(); got != maxViewer {
		t.Fatalf("两个实例本地人数之和 = %d, want %d", got, maxViewer)
	}
	if got := room1.refreshDistributedOnlineViewerCount(); got != maxViewer {
		t.Fatalf("刷新后的全局在线人数 = %d, want %d", got, maxViewer)
	}
}

func TestDistributedJoinDeduplicatesViewerAcrossInstances(t *testing.T) {
	const ttl = 10 * time.Second
	clock := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	dataSource := newDistributedMemoryDataSource()
	dataSource.now = func() time.Time { return clock }

	room1 := newDistributedTestRoom(t, dataSource, "deduplicate-room", 10, ttl)
	room2 := newDistributedTestRoom(t, dataSource, "deduplicate-room", 10, ttl)
	defer room1.Close()
	defer room2.Close()

	viewer1 := NewViewer(room1.GetRoomCtx(), "same-viewer", "观众", nil)
	if err := room1.JoinRoom(viewer1); err != nil {
		t.Fatalf("room1.JoinRoom() error = %v", err)
	}
	viewer2 := NewViewer(room2.GetRoomCtx(), "same-viewer", "观众", nil)
	if err := room2.JoinRoom(viewer2); !errors.Is(err, ErrViewerAlreadyJoined) {
		t.Fatalf("跨实例重复加入 error = %v, want ErrViewerAlreadyJoined", err)
	}
	if got := room2.GetOnlineViewerCount(); got != 1 {
		t.Fatalf("重复用户加入后的全局在线人数 = %d, want 1", got)
	}

	clock = clock.Add(ttl + time.Millisecond)
	if err := room2.JoinRoom(viewer2); err != nil {
		t.Fatalf("原实例租约过期后重新加入 error = %v", err)
	}
	if got := room2.GetOnlineViewerCount(); got != 1 {
		t.Fatalf("租约迁移后的全局在线人数 = %d, want 1", got)
	}

	// 旧实例晚到的 Close 不能删除新实例已获得的用户归属。
	room1.Close()
	if got := room2.refreshDistributedOnlineViewerCount(); got != 1 {
		t.Fatalf("旧实例关闭后的全局在线人数 = %d, want 1", got)
	}
}

func TestDistributedPresenceExpiresCrashedInstance(t *testing.T) {
	const ttl = 10 * time.Second
	clock := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	dataSource := newDistributedMemoryDataSource()
	dataSource.now = func() time.Time { return clock }

	room1 := newDistributedTestRoom(t, dataSource, "expiry-room", 10, ttl)
	room2 := newDistributedTestRoom(t, dataSource, "expiry-room", 10, ttl)
	defer room1.Close()
	defer room2.Close()

	for i := 0; i < 2; i++ {
		viewer := NewViewer(room1.GetRoomCtx(), fmt.Sprintf("expired-viewer-%d", i), "观众", nil)
		if err := room1.JoinRoom(viewer); err != nil {
			t.Fatalf("room1.JoinRoom() error = %v", err)
		}
	}

	clock = clock.Add(ttl + time.Millisecond)
	viewer := NewViewer(room2.GetRoomCtx(), "active-viewer", "观众", nil)
	if err := room2.JoinRoom(viewer); err != nil {
		t.Fatalf("room2.JoinRoom() error = %v", err)
	}
	if got := room2.GetOnlineViewerCount(); got != 1 {
		t.Fatalf("清理崩溃实例后的全局在线人数 = %d, want 1", got)
	}
}

func TestDistributedJoinRollsBackWhenPresenceSyncFails(t *testing.T) {
	dataSource := newDistributedMemoryDataSource()
	dataSource.syncErr = errors.New("redis unavailable")
	room := newDistributedTestRoom(t, dataSource, "failure-room", 10, time.Minute)
	defer room.Close()

	viewer := NewViewer(room.GetRoomCtx(), "viewer", "观众", nil)
	err := room.JoinRoom(viewer)
	if !errors.Is(err, ErrOnlinePresenceSync) {
		t.Fatalf("JoinRoom() error = %v, want ErrOnlinePresenceSync", err)
	}
	if got := room.GetLocalOnlineViewerCount(); got != 0 {
		t.Fatalf("同步失败后的本地在线人数 = %d, want 0", got)
	}
	if got := room.GetViewer("viewer"); got != nil {
		t.Fatalf("同步失败后观众仍被加入房间")
	}
}

func TestDistributedJoinRejectsMismatchedRoomCapacity(t *testing.T) {
	dataSource := newDistributedMemoryDataSource()
	room1 := newDistributedTestRoom(t, dataSource, "mismatched-capacity-room", 10, time.Minute)
	room2 := newDistributedTestRoom(t, dataSource, "mismatched-capacity-room", 20, time.Minute)
	defer room1.Close()
	defer room2.Close()

	viewer1 := NewViewer(room1.GetRoomCtx(), "viewer-1", "观众1", nil)
	if err := room1.JoinRoom(viewer1); err != nil {
		t.Fatalf("room1.JoinRoom() error = %v", err)
	}

	viewer2 := NewViewer(room2.GetRoomCtx(), "viewer-2", "观众2", nil)
	err := room2.JoinRoom(viewer2)
	if !errors.Is(err, ErrRoomMaxMismatch) {
		t.Fatalf("容量配置不一致时 JoinRoom() error = %v, want ErrRoomMaxMismatch", err)
	}
	if got := room2.GetLocalOnlineViewerCount(); got != 0 {
		t.Fatalf("容量配置不一致时第二实例本地人数 = %d, want 0", got)
	}
}

func TestDistributedHeartbeatRemovesViewerOwnedByAnotherInstance(t *testing.T) {
	dataSource := newDistributedMemoryDataSource()
	room1 := newDistributedTestRoom(t, dataSource, "heartbeat-conflict-room", 10, time.Minute)
	room2 := newDistributedTestRoom(t, dataSource, "heartbeat-conflict-room", 10, time.Minute)
	defer room1.Close()
	defer room2.Close()

	viewer := NewViewer(room1.GetRoomCtx(), "conflicted-viewer", "观众", nil)
	if err := room1.JoinRoom(viewer); err != nil {
		t.Fatalf("room1.JoinRoom() error = %v", err)
	}

	userOwnerKey, _, _, _ := room1.onlinePresenceKeys()
	dataSource.presenceMu.Lock()
	dataSource.owners[userOwnerKey][viewer.GetViewerID()] = distributedOwner{
		instanceID: room2.instanceID,
		expiresAt:  dataSource.now().Add(time.Minute),
	}
	dataSource.presenceMu.Unlock()

	room1.refreshDistributedOnlineViewerCount()
	if got := room1.GetLocalOnlineViewerCount(); got != 0 {
		t.Fatalf("冲突心跳后的本地在线人数 = %d, want 0", got)
	}
	if got := room1.GetViewer(viewer.GetViewerID()); got != nil {
		t.Fatalf("归属于其他实例的冲突观众仍保留在本地房间")
	}
}

func TestDistributedHeartbeatRebuildsLostPresenceState(t *testing.T) {
	dataSource := newDistributedMemoryDataSource()
	room := newDistributedTestRoom(t, dataSource, "heartbeat-rebuild-room", 10, time.Minute)
	defer room.Close()

	viewer := NewViewer(room.GetRoomCtx(), "rebuild-viewer", "观众", nil)
	if err := room.JoinRoom(viewer); err != nil {
		t.Fatalf("JoinRoom() error = %v", err)
	}

	userOwnerKey, _, _, _ := room.onlinePresenceKeys()
	dataSource.presenceMu.Lock()
	delete(dataSource.owners, userOwnerKey)
	dataSource.presenceMu.Unlock()

	if got := room.refreshDistributedOnlineViewerCount(); got != 1 {
		t.Fatalf("重建丢失状态后的全局在线人数 = %d, want 1", got)
	}
	if got := room.GetLocalOnlineViewerCount(); got != 1 {
		t.Fatalf("重建丢失状态后的本地在线人数 = %d, want 1", got)
	}
}

func TestDistributedLiveOnlineViewerPeakMaxKeepsGlobalMaximum(t *testing.T) {
	dataSource := newDistributedMemoryDataSource()
	room1 := newDistributedTestRoom(t, dataSource, "distributed-peak-max", 10, time.Minute)
	room2 := newDistributedTestRoom(t, dataSource, "distributed-peak-max", 10, time.Minute)
	defer room1.Close()
	defer room2.Close()

	viewer1 := NewViewer(room1.GetRoomCtx(), "peak-max-viewer-1", "观众1", nil)
	viewer2 := NewViewer(room2.GetRoomCtx(), "peak-max-viewer-2", "观众2", nil)
	viewer3 := NewViewer(room1.GetRoomCtx(), "peak-max-viewer-3", "观众3", nil)
	for _, item := range []struct {
		room   *Room
		viewer *Viewer
	}{
		{room1, viewer1},
		{room2, viewer2},
		{room1, viewer3},
	} {
		if err := item.room.JoinRoom(item.viewer); err != nil {
			t.Fatalf("JoinRoom() error = %v", err)
		}
	}

	room1.LeaveRoom(viewer1)
	room2.LeaveRoom(viewer2)

	got, err := room1.GetLiveOnlineViewerPeakMax(context.Background())
	if err != nil {
		t.Fatalf("GetLiveOnlineViewerPeakMax() error = %v", err)
	}
	if got != 3 {
		t.Fatalf("跨实例整场最高在线人数 = %d, want 3", got)
	}
}

package sdk

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryDataSource struct {
	mu        sync.Mutex
	values    map[string]string
	retention map[string]time.Duration
}

func newMemoryDataSource() *memoryDataSource {
	return &memoryDataSource{
		values:    make(map[string]string),
		retention: make(map[string]time.Duration),
	}
}

func (m *memoryDataSource) SendMessage(context.Context, string, *MessagePb) error { return nil }
func (m *memoryDataSource) GetMessage(context.Context, string) []*MessagePb       { return nil }
func (m *memoryDataSource) GetRedisBytesSent(string) int64                        { return 0 }
func (m *memoryDataSource) GetRedisBytesRecv(string) int64                        { return 0 }

func (m *memoryDataSource) Store(_ context.Context, key string, value any, duration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = fmt.Sprint(value)
	m.retention[key] = duration
	return nil
}

func (m *memoryDataSource) Get(_ context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[key]
	if !ok {
		return "", fmt.Errorf("key not found: %s", key)
	}
	return value, nil
}

func (m *memoryDataSource) AccumulatedBy(_ context.Context, key string, value int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, _ := strconv.ParseInt(m.values[key], 10, 64)
	m.values[key] = strconv.FormatInt(current+value, 10)
	return nil
}

func TestOnlineViewerPeakRollsByConfiguredInterval(t *testing.T) {
	const interval = 5 * time.Minute
	const retention = 24 * time.Hour

	room, err := NewRoomWithConfig(context.Background(), "测试房间", "room-peak", 100, "firm-1", RoomConfig{
		OnlinePeakInterval:  interval,
		OnlinePeakRetention: retention,
	})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	dataSource := newMemoryDataSource()
	room.dataSource = dataSource
	defer room.Close()

	windowStart := time.Date(2026, 7, 17, 10, 0, 0, 0, time.Local).Truncate(interval)
	room.onlinePeakMu.Lock()
	room.onlinePeakWindowStart = windowStart
	room.onlinePeakCount = 0
	room.onlinePeakMu.Unlock()

	room.observeOnlineViewerCount(windowStart.Add(time.Minute), 3)
	room.observeOnlineViewerCount(windowStart.Add(2*time.Minute), 9)
	room.observeOnlineViewerCount(windowStart.Add(4*time.Minute), 5)
	room.observeOnlineViewerCount(windowStart.Add(interval), 2)

	key := fmt.Sprintf(Live_Online_User_Peak_Count, "firm-1", "room-peak", windowStart.UnixMilli())
	stored, err := dataSource.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("峰值键未写入: %v", err)
	}
	if stored != "9" {
		t.Fatalf("上一统计区间峰值 = %s, want 9", stored)
	}
	if got := dataSource.retention[key]; got != retention {
		t.Fatalf("峰值保留时间 = %v, want %v", got, retention)
	}

	storedPeak, err := room.GetOnlineViewerPeak(context.Background(), windowStart.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("GetOnlineViewerPeak() error = %v", err)
	}
	if storedPeak.WindowStart != windowStart || storedPeak.Count != 9 {
		t.Fatalf("查询到的历史峰值 = %+v, want windowStart=%v count=9", storedPeak, windowStart)
	}

	current := room.GetCurrentOnlineViewerPeak()
	if current.WindowStart != windowStart.Add(interval) {
		t.Fatalf("当前统计区间开始时间 = %v, want %v", current.WindowStart, windowStart.Add(interval))
	}
	if current.Count != 2 {
		t.Fatalf("当前统计区间峰值 = %d, want 2", current.Count)
	}
}

func TestStoreOnlineViewerSummaryPersistsCurrentAndPeak(t *testing.T) {
	room, err := NewRoomWithConfig(context.Background(), "测试房间", "room-summary", 10, "firm-2", RoomConfig{
		OnlinePeakInterval: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	dataSource := newMemoryDataSource()
	room.dataSource = dataSource
	defer room.Close()

	viewer1 := NewViewer(room.GetRoomCtx(), "viewer-1", "观众1", nil)
	viewer2 := NewViewer(room.GetRoomCtx(), "viewer-2", "观众2", nil)
	if err := room.JoinRoom(viewer1); err != nil {
		t.Fatalf("JoinRoom(viewer1) error = %v", err)
	}
	if err := room.JoinRoom(viewer2); err != nil {
		t.Fatalf("JoinRoom(viewer2) error = %v", err)
	}

	room.storeOnlineViewerSummary()

	currentKey := fmt.Sprintf(Live_Online_User_Count, "firm-2", "room-summary")
	current, err := dataSource.Get(context.Background(), currentKey)
	if err != nil {
		t.Fatalf("实时在线人数未写入: %v", err)
	}
	if current != "2" {
		t.Fatalf("实时在线人数 = %s, want 2", current)
	}

	peak := room.GetCurrentOnlineViewerPeak()
	peakKey := fmt.Sprintf(Live_Online_User_Peak_Count, "firm-2", "room-summary", peak.WindowStart.UnixMilli())
	storedPeak, err := dataSource.Get(context.Background(), peakKey)
	if err != nil {
		t.Fatalf("当前区间峰值未写入: %v", err)
	}
	if storedPeak != "2" {
		t.Fatalf("当前区间峰值 = %s, want 2", storedPeak)
	}

	room.LeaveRoom(viewer1)
	room.LeaveRoom(viewer1)
	if got := room.GetOnlineViewerCount(); got != 1 {
		t.Fatalf("重复离房后的实时在线人数 = %d, want 1", got)
	}
}

func TestConcurrentJoinRespectsCapacity(t *testing.T) {
	const capacity = 10
	const attempts = 30

	room, err := NewRoom(context.Background(), "并发测试房间", "room-capacity", capacity, "firm-3")
	if err != nil {
		t.Fatalf("NewRoom() error = %v", err)
	}
	defer room.Close()

	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			viewer := NewViewer(room.GetRoomCtx(), fmt.Sprintf("viewer-%d", index), "观众", nil)
			if err := room.JoinRoom(viewer); err == nil {
				success.Add(1)
			} else if err != ErrRoomIsFull {
				t.Errorf("JoinRoom() error = %v, want ErrRoomIsFull", err)
			}
		}(i)
	}
	wg.Wait()

	if got := success.Load(); got != capacity {
		t.Fatalf("成功加入人数 = %d, want %d", got, capacity)
	}
	if got := room.GetOnlineViewerCount(); got != capacity {
		t.Fatalf("实时在线人数 = %d, want %d", got, capacity)
	}
	if got := room.GetTotalViewerCount(); got != capacity {
		t.Fatalf("累计观看人数 = %d, want %d", got, capacity)
	}
}

func TestDuplicateJoinIsIdempotent(t *testing.T) {
	room, err := NewRoom(context.Background(), "幂等测试房间", "room-idempotent", 10, "firm-4")
	if err != nil {
		t.Fatalf("NewRoom() error = %v", err)
	}
	defer room.Close()

	viewer := NewViewer(room.GetRoomCtx(), "viewer-1", "观众1", nil)
	if err := room.JoinRoom(viewer); err != nil {
		t.Fatalf("第一次 JoinRoom() error = %v", err)
	}
	if err := room.JoinRoom(viewer); err != nil {
		t.Fatalf("第二次 JoinRoom() error = %v", err)
	}

	if got := room.GetOnlineViewerCount(); got != 1 {
		t.Fatalf("重复加入后的实时在线人数 = %d, want 1", got)
	}
	if got := room.GetTotalViewerCount(); got != 1 {
		t.Fatalf("重复加入后的累计观看人数 = %d, want 1", got)
	}
}

func TestRoomConfigUsesDefaultPeakInterval(t *testing.T) {
	room, err := NewRoomWithConfig(context.Background(), "默认配置房间", "room-default", 10, "firm-5", RoomConfig{})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	defer room.Close()

	if got := room.GetOnlinePeakInterval(); got != DefaultOnlinePeakInterval {
		t.Fatalf("默认峰值统计区间 = %v, want %v", got, DefaultOnlinePeakInterval)
	}
}

func TestOnlineViewerPeakCollectorUsesConfiguredInterval(t *testing.T) {
	const interval = 50 * time.Millisecond

	room, err := NewRoomWithConfig(context.Background(), "定时滚动房间", "room-timer", 10, "firm-6", RoomConfig{
		OnlinePeakInterval: interval,
	})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	dataSource := newMemoryDataSource()
	room.Start(dataSource)
	defer room.Close()

	for i := 0; i < 3; i++ {
		viewer := NewViewer(room.GetRoomCtx(), fmt.Sprintf("timer-viewer-%d", i), "观众", nil)
		if err := room.JoinRoom(viewer); err != nil {
			t.Fatalf("JoinRoom() error = %v", err)
		}
	}

	windowStart := room.GetCurrentOnlineViewerPeak().WindowStart
	key := fmt.Sprintf(Live_Online_User_Peak_Count, "firm-6", "room-timer", windowStart.UnixMilli())
	deadline := time.Now().Add(time.Second)
	for {
		stored, getErr := dataSource.Get(context.Background(), key)
		if getErr == nil {
			if stored != "3" {
				t.Fatalf("自动落库的区间峰值 = %s, want 3", stored)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待自动落库超时，key = %s", key)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLiveOnlineViewerPeakMaxKeepsHighestValue(t *testing.T) {
	const interval = 5 * time.Minute
	const retention = 24 * time.Hour

	room, err := NewRoomWithConfig(context.Background(), "最高峰值房间", "room-peak-max", 100, "firm-peak-max", RoomConfig{
		OnlinePeakInterval:  interval,
		OnlinePeakRetention: retention,
	})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	dataSource := newMemoryDataSource()
	room.dataSource = dataSource
	defer room.Close()

	firstWindow := time.Date(2026, 8, 5, 10, 0, 0, 0, time.Local).Truncate(interval)
	room.persistOnlineViewerPeak(firstWindow, 7)
	room.persistOnlineViewerPeak(firstWindow.Add(interval), 12)
	room.persistOnlineViewerPeak(firstWindow.Add(2*interval), 9)

	key := LiveOnlineViewerPeakMaxKey("firm-peak-max", "room-peak-max")
	if got, want := key, "firm-peak-max:own_live:room-peak-max:online_user_peak_max_count"; got != want {
		t.Fatalf("最高峰值键 = %q, want %q", got, want)
	}
	stored, err := dataSource.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("最高峰值键未写入: %v", err)
	}
	if stored != "12" {
		t.Fatalf("最高峰值键 = %s, want 12", stored)
	}
	if got := dataSource.retention[key]; got != retention {
		t.Fatalf("最高峰值保留时间 = %v, want %v", got, retention)
	}

	got, err := room.GetLiveOnlineViewerPeakMax(context.Background())
	if err != nil {
		t.Fatalf("GetLiveOnlineViewerPeakMax() error = %v", err)
	}
	if got != 12 {
		t.Fatalf("GetLiveOnlineViewerPeakMax() = %d, want 12", got)
	}

	// 后续较小的区间峰值不能覆盖已经写入的整场最高值。
	room.persistOnlineViewerPeak(firstWindow.Add(3*interval), 6)
	got, err = room.GetLiveOnlineViewerPeakMax(context.Background())
	if err != nil {
		t.Fatalf("第二次 GetLiveOnlineViewerPeakMax() error = %v", err)
	}
	if got != 12 {
		t.Fatalf("较小峰值覆盖后的整场最高值 = %d, want 12", got)
	}
}

func TestResetLiveOnlineViewerPeakMax(t *testing.T) {
	room, err := NewRoomWithConfig(context.Background(), "重置峰值房间", "room-peak-reset", 100, "firm-peak-reset", RoomConfig{})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	dataSource := newMemoryDataSource()
	room.dataSource = dataSource
	defer room.Close()

	room.persistLiveOnlineViewerPeakMax(15)
	if err := room.ResetLiveOnlineViewerPeakMax(context.Background()); err != nil {
		t.Fatalf("ResetLiveOnlineViewerPeakMax() error = %v", err)
	}
	got, err := room.GetLiveOnlineViewerPeakMax(context.Background())
	if err != nil {
		t.Fatalf("GetLiveOnlineViewerPeakMax() after reset error = %v", err)
	}
	if got != 0 {
		t.Fatalf("重置后的最高峰值 = %d, want 0", got)
	}

	room.persistLiveOnlineViewerPeakMax(4)
	got, err = room.GetLiveOnlineViewerPeakMax(context.Background())
	if err != nil {
		t.Fatalf("GetLiveOnlineViewerPeakMax() after new peak error = %v", err)
	}
	if got != 4 {
		t.Fatalf("新直播峰值 = %d, want 4", got)
	}
}

func TestCloseFlushesLiveOnlineViewerPeakMax(t *testing.T) {
	room, err := NewRoomWithConfig(context.Background(), "关房峰值房间", "room-peak-close", 100, "firm-peak-close", RoomConfig{})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	dataSource := newMemoryDataSource()
	room.dataSource = dataSource

	room.onlinePeakMu.Lock()
	room.onlinePeakCount = 11
	room.onlinePeakMu.Unlock()
	room.Close()

	got, err := room.GetLiveOnlineViewerPeakMax(context.Background())
	if err != nil {
		t.Fatalf("GetLiveOnlineViewerPeakMax() after Close error = %v", err)
	}
	if got != 11 {
		t.Fatalf("关房补刷后的最高峰值 = %d, want 11", got)
	}
}

func TestSaveLiveOnlineViewerPeakStoresNumericMax(t *testing.T) {
	const interval = 5 * time.Minute
	room, err := NewRoomWithConfig(context.Background(), "汇总峰值房间", "room-peak-save", 100, "firm-peak-save", RoomConfig{
		OnlinePeakInterval: interval,
	})
	if err != nil {
		t.Fatalf("NewRoomWithConfig() error = %v", err)
	}
	dataSource := newMemoryDataSource()
	room.dataSource = dataSource
	defer room.Close()

	start := time.Date(2026, 8, 5, 10, 1, 0, 0, time.Local)
	firstWindow := start.Truncate(interval)
	secondWindow := firstWindow.Add(interval)
	if err := dataSource.Store(context.Background(), OnlineViewerPeakKey("firm-peak-save", "room-peak-save", firstWindow), 3, 0); err != nil {
		t.Fatal(err)
	}
	if err := dataSource.Store(context.Background(), OnlineViewerPeakKey("firm-peak-save", "room-peak-save", secondWindow), 9, 0); err != nil {
		t.Fatal(err)
	}

	if err := room.SaveLiveOnlineViewerPeak(context.Background(), start, secondWindow.Add(2*time.Minute)); err != nil {
		t.Fatalf("SaveLiveOnlineViewerPeak() error = %v", err)
	}
	got, err := room.GetLiveOnlineViewerPeakMax(context.Background())
	if err != nil {
		t.Fatalf("GetLiveOnlineViewerPeakMax() error = %v", err)
	}
	if got != 9 {
		t.Fatalf("汇总写入的最高峰值 = %d, want 9", got)
	}
}

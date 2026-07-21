package sdk

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRoomCloseRacesWithLeaveRoom(t *testing.T) {
	const iterations = 100

	for i := 0; i < iterations; i++ {
		room, err := NewRoom(context.Background(), "并发关房", "room-close-race", 10, "firm-close-race")
		if err != nil {
			t.Fatalf("iteration %d: NewRoom() error = %v", i, err)
		}
		room.dataSource = newMemoryDataSource()

		viewer := NewViewer(room.GetRoomCtx(), "viewer-close-race", "并发观众", nil)
		if err := room.JoinRoom(viewer); err != nil {
			t.Fatalf("iteration %d: JoinRoom() error = %v", i, err)
		}

		var callbackCount atomic.Int32
		callbackDone := make(chan struct{})
		viewer.SetOnLeaveCallback(func(v *Viewer) {
			callbackCount.Add(1)
			// 验证回调重入 LeaveRoom 时不会与 Room.Close 形成锁递归或 panic。
			room.LeaveRoom(v)
			close(callbackDone)
		})

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			room.Close()
		}()
		go func() {
			defer wg.Done()
			<-start
			room.LeaveRoom(viewer)
		}()
		close(start)
		wg.Wait()

		select {
		case <-callbackDone:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: 关闭回调未完成，可能发生死锁", i)
		}

		if room.IsOpen() {
			t.Fatalf("iteration %d: Room.Close() 后房间仍处于打开状态", i)
		}
		if got := room.GetLocalOnlineViewerCount(); got != 0 {
			t.Fatalf("iteration %d: 本实例在线人数 = %d, want 0", i, got)
		}
		if !viewer.closed.Load() {
			t.Fatalf("iteration %d: viewer 未进入关闭状态", i)
		}
		if got := callbackCount.Load(); got != 1 {
			t.Fatalf("iteration %d: 离开回调次数 = %d, want 1", i, got)
		}
	}
}

func TestViewerCloseIsConcurrentAndCallbackIsOnce(t *testing.T) {
	room, err := NewRoom(context.Background(), "重复关闭", "room-viewer-close", 10, "firm-viewer-close")
	if err != nil {
		t.Fatalf("NewRoom() error = %v", err)
	}
	defer room.Close()
	room.dataSource = newMemoryDataSource()

	viewer := NewViewer(room.GetRoomCtx(), "viewer-close-once", "重复关闭观众", nil)
	if err := room.JoinRoom(viewer); err != nil {
		t.Fatalf("JoinRoom() error = %v", err)
	}

	var callbackCount atomic.Int32
	callbackDone := make(chan struct{})
	viewer.SetOnLeaveCallback(func(*Viewer) {
		if callbackCount.Add(1) == 1 {
			close(callbackDone)
		}
	})

	const callers = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			viewer.Close()
		}()
	}
	close(start)
	wg.Wait()

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("并发 Close 后离开回调未执行")
	}

	if got := callbackCount.Load(); got != 1 {
		t.Fatalf("离开回调次数 = %d, want 1", got)
	}
	if !viewer.closed.Load() {
		t.Fatal("并发 Close 后 viewer 未进入关闭状态")
	}

	// 网络关闭触发的 Close 与房间主动离开同时发生时也必须幂等。
	var leaveWG sync.WaitGroup
	leaveWG.Add(2)
	go func() {
		defer leaveWG.Done()
		room.LeaveRoom(viewer)
	}()
	go func() {
		defer leaveWG.Done()
		room.Close()
	}()
	leaveWG.Wait()

	if got := room.GetLocalOnlineViewerCount(); got != 0 {
		t.Fatalf("并发离开与关房后本实例在线人数 = %d, want 0", got)
	}
}

func TestRoomCloseIsIdempotentUnderContention(t *testing.T) {
	room, err := NewRoom(context.Background(), "重复关房", "room-close-once", 10, "firm-close-once")
	if err != nil {
		t.Fatalf("NewRoom() error = %v", err)
	}
	room.dataSource = newMemoryDataSource()

	for i := 0; i < 8; i++ {
		viewer := NewViewer(room.GetRoomCtx(), "viewer-close-"+string(rune('a'+i)), "观众", nil)
		if err := room.JoinRoom(viewer); err != nil {
			t.Fatalf("JoinRoom() error = %v", err)
		}
	}

	const callers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			room.Close()
		}()
	}
	close(start)
	wg.Wait()

	if room.IsOpen() {
		t.Fatal("并发 Room.Close 后房间仍处于打开状态")
	}
	if got := room.GetLocalOnlineViewerCount(); got != 0 {
		t.Fatalf("并发 Room.Close 后本实例在线人数 = %d, want 0", got)
	}
}

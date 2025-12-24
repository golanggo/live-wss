package main

import (
	"context"
	"fmt"
	"time"

	"live-wss/live"
)

func main() {
	fmt.Println("=== 3人房间测试开始 ===")

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. 创建Redis数据源
	dataSource := live.NewSimpleRedisStream()

	// 2. 创建房间
	roomNumber := "test_3_users_room"
	roomName := "3人测试房间"
	maxViewer := uint32(10)

	// 为房间创建Stream处理器
	dataSource.CreateStreamHandler(live.RoomNumber(roomNumber), ctx)

	room, err := live.NewRoom(ctx, roomName, live.RoomNumber(roomNumber), maxViewer)
	if err != nil {
		fmt.Printf("创建房间失败: %v\n", err)
		return
	}

	// 3. 启动房间
	room.Start(dataSource)
	fmt.Printf("房间 %s 创建成功，最大容量: %d\n", roomNumber, maxViewer)

	// 4. 创建3个观众
	viewers := make([]*live.Viewer, 0, 3)
	for i := 0; i < 3; i++ {
		viewerID := live.ViewerID(fmt.Sprintf("viewer_%d", i+1))
		viewerName := fmt.Sprintf("观众%d", i+1)

		viewer := live.NewViewer(ctx, viewerID, viewerName, live.UserTypeViewer)
		viewer.Room = room

		// 加入房间
		if err := room.JoinRoom(viewer); err != nil {
			fmt.Printf("观众 %s 加入房间失败: %v\n", viewerName, err)
			continue
		}

		// 启动观众的消息读取器
		viewer.Start()
		viewers = append(viewers, viewer)

		fmt.Printf("观众 %s 加入房间成功\n", viewerName)
	}

	// 等待所有观众加入完成
	time.Sleep(2 * time.Second)

	// 5. 向房间发送消息
	totalMessages := 5
	for i := 0; i < totalMessages; i++ {
		msgContent := fmt.Sprintf("测试消息 %d: 这是发送给3人房间的消息", i+1)
		msg := &live.Message{
			Type:     live.MessageTypeText,
			Data:     []byte(msgContent),
			ViewerID: live.ViewerID("system"),
			Time:     time.Now(),
		}

		err := dataSource.SendMessage(ctx, live.RoomNumber(roomNumber), msg)
		if err != nil {
			fmt.Printf("发送消息失败: %v\n", err)
		} else {
			fmt.Printf("发送消息: %s\n", msgContent)
		}
		time.Sleep(1 * time.Second)
	}

	// 6. 等待消息接收
	time.Sleep(5 * time.Second)

	// 7. 统计结果
	fmt.Println("\n=== 测试结果统计 ===")
	fmt.Printf("房间观众数: %d\n", len(viewers))
	fmt.Printf("发送消息总数: %d\n", totalMessages)

	for i, viewer := range viewers {
		recvCount := viewer.ReceivedMessages()
		fmt.Printf("观众%d 接收消息数: %d\n", i+1, recvCount)
	}

	// 8. 清理资源
	for _, viewer := range viewers {
		viewer.Close()
	}

	room.Close()
	fmt.Println("\n=== 3人房间测试结束 ===")
}

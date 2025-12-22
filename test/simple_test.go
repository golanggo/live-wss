package test

import (
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func RunSimpleTest() {
	// 测试用户1：主播
	go func() {
		fmt.Println("=== 主播连接测试 ===")
		testWebSocket("主播", "anchor", "我是主播，欢迎大家来到我的直播间！")
		time.Sleep(5 * time.Second)
	}()

	// 测试用户2：观众1
	go func() {
		time.Sleep(1 * time.Second)
		fmt.Println("\n=== 观众1连接测试 ===")
		testWebSocket("观众1", "viewer", "大家好！")
	}()

	// 测试用户3：观众2
	go func() {
		time.Sleep(2 * time.Second)
		fmt.Println("\n=== 观众2连接测试 ===")
		testWebSocket("观众2", "viewer", "主播好，请问今天的主题是什么？")
	}()

	// 保持主函数运行
	time.Sleep(30 * time.Second)
	fmt.Println("\n=== 所有测试完成 ===")
}

func testWebSocket(username, userType, message string) {
	// 生成唯一用户ID
	userID := fmt.Sprintf("%s_%d", username, time.Now().UnixNano())

	// WebSocket连接URL - 使用正确的参数名称
	url := "ws://localhost:8080/ws?room=test_room&userid=" + userID + "&username=" + username

	// 连接到WebSocket服务器
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Printf("%s 连接失败: %v\n", username, err)
		return
	}
	defer conn.Close()
	defer fmt.Printf("%s 断开连接\n", username)

	// 发送消息
	err = conn.WriteMessage(websocket.TextMessage, []byte(message))
	if err != nil {
		log.Printf("%s 发送消息失败: %v\n", username, err)
		return
	}
	fmt.Printf("%s 发送消息: %s\n", username, message)

	// 设置接收超时
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// 接收消息（最多接收3条）
	for i := 0; i < 3; i++ {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			// 如果是超时，就退出循环
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("%s 接收消息错误: %v\n", username, err)
			}
			break
		}
		fmt.Printf("%s 收到消息: %s\n", username, msg)
	}

	// 等待一段时间后断开连接
	time.Sleep(5 * time.Second)
}

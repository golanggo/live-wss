package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrInvalidRoomNumber = errors.New("房间号不能为空")
	ErrInvalidRoomName   = errors.New("房间名字不能为空")
	ErrRoomFull          = errors.New("超过“房间管理器”最大房间数")
)

type RoomManager struct {
	Rooms      map[string]*Room
	roomRWLock sync.RWMutex
	MaxRoom    uint16
}

func NewRoomManager(maxRoom uint16) *RoomManager {
	var once sync.Once
	var roomManager *RoomManager
	once.Do(func() {
		roomManager = &RoomManager{
			//设置默认最大房间数
			Rooms:   make(map[string]*Room, maxRoom),
			MaxRoom: maxRoom,
		}
	})
	fmt.Println("房间管理器初始化完成。")
	return roomManager
}

func (m *RoomManager) CreateRoom(ctx context.Context, roomNumber string, roomName string, maxViewer uint32, firmUUID string) error {
	if roomNumber == "" {
		return ErrInvalidRoomNumber
	}
	if roomName == "" {
		return ErrInvalidRoomName
	}
	if maxViewer == 0 || maxViewer > 100000 {
		maxViewer = 100000
	}

	//超过最大房间数
	if len(m.Rooms) >= int(m.MaxRoom) {
		return ErrRoomFull
	}

	room, err := NewRoom(ctx, roomName, roomNumber, maxViewer, firmUUID)
	if err != nil {
		return err
	}
	m.SetRoom(room)
	return nil
}

func (m *RoomManager) SetRoom(room *Room) error {
	m.roomRWLock.Lock()
	defer m.roomRWLock.Unlock()
	m.Rooms[room.roomNumber] = room
	fmt.Printf("房间 %s 创建成功。最大容纳人数: %d\n", room.roomNumber, room.maxViewer)
	return nil
}

func (m *RoomManager) GetRoom(roomNumber string) *Room {
	m.roomRWLock.Lock()
	defer m.roomRWLock.Unlock()
	return m.Rooms[roomNumber]
}

func (m *RoomManager) RemoveRoom(roomNumber string) {
	m.roomRWLock.Lock()
	defer m.roomRWLock.Unlock()
	delete(m.Rooms, roomNumber)
	fmt.Printf("房间 %s 已删除。\n", roomNumber)
}

// 获取房间管理器详细信息，比如房间管理器中的房间数量，每个房间的人数，每个房间的直播状态等
func (m *RoomManager) Info() string {
	info := fmt.Sprintf("总房间数量: %d", len(m.Rooms))
	for _, room := range m.Rooms {
		//同时打印每个房间结构体的每个属性，还有消息队列中的消息数量
		info += room.Info()
	}
	return info
}

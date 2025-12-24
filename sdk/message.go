package sdk

import (
	"bytes"
	"compress/flate"
	"sync"
	"time"
)

type RoomNumber string

type MessageType string

const (
	MessageTypeJoin  MessageType = "join"
	MessageTypeLeave MessageType = "leave"
	MessageTypeText  MessageType = "text"
	MessageTypeLike  MessageType = "like"
	MessageTypeError MessageType = "single"
)

type MessagePriority int

const (
	MessagePriorityLow  MessagePriority = iota // 低优先级
	MessagePriorityHigh                        // 高优先级
)

// 压缩相关配置
const (
	// CompressionLevel 压缩级别（-2到9），-2表示最快压缩，9表示最高压缩率
	CompressionLevel = -2
)

// 全局消息池
type Message struct {
	ViewerID ViewerID        `json:"viewerID"`
	Type     MessageType     `json:"type"`
	Data     []byte          `json:"data"`
	Time     time.Time       `json:"timestamp"`
	Priority MessagePriority `json:"priority"` // 添加优先级字段
}

// MessagePool 消息对象池，减少内存分配和GC压力
// 4万人房间优化：重用消息对象，降低内存占用和GC频率
var MessagePool = sync.Pool{
	New: func() interface{} {
		return &Message{}
	},
}

// GetMessageFromPool 从对象池获取消息对象
func GetMessageFromPool(msgType MessageType, data string) *Message {
	msg := MessagePool.Get().(*Message)
	msg.Type = msgType
	msg.Data = []byte(data)
	return msg
}

// PutMessageToPool 将消息对象放回对象池
func PutMessageToPool(msg *Message) {
	if msg == nil {
		return
	}
	// 清空消息内容，避免内存泄漏
	msg.Type = ""
	msg.Data = nil
	MessagePool.Put(msg)
}

// CompressMessage 压缩消息数据
// 4万人房间优化：减少网络传输量和内存消耗
func CompressMessage(data []byte) ([]byte, error) {
	// 如果数据太小，不进行压缩（避免开销大于收益）
	if len(data) < 100 {
		return data, nil
	}

	var buf bytes.Buffer
	writer, err := flate.NewWriter(&buf, CompressionLevel)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(data)
	if err != nil {
		writer.Close()
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecompressMessage 解压缩消息数据
// 4万人房间优化：处理压缩后的消息
func DecompressMessage(data []byte) ([]byte, error) {
	// 如果数据太小，认为未压缩
	if len(data) < 100 {
		return data, nil
	}

	reader := flate.NewReader(bytes.NewReader(data))
	defer reader.Close()

	var buf bytes.Buffer
	_, err := buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

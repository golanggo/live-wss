package sdk

import (
	"bytes"
	"compress/flate"
	"errors"
	"sync"
)

const (
	Live_Entry             = "%v:own_live:%v:entry:%v"                // 进入直播
	Live_Banned            = "%v:own_live:%v:banned:%v"               // 封禁用户
	Live_Mute              = "%v:own_live:%v:mute:%v"                 // 禁言用户
	Live_Total_Count       = "%v:own_live:%v:total_count"             // 观看人次
	Live_Online_User_Count = "%v:own_live:%v:total_online_user_count" // 观看在线人数
	Live_Liked_Count       = "%v:own_live:%v:liked_count"             // 点赞数
	Live_Comment_Count     = "%v:own_live:%v:comment_count"           // 评论数
	Live_Msg_Broadcast     = "%v:own_live:%v:broadcast"               // 消息广播
	Live_Msg_Broadcast_HP  = "%v:own_live:%v:broadcast:hp"            // 高优先级消息广播
	Live_WatchDuration     = "%v:own_live:%v:watch_duration:%v"       // 进入直播观看时长
)

const (
	Code_Event_User_Click_Like = "event_user_click_like" // 用户点赞
)

// 连接监控和清理配置常量
const (
	// MaxPingInterval 最大Ping间隔（秒）
	MaxPingInterval = 30
	// MessageRingBufferSize 消息环形缓冲区大小，用于存储待发送到数据源的消息
	MessageRingBufferSize = 65536
)

var (
	ErrRoomNoLiving    = errors.New("直播已经结束")
	ErrRoomIsFull      = errors.New("房间已满")
	ErrNewRoomName     = errors.New("新房间名称不能为空")
	ErrNewRoomNumber   = errors.New("新房间号不能为空")
	ErrNewRoomFirmUUID = errors.New("事业部ID不能为空")
)

const (
	DefaultHostName     = "default-talk-host"
	DefaultConsumerName = "talk-consumer1"
	GroupName           = "%s:group"
)

// 压缩相关配置
const (
	// CompressionLevel 压缩级别（-2到9），-2表示最快压缩，9表示最高压缩率
	CompressionLevel = -2
)

// MessagePbPool 消息对象池，减少内存分配和GC压力
// 4万人房间优化：重用消息对象，降低内存占用和GC频率
var MessagePbPool = sync.Pool{
	New: func() interface{} {
		return &MessagePb{}
	},
}

// GetMessagePbFromPool 从对象池获取消息对象
func GetMessagePbFromPool(code string, data string) *MessagePb {
	msg := MessagePbPool.Get().(*MessagePb)
	msg.Code = code
	msg.Data = data
	return msg
}

// PutMessagePbToPool 将消息对象放回对象池
func PutMessagePbToPool(msg *MessagePb) {
	if msg == nil {
		return
	}
	// 清空消息内容，避免内存泄漏
	msg.Code = ""
	msg.Data = ""
	MessagePbPool.Put(msg)
}

// CompressMessagePb 压缩消息数据
// 4万人房间优化：减少网络传输量和内存消耗
func CompressMessagePb(data []byte) ([]byte, error) {
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

// DecompressMessagePb 解压缩消息数据
// 4万人房间优化：处理压缩后的消息
func DecompressMessagePb(data []byte) ([]byte, error) {
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

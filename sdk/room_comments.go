package sdk

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

const commentPublishAttempts = 3

func makeCommentCodeSet(codes []string) map[string]struct{} {
	result := make(map[string]struct{}, len(codes))
	for _, code := range codes {
		if code != "" {
			result[code] = struct{}{}
		}
	}
	return result
}

// IsCommentMessage 判断消息是否被配置为需要统计并发送到 MQ 的用户评论。
func (r *Room) IsCommentMessage(message *MessagePb) bool {
	if message == nil || message.SendClient == nil || message.SendClient.UserId == "" {
		return false
	}
	_, ok := r.commentCodes[message.Code]
	return ok
}

// LiveCommentUserCountKey 返回去重评论人数键。
func LiveCommentUserCountKey(firmUUID, roomNumber string) string {
	return fmt.Sprintf(Live_Comment_User_Count, firmUUID, roomNumber)
}

// LiveCommentUserSetKey 返回已发送评论用户的去重集合键。
func LiveCommentUserSetKey(firmUUID, roomNumber string) string {
	return fmt.Sprintf(Live_Comment_User_Set, firmUUID, roomNumber)
}

func (r *Room) commentStatisticsDataSource() (CommentStatisticsDataSource, bool) {
	dataSource, ok := r.dataSource.(CommentStatisticsDataSource)
	return dataSource, ok
}

func (r *Room) recordCommentUser(ctx context.Context, message *MessagePb) error {
	if !r.IsCommentMessage(message) {
		return nil
	}
	statisticsDataSource, ok := r.commentStatisticsDataSource()
	if !ok {
		return fmt.Errorf("当前数据源未实现评论人数统计能力")
	}
	_, err := statisticsDataSource.RecordCommentUser(
		ctx,
		LiveCommentUserSetKey(r.firmUUID, r.roomNumber),
		LiveCommentUserCountKey(r.firmUUID, r.roomNumber),
		message.SendClient.UserId,
		r.commentRetention,
	)
	return err
}

// GetLiveCommentUserCount 查询本场直播内发送过至少一条评论的去重用户数。
func (r *Room) GetLiveCommentUserCount(ctx context.Context) (uint32, error) {
	if r.dataSource == nil {
		return 0, fmt.Errorf("房间数据源未初始化")
	}
	if ctx == nil {
		ctx = r.roomCtx
	}
	value, err := r.dataSource.Get(ctx, LiveCommentUserCountKey(r.firmUUID, r.roomNumber))
	if err != nil {
		return 0, err
	}
	count, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("解析评论人数失败: %w", err)
	}
	return uint32(count), nil
}

// ResetLiveCommentUsers 清空新场次开始前的评论去重集合与人数。
// 同一 roomNumber 开启新场次前应仅由直播编排层调用一次。
func (r *Room) ResetLiveCommentUsers(ctx context.Context) error {
	statisticsDataSource, ok := r.commentStatisticsDataSource()
	if !ok {
		return fmt.Errorf("当前数据源未实现评论人数重置能力")
	}
	if ctx == nil {
		ctx = r.roomCtx
	}
	return statisticsDataSource.ResetCommentUsers(
		ctx,
		LiveCommentUserSetKey(r.firmUUID, r.roomNumber),
		LiveCommentUserCountKey(r.firmUUID, r.roomNumber),
	)
}

func (r *Room) newCommentEvent(message *MessagePb) CommentEvent {
	createdAt := time.Now().Local()
	if message.Timestamp == 0 {
		message.Timestamp = createdAt.Unix()
	}
	eventID := message.MessageId
	if eventID == "" {
		eventID = fmt.Sprintf("%v%v", message.SendClient.UserId, createdAt.UnixNano())
	}

	viewer := r.GetViewer(message.SendClient.UserId)
	avatar := ""
	userLevel := message.SendClient.UserTags
	var shopUUID int64
	shopName := ""
	var shopClerkUUID int64
	shopClerkName := ""
	if viewer != nil {
		userAvatar, ok := viewer.GetCustomData("UserAvatar")
		if ok {
			avatar = userAvatar.(string)
		}
		shopUUIDStr, ok := viewer.GetCustomData("ShopUUID")
		if ok {
			shopUUID = shopUUIDStr.(int64)
		}
		shopNameStr, ok := viewer.GetCustomData("ShopName")
		if ok {
			shopName = shopNameStr.(string)
		}
		shopClerkUUIDStr, ok := viewer.GetCustomData("ShopClerkUUID")
		if ok {
			shopClerkUUID = shopClerkUUIDStr.(int64)
		}
		shopClerkNameStr, ok := viewer.GetCustomData("ShopClerkName")
		if ok {
			shopClerkName = shopClerkNameStr.(string)
		}
	}
	return CommentEvent{
		MessageID:     eventID,
		FirmUUID:      r.firmUUID,
		RoomID:        r.roomNumber,
		ViewerID:      message.SendClient.UserId,
		NickName:      message.SendClient.NickName,
		Avatar:        avatar,
		UserLevel:     userLevel,
		Code:          message.Code,
		Data:          message.Data,
		CreatedAt:     createdAt,
		Timestamp:     message.Timestamp,
		ShopUUID:      shopUUID,
		ShopName:      shopName,
		ShopClerkUUID: shopClerkUUID,
		ShopClerkName: shopClerkName,
	}
}

// publishCommentToMQ 以固定 EventID 重试发布。消费者必须以 event_id 做幂等落库，
// 因为发布确认发生网络中断时，Broker 是否已收到消息可能无法由生产者确定。
func (r *Room) publishCommentToMQ(ctx context.Context, message *MessagePb) error {
	if !r.IsCommentMessage(message) {
		return nil
	}
	if r.commentPublisher == nil {
		return fmt.Errorf("评论 MQ 发布器未配置")
	}

	event := r.newCommentEvent(message)
	var lastErr error
	for attempt := 0; attempt < commentPublishAttempts; attempt++ {
		publishCtx, cancel := context.WithTimeout(ctx, DefaultCommentPublishTimeout)
		err := r.commentPublisher.PublishComment(publishCtx, event)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt+1 < commentPublishAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(1<<attempt) * 100 * time.Millisecond):
			}
		}
	}
	return fmt.Errorf("评论事件发布到 MQ 失败，已重试 %d 次: %w", commentPublishAttempts, lastErr)
}

// flushPendingMessagesToDataSource 在关播前使用房间上下文同步处理最后一批消息。
func (r *Room) flushPendingMessagesToDataSource() {
	streamKey := fmt.Sprintf(Live_Msg_Broadcast, r.firmUUID, r.roomNumber)
	r.flushPendingMessagesToDataSourceForStream(streamKey)
}

// 将消息发送到数据源的 Stream，并处理评论人数统计与 MQ 发布。
func (r *Room) flushPendingMessagesToDataSourceForStream(streamKey string) {
	messages := r.readFromMessageRingBuffer()
	for _, message := range messages {
		if message == nil || r.dataSource == nil {
			continue
		}

		// 原广播 Stream 始终照常写入；MQ 异常不会删除或替代其中的消息内容。
		if err := r.dataSource.SendMessage(r.roomCtx, streamKey, message); err != nil {
			log.Printf("发送消息到广播 Redis Stream 失败: %v, 房间: %s", err, r.roomNumber)
		}
		if !r.IsCommentMessage(message) {
			continue
		}
		if err := r.recordCommentUser(r.roomCtx, message); err != nil {
			log.Printf("存储评论人数失败: %v, 房间: %s, 用户: %s", err, r.roomNumber, message.SendClient.UserId)
		}
		if err := r.publishCommentToMQ(r.roomCtx, message); err != nil {
			log.Printf("发送评论到 RabbitMQ 失败: %v, 房间: %s, 用户: %s", err, r.roomNumber, message.SendClient.UserId)
		}
	}
}

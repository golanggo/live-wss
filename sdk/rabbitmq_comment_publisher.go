package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultCommentMQExchange   = "live_comment"
	DefaultCommentMQQueue      = "live_comment_persist"
	DefaultCommentMQRoutingKey = "comment"
)

// RabbitMQCommentPublisherConfig 定义评论事件直发 RabbitMQ 的连接与拓扑参数。
type RabbitMQCommentPublisherConfig struct {
	URL        string
	Exchange   string
	Queue      string
	RoutingKey string
}

func (c RabbitMQCommentPublisherConfig) withDefaults() (RabbitMQCommentPublisherConfig, error) {
	if c.URL == "" {
		return c, fmt.Errorf("RabbitMQ URL 不能为空")
	}
	parsedURL, err := url.Parse(c.URL)
	if err != nil {
		return c, fmt.Errorf("RabbitMQ URL 格式无效: %w", err)
	}
	if parsedURL.Hostname() == "" {
		return c, fmt.Errorf("RabbitMQ URL 必须包含主机地址")
	}
	uri, err := amqp.ParseURI(c.URL)
	if err != nil {
		return c, fmt.Errorf("RabbitMQ URL 格式无效: %w", err)
	}
	if uri.Scheme != "amqp" && uri.Scheme != "amqps" {
		return c, fmt.Errorf("RabbitMQ URL 协议必须为 amqp:// 或 amqps://")
	}
	if c.Exchange == "" {
		c.Exchange = DefaultCommentMQExchange
	}
	if c.Queue == "" {
		c.Queue = DefaultCommentMQQueue
	}
	if c.RoutingKey == "" {
		c.RoutingKey = DefaultCommentMQRoutingKey
	}
	return c, nil
}

// RabbitMQCommentPublisher 将评论事件发布到持久化 RabbitMQ 队列。
// 发布操作串行化，以确保每一次 Publisher Confirm 与对应事件准确配对。
type RabbitMQCommentPublisher struct {
	config RabbitMQCommentPublisherConfig

	mu            sync.Mutex
	connection    *amqp.Connection
	channel       *amqp.Channel
	confirmations <-chan amqp.Confirmation
	returns       <-chan amqp.Return
	closed        bool
}

// NewRabbitMQCommentPublisher 创建发布器并声明持久化交换机、队列和绑定。
func NewRabbitMQCommentPublisher(config RabbitMQCommentPublisherConfig) (*RabbitMQCommentPublisher, error) {
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	publisher := &RabbitMQCommentPublisher{config: config}
	publisher.mu.Lock()
	err = publisher.connectLocked()
	publisher.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return publisher, nil
}

// PublishComment 以持久化消息发布评论；只有 Broker 确认后才返回 nil。
func (p *RabbitMQCommentPublisher) PublishComment(ctx context.Context, event CommentEvent) error {
	if event.MessageID == "" {
		return fmt.Errorf("评论事件 ID 不能为空")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("序列化评论事件失败: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("RabbitMQ 评论发布器已关闭")
	}
	if err := p.connectLocked(); err != nil {
		return err
	}

	if err := p.channel.PublishWithContext(ctx, p.config.Exchange, p.config.RoutingKey, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    event.MessageID,
		Timestamp:    event.CreatedAt,
		Type:         "live.comment.created",
		Body:         body,
	}); err != nil {
		p.invalidateLocked()
		return fmt.Errorf("发布评论事件失败: %w", err)
	}

	select {
	case returned, ok := <-p.returns:
		if !ok {
			p.invalidateLocked()
			return fmt.Errorf("RabbitMQ 返回通知通道已关闭")
		}
		return fmt.Errorf("评论事件未路由到队列: reply_code=%d reply_text=%s", returned.ReplyCode, returned.ReplyText)
	case confirmation, ok := <-p.confirmations:
		if !ok {
			p.invalidateLocked()
			return fmt.Errorf("RabbitMQ 发布确认通道已关闭")
		}
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ 未确认评论事件，delivery_tag=%d", confirmation.DeliveryTag)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("等待 RabbitMQ 发布确认超时: %w", ctx.Err())
	}
}

// Close 关闭 RabbitMQ 连接；通常由 SDK 的拥有者在所有房间停止后调用。
func (p *RabbitMQCommentPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.closeLocked()
}

func (p *RabbitMQCommentPublisher) connectLocked() error {
	if p.closed {
		return fmt.Errorf("RabbitMQ 评论发布器已关闭")
	}
	if p.connection != nil && !p.connection.IsClosed() && p.channel != nil && !p.channel.IsClosed() {
		return nil
	}
	if err := p.closeLocked(); err != nil {
		return err
	}

	connection, err := amqp.Dial(p.config.URL)
	if err != nil {
		return formatRabbitMQConnectionError(p.config.URL, err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return fmt.Errorf("创建 RabbitMQ 通道失败: %w", err)
	}
	if err := channel.ExchangeDeclare(p.config.Exchange, "direct", true, false, false, false, nil); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return fmt.Errorf("声明评论交换机失败: %w", err)
	}
	queue, err := channel.QueueDeclare(p.config.Queue, true, false, false, false, nil)
	if err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return fmt.Errorf("声明评论队列失败: %w", err)
	}
	if err := channel.QueueBind(queue.Name, p.config.RoutingKey, p.config.Exchange, false, nil); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return fmt.Errorf("绑定评论队列失败: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return fmt.Errorf("启用 RabbitMQ Publisher Confirm 失败: %w", err)
	}

	p.connection = connection
	p.channel = channel
	p.confirmations = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	p.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
	return nil
}

// formatRabbitMQConnectionError 将认证信息从诊断文本中剥离，并对握手阶段 EOF 给出可操作提示。
func formatRabbitMQConnectionError(rawURL string, err error) error {
	uri, parseErr := amqp.ParseURI(rawURL)
	if parseErr != nil {
		return fmt.Errorf("连接 RabbitMQ 失败: %w", err)
	}
	endpoint := fmt.Sprintf("%s://%s", uri.Scheme, net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port)))

	var amqpErr *amqp.Error
	isHandshakeEOF := errors.Is(err, io.EOF) || strings.Contains(strings.ToLower(err.Error()), "eof")
	if errors.As(err, &amqpErr) && amqpErr.Code == amqp.FrameError && strings.EqualFold(amqpErr.Reason, "EOF") {
		isHandshakeEOF = true
	}
	if isHandshakeEOF {
		return fmt.Errorf("连接 RabbitMQ 失败: %w；目标 %s 在 TCP 建连后未完成 AMQP 0-9-1 握手。请确认该地址是 RabbitMQ AMQP 监听器而非管理端口/HTTP 或四层代理，并检查监听器协议是否与 URL 一致（非 TLS 使用 amqp://，TLS 使用 amqps://）。若 RabbitMQ 监听器启用了 PROXY protocol，负载均衡器必须发送 PROXY 头，客户端不能直接连接该监听器", err, endpoint)
	}
	return fmt.Errorf("连接 RabbitMQ 失败: %w（目标 %s）", err, endpoint)
}

func (p *RabbitMQCommentPublisher) invalidateLocked() {
	_ = p.closeLocked()
}

func (p *RabbitMQCommentPublisher) closeLocked() error {
	var errs []error
	if p.channel != nil {
		if err := p.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, err)
		}
	}
	if p.connection != nil {
		if err := p.connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, err)
		}
	}
	p.connection = nil
	p.channel = nil
	p.confirmations = nil
	p.returns = nil
	return errors.Join(errs...)
}

var _ CommentPublisher = (*RabbitMQCommentPublisher)(nil)

// DefaultCommentPublishTimeout 限制单条评论等待 Publisher Confirm 的最长时间。
const DefaultCommentPublishTimeout = 5 * time.Second

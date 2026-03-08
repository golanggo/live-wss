package sdk

import "regexp"

const (
	MessageFilterAction_Block      = iota // 消息不允许发送
	MessageFilterAction_Allow             // 消息允许发送
	MessageFilterAction_Modify            // 修改消息
	MessageFilterAction_Limit             // 匹配消息限制消息数量
	MessageFilterAction_Rate_Limit        // 限流消息数量
)

type MessageFilterRule struct {
	ID          string         // Unique identifier for the rule
	Pattern     *regexp.Regexp // Compiled regex pattern
	Action      int            // What to do when pattern matches
	Replacement string         // For modify action, what to replace with
	Priority    int            // Rule priority (lower runs first)
	Limit       int64
}

type MessageFilter interface {
	ShouldAllowMessage(msg *MessagePb, limit int64) (allow bool, modifiedMsg *MessagePb, err error)
	AddRule(rule *MessageFilterRule) error
	RemoveRule(id string) error
	GetRules() []*MessageFilterRule
}

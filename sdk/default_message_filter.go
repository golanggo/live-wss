package sdk

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"
)

// DefaultMessageFilter implements basic regex-based filtering
type DefaultMessageFilter struct {
	rules []*MessageFilterRule
	mu    sync.RWMutex
}

func NewDefaultMessageFilter() *DefaultMessageFilter {
	return &DefaultMessageFilter{
		rules: make([]*MessageFilterRule, 0),
	}
}

func (f *DefaultMessageFilter) AddRule(rule *MessageFilterRule) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Check if rule with same ID already exists
	for i, existingRule := range f.rules {
		if existingRule.ID == rule.ID {
			f.rules[i] = rule
			return nil
		}
	}

	f.rules = append(f.rules, rule)

	// Sort by priority
	sort.Slice(f.rules, func(i, j int) bool {
		return f.rules[i].Priority < f.rules[j].Priority
	})

	return nil
}

func (f *DefaultMessageFilter) RemoveRule(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i, rule := range f.rules {
		if rule.ID == id {
			f.rules = append(f.rules[:i], f.rules[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("rule with ID %s not found", id)
}

func (f *DefaultMessageFilter) GetRules() []*MessageFilterRule {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Return a copy to prevent external modification
	rulesCopy := make([]*MessageFilterRule, len(f.rules))
	copy(rulesCopy, f.rules)
	return rulesCopy
}

func (f *DefaultMessageFilter) ShouldAllowMessage(msg *MessagePb, limit int64) (bool, *MessagePb, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	modifiedMsg := msg

	for _, rule := range f.rules {
		// Check if the message content matches the pattern
		if rule.Pattern.MatchString(msg.Data) {
			switch rule.Action {
			case MessageFilterAction_Block:
				return false, nil, nil
			case MessageFilterAction_Allow:
				return true, modifiedMsg, nil
			case MessageFilterAction_Limit:
				ok := rand.Int63n(limit) == 0
				fmt.Println("MessageFilterAction_Limit,ok:", ok)
				return ok, modifiedMsg, nil
			case MessageFilterAction_Modify:
				// Create a copy of the message and modify it
				newMsg := proto.Clone(msg).(*MessagePb)
				newMsg.Data = rule.Pattern.ReplaceAllString(msg.Data, rule.Replacement)
				modifiedMsg = newMsg
			}
		}
	}

	// If no rules matched or only allow/modify rules applied, allow the message
	return true, modifiedMsg, nil
}

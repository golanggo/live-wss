package sdk

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"

	"github.com/kirklin/go-swd"
	"google.golang.org/protobuf/proto"
)

// 全局敏感词检测器单例
var (
	sensitiveDetector     *swd.SWD
	sensitiveDetectorOnce sync.Once
	sensitiveDetectorErr  error
)

// initSensitiveDetector 初始化敏感词检测器（只执行一次）
func initSensitiveDetector() {
	detector, err := swd.New()
	if err != nil {
		sensitiveDetectorErr = err
		return
	}

	// 添加自定义敏感词
	customWords := map[string]swd.Category{
		"涉黄":    swd.Pornography,
		"涉政":    swd.Political,
		"赌博词汇":  swd.Gambling,
		"毒品词汇":  swd.Drugs,
		"脏话词汇":  swd.Profanity,
		"歧视词汇":  swd.Discrimination,
		"诈骗词汇":  swd.Scam,
		"自定义词汇": swd.Custom,
	}

	if err := detector.AddWords(customWords); err != nil {
		sensitiveDetectorErr = err
		return
	}

	sensitiveDetector = detector
}

// getSensitiveDetector 获取敏感词检测器（线程安全）
func getSensitiveDetector() (*swd.SWD, error) {
	sensitiveDetectorOnce.Do(initSensitiveDetector)
	return sensitiveDetector, sensitiveDetectorErr
}

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
		switch rule.Action {
		case MessageFilterAction_Block:
			if rule.Pattern.MatchString(msg.Data) {
				return false, nil, nil
			}
		case MessageFilterAction_Allow:
			if rule.Pattern.MatchString(msg.Data) {
				return true, modifiedMsg, nil
			}
		case MessageFilterAction_Rate_Limit:
			ok := rand.Int63n(100) < limit
			return ok, modifiedMsg, nil
		case MessageFilterAction_Limit:
			if rule.Pattern.MatchString(msg.Data) {
				ok := rand.Int63n(limit) == 0
				return ok, modifiedMsg, nil
			}
		case MessageFilterAction_Modify:
			maskedData, isTriggered := SensitiveMask(msg.Data)
			if isTriggered {
				newMsg := proto.Clone(msg).(*MessagePb)
				newMsg.Data = maskedData
				modifiedMsg = newMsg
				return true, modifiedMsg, nil
			}
		}
	}

	return true, modifiedMsg, nil
}

func SensitiveMask(text string) (string, bool) {
	isTrigerSwd := false
	detector, err := getSensitiveDetector()
	if err != nil || detector == nil {
		return text, isTrigerSwd
	}
	words := detector.MatchAll(text)
	for _, word := range words {
		chars := make([]rune, len([]rune(word.Word)))
		for i := range chars {
			chars[i] = '*'
		}
		text = strings.Replace(text, word.Word, string(chars), -1)
		isTrigerSwd = true
	}

	return text, isTrigerSwd
}

// 支持动态添加敏感词
func AddSensitiveWords(words map[string]swd.Category) error {
	detector, err := getSensitiveDetector()
	if err != nil || detector == nil {
		return err
	}
	return detector.AddWords(words)
}

// 获取检测器状态（用于健康检查）
func GetSensitiveDetectorStatus() (bool, error) {
	detector, err := getSensitiveDetector()
	if err != nil {
		return false, err
	}
	return detector != nil, nil
}

package sdk

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/dynamomq/internal/clock"
)

func NewDefaultMessage[T any](id string, data *T, now time.Time) *Message[T] {
	return &Message[T]{
		ID:                   id,
		Data:                 data,
		SystemInfo:           NewDefaultSystemInfo(id, now),
		Queued:               0,
		LastUpdatedTimestamp: clock.FormatRFC3339(now),
		DLQ:                  0,
	}
}

type Message[T any] struct {
	ID         string      `json:"id" dynamodbav:"id"`
	Data       *T          `json:"data" dynamodbav:"data"`
	SystemInfo *SystemInfo `json:"system_info" dynamodbav:"system_info"`

	Queued               int    `json:"queued" dynamodbav:"queued,omitempty"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp,omitempty"`
	DLQ                  int    `json:"DLQ" dynamodbav:"DLQ,omitempty"`
}

func (m *Message[T]) IsQueueSelected(now time.Time, visibilityTimeout time.Duration) bool {
	if !m.SystemInfo.SelectedFromQueue {
		return false
	}
	timeDifference := now.UnixMilli() - m.SystemInfo.PeekUTCTimestamp
	return timeDifference <= visibilityTimeout.Milliseconds()
}

func (m *Message[T]) IsRemoved() bool {
	return m.Queued == 0 &&
		m.DLQ == 0 &&
		m.SystemInfo.InQueue == 0 &&
		m.SystemInfo.SelectedFromQueue == false &&
		m.SystemInfo.RemoveFromQueueTimestamp != ""
}

func (m *Message[T]) IsEnqueued() bool {
	return m.Queued == 1 &&
		m.DLQ == 0 &&
		m.SystemInfo.InQueue == 1 &&
		m.SystemInfo.SelectedFromQueue == false &&
		m.SystemInfo.Status == StatusReady &&
		m.SystemInfo.AddToQueueTimestamp != "" &&
		m.SystemInfo.RemoveFromQueueTimestamp == ""
}

func (m *Message[T]) IsDLQ() bool {
	return m.Queued == 0 &&
		m.DLQ == 1 &&
		m.SystemInfo.InQueue == 0 &&
		m.SystemInfo.SelectedFromQueue == false &&
		m.SystemInfo.AddToDLQTimestamp != "" &&
		m.SystemInfo.Status == StatusInDLQ
}

func (m *Message[T]) MarkAsReady(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.LastUpdatedTimestamp = ts
	m.SystemInfo.LastUpdatedTimestamp = ts
	m.SystemInfo.Status = StatusReady
}

func (m *Message[T]) MarkAsEnqueued(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.Queued = 1
	m.DLQ = 0
	m.LastUpdatedTimestamp = ts
	m.SystemInfo.InQueue = 1
	m.SystemInfo.SelectedFromQueue = false
	m.SystemInfo.LastUpdatedTimestamp = ts
	m.SystemInfo.AddToQueueTimestamp = ts
	m.SystemInfo.Status = StatusReady
}

func (m *Message[T]) MarkAsPeeked(now time.Time) {
	ts := clock.FormatRFC3339(now)
	unixTime := now.UnixMilli()
	m.Queued = 1
	m.LastUpdatedTimestamp = ts
	m.SystemInfo.InQueue = 1
	m.SystemInfo.SelectedFromQueue = true
	m.SystemInfo.LastUpdatedTimestamp = ts
	m.SystemInfo.PeekFromQueueTimestamp = ts
	m.SystemInfo.PeekUTCTimestamp = unixTime
	m.SystemInfo.Status = StatusProcessing
}

func (m *Message[T]) MarkAsRemoved(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.Queued = 0
	m.DLQ = 0
	m.LastUpdatedTimestamp = ts
	m.SystemInfo.InQueue = 0
	m.SystemInfo.SelectedFromQueue = false
	m.SystemInfo.LastUpdatedTimestamp = ts
	m.SystemInfo.RemoveFromQueueTimestamp = ts
}

func (m *Message[T]) MarkAsDLQ(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.Queued = 0
	m.DLQ = 1
	m.LastUpdatedTimestamp = ts
	m.SystemInfo.InQueue = 0
	m.SystemInfo.SelectedFromQueue = false
	m.SystemInfo.LastUpdatedTimestamp = ts
	m.SystemInfo.AddToDLQTimestamp = ts
	m.SystemInfo.Status = StatusInDLQ
}

func (m *Message[T]) ResetSystemInfo(now time.Time) {
	m.SystemInfo = NewDefaultSystemInfo(m.ID, now)
}

func (m *Message[T]) Touch(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.LastUpdatedTimestamp = ts
	m.SystemInfo.LastUpdatedTimestamp = ts
}

func (m *Message[T]) Update(message *Message[T], now time.Time) {
	formatted := clock.FormatRFC3339(now)
	nextVersion := m.SystemInfo.Version + 1

	m.Data = message.Data
	m.SystemInfo = message.SystemInfo
	m.SystemInfo.Version = nextVersion
	m.SystemInfo.LastUpdatedTimestamp = formatted

	m.Queued = message.Queued
	m.LastUpdatedTimestamp = formatted
	m.DLQ = message.DLQ
}

func (m *Message[T]) ChangeStatus(status Status, now time.Time) {
	formatted := clock.FormatRFC3339(now)

	m.SystemInfo.Status = status
	m.SystemInfo.LastUpdatedTimestamp = formatted
	m.LastUpdatedTimestamp = formatted
}

func (m *Message[T]) MarshalMap() (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(m)
	if err != nil {
		return nil, &MarshalingAttributeError{Cause: err}
	}
	return item, nil
}

func (m *Message[T]) MarshalMapUnsafe() map[string]types.AttributeValue {
	item, _ := attributevalue.MarshalMap(m)
	return item
}

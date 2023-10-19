package sdk

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/dynamomq/internal/clock"
)

type SystemInfo struct {
	ID                         string `json:"id" dynamodbav:"id"`
	Status                     Status `json:"status" dynamodbav:"status"`
	ReceiveCount               int    `json:"receive_count" dynamodbav:"receive_count"`
	Queued                     int    `json:"queued" dynamodbav:"queued"`
	DLQ                        int    `json:"dlq" dynamodbav:"dlq"`
	Version                    int    `json:"version" dynamodbav:"version"`
	CreationTimestamp          string `json:"creation_timestamp" dynamodbav:"creation_timestamp"`
	LastUpdatedTimestamp       string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	AddToQueueTimestamp        string `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	PeekFromQueueTimestamp     string `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
	CompleteFromQueueTimestamp string `json:"queue_complete_timestamp" dynamodbav:"queue_complete_timestamp"`
	AddToDLQTimestamp          string `json:"dlq_add_timestamp" dynamodbav:"dlq_add_timestamp"`
}

func newDefaultSystemInfo(id string, now time.Time) *SystemInfo {
	ts := clock.FormatRFC3339(now)
	return &SystemInfo{
		ID:                         id,
		Status:                     StatusPending,
		ReceiveCount:               0,
		Queued:                     0,
		DLQ:                        0,
		Version:                    1,
		CreationTimestamp:          ts,
		LastUpdatedTimestamp:       ts,
		AddToQueueTimestamp:        "",
		PeekFromQueueTimestamp:     "",
		CompleteFromQueueTimestamp: "",
		AddToDLQTimestamp:          "",
	}
}

func NewDefaultMessage[T any](id string, data *T, now time.Time) *Message[T] {
	system := newDefaultSystemInfo(id, now)
	return &Message[T]{
		ID:                         id,
		Data:                       data,
		Status:                     system.Status,
		Queued:                     system.Queued,
		DLQ:                        system.DLQ,
		ReceiveCount:               system.ReceiveCount,
		Version:                    system.Version,
		CreationTimestamp:          system.CreationTimestamp,
		LastUpdatedTimestamp:       system.LastUpdatedTimestamp,
		AddToQueueTimestamp:        system.AddToQueueTimestamp,
		PeekFromQueueTimestamp:     system.PeekFromQueueTimestamp,
		CompleteFromQueueTimestamp: system.CompleteFromQueueTimestamp,
		AddToDLQTimestamp:          system.AddToDLQTimestamp,
	}
}

type Message[T any] struct {
	ID                         string `json:"id" dynamodbav:"id"`
	Data                       *T     `json:"data" dynamodbav:"data"`
	Status                     Status `json:"status" dynamodbav:"status"`
	ReceiveCount               int    `json:"receive_count" dynamodbav:"receive_count"`
	Queued                     int    `json:"queued" dynamodbav:"queued,omitempty"`
	DLQ                        int    `json:"DLQ" dynamodbav:"DLQ,omitempty"`
	Version                    int    `json:"version" dynamodbav:"version"`
	CreationTimestamp          string `json:"creation_timestamp" dynamodbav:"creation_timestamp"`
	LastUpdatedTimestamp       string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	AddToQueueTimestamp        string `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	PeekFromQueueTimestamp     string `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
	CompleteFromQueueTimestamp string `json:"queue_complete_timestamp" dynamodbav:"queue_complete_timestamp"`
	AddToDLQTimestamp          string `json:"dlq_add_timestamp" dynamodbav:"dlq_add_timestamp"`
}

func (m *Message[T]) GetSystemInfo() *SystemInfo {
	return &SystemInfo{
		ID:                         m.ID,
		Status:                     m.Status,
		ReceiveCount:               m.ReceiveCount,
		Queued:                     m.Queued,
		DLQ:                        m.DLQ,
		Version:                    m.Version,
		CreationTimestamp:          m.CreationTimestamp,
		LastUpdatedTimestamp:       m.LastUpdatedTimestamp,
		AddToQueueTimestamp:        m.AddToQueueTimestamp,
		PeekFromQueueTimestamp:     m.PeekFromQueueTimestamp,
		CompleteFromQueueTimestamp: m.CompleteFromQueueTimestamp,
		AddToDLQTimestamp:          m.AddToDLQTimestamp,
	}
}

func (m *Message[T]) IsQueueSelected(now time.Time, visibilityTimeout time.Duration) bool {
	if m.Status != StatusProcessing {
		return false
	}
	peekUTCTimestamp := clock.RFC3339ToUnixMilli(m.PeekFromQueueTimestamp)
	timeDifference := now.UnixMilli() - peekUTCTimestamp
	return timeDifference <= visibilityTimeout.Milliseconds()
}

func (m *Message[T]) IsRemoved() bool {
	return m.Queued == 0 &&
		m.DLQ == 0 &&
		m.CompleteFromQueueTimestamp != ""
}

func (m *Message[T]) IsDone() bool {
	return m.Queued == 0 &&
		m.DLQ == 0 &&
		m.CompleteFromQueueTimestamp != "" &&
		m.Status == StatusCompleted
}

func (m *Message[T]) IsReady() bool {
	return m.Queued == 1 &&
		m.DLQ == 0 &&
		m.Status == StatusReady &&
		m.AddToQueueTimestamp != "" &&
		m.CompleteFromQueueTimestamp == ""
}

func (m *Message[T]) IsDLQ() bool {
	return m.Queued == 0 &&
		m.DLQ == 1 &&
		m.AddToDLQTimestamp != "" &&
		m.Status == StatusInDLQ
}

func (m *Message[T]) MarkAsReady(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.LastUpdatedTimestamp = ts
	m.Status = StatusReady
}

func (m *Message[T]) MarkAsEnqueued(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.Queued = 1
	m.DLQ = 0
	m.LastUpdatedTimestamp = ts
	m.LastUpdatedTimestamp = ts
	m.AddToQueueTimestamp = ts
	m.Status = StatusReady
}

func (m *Message[T]) MarkAsPeeked(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.Queued = 1
	m.LastUpdatedTimestamp = ts
	m.LastUpdatedTimestamp = ts
	m.PeekFromQueueTimestamp = ts
	m.Status = StatusProcessing
}

func (m *Message[T]) MarkAsRemoved(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.Queued = 0
	m.DLQ = 0
	m.LastUpdatedTimestamp = ts
	m.CompleteFromQueueTimestamp = ts
}

func (m *Message[T]) MarkAsDone(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.Queued = 0
	m.DLQ = 0
	m.LastUpdatedTimestamp = ts
	m.Status = StatusCompleted
	m.LastUpdatedTimestamp = ts
	m.CompleteFromQueueTimestamp = ts
}

func (m *Message[T]) MarkAsDLQ(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.Queued = 0
	m.DLQ = 1
	m.LastUpdatedTimestamp = ts
	m.LastUpdatedTimestamp = ts
	m.AddToDLQTimestamp = ts
	m.Status = StatusInDLQ
}

func (m *Message[T]) ResetSystemInfo(now time.Time) {
	system := newDefaultSystemInfo(m.ID, now)
	m.Status = system.Status
	m.Queued = system.Queued
	m.DLQ = system.DLQ
	m.ReceiveCount = system.ReceiveCount
	m.Version = system.Version
	m.CreationTimestamp = system.CreationTimestamp
	m.LastUpdatedTimestamp = system.LastUpdatedTimestamp
	m.AddToQueueTimestamp = system.AddToQueueTimestamp
	m.PeekFromQueueTimestamp = system.PeekFromQueueTimestamp
	m.CompleteFromQueueTimestamp = system.CompleteFromQueueTimestamp
	m.AddToDLQTimestamp = system.AddToDLQTimestamp

}

func (m *Message[T]) Touch(now time.Time) {
	ts := clock.FormatRFC3339(now)
	m.LastUpdatedTimestamp = ts
}

func (m *Message[T]) Update(message *Message[T], now time.Time) {
	formatted := clock.FormatRFC3339(now)
	nextVersion := m.Version + 1

	m.Data = message.Data
	m.Status = message.Status
	m.Queued = message.Queued
	m.DLQ = message.DLQ
	m.ReceiveCount = message.ReceiveCount
	m.CreationTimestamp = message.CreationTimestamp
	m.LastUpdatedTimestamp = formatted
	m.AddToQueueTimestamp = message.AddToQueueTimestamp
	m.PeekFromQueueTimestamp = message.PeekFromQueueTimestamp
	m.CompleteFromQueueTimestamp = message.CompleteFromQueueTimestamp
	m.AddToDLQTimestamp = message.AddToDLQTimestamp
	m.Version = nextVersion
}

func (m *Message[T]) ChangeStatus(status Status, now time.Time) {
	formatted := clock.FormatRFC3339(now)
	m.Status = status
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

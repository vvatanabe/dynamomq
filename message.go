package dynamomq

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/dynamomq/internal/clock"
)

type SystemInfo struct {
	ID                     string    `json:"id" dynamodbav:"id"`
	Status                 Status    `json:"status" dynamodbav:"status"`
	ReceiveCount           int       `json:"receive_count" dynamodbav:"receive_count"`
	QueueType              QueueType `json:"queue_type" dynamodbav:"queue_type"`
	Version                int       `json:"version" dynamodbav:"version"`
	CreationTimestamp      string    `json:"creation_timestamp" dynamodbav:"creation_timestamp"`
	LastUpdatedTimestamp   string    `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	AddToQueueTimestamp    string    `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	PeekFromQueueTimestamp string    `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
}

func newDefaultSystemInfo(id string, now time.Time) *SystemInfo {
	ts := clock.FormatRFC3339(now)
	return &SystemInfo{
		ID:                     id,
		Status:                 StatusReady,
		ReceiveCount:           0,
		QueueType:              QueueTypeStandard,
		Version:                1,
		CreationTimestamp:      ts,
		LastUpdatedTimestamp:   ts,
		AddToQueueTimestamp:    ts,
		PeekFromQueueTimestamp: "",
	}
}

func NewDefaultMessage[T any](id string, data T, now time.Time) *Message[T] {
	system := newDefaultSystemInfo(id, now)
	return &Message[T]{
		ID:                     id,
		Data:                   data,
		Status:                 system.Status,
		QueueType:              system.QueueType,
		ReceiveCount:           system.ReceiveCount,
		Version:                system.Version,
		CreationTimestamp:      system.CreationTimestamp,
		LastUpdatedTimestamp:   system.LastUpdatedTimestamp,
		AddToQueueTimestamp:    system.AddToQueueTimestamp,
		PeekFromQueueTimestamp: system.PeekFromQueueTimestamp,
	}
}

type Message[T any] struct {
	ID                     string    `json:"id" dynamodbav:"id"`
	Data                   T         `json:"data" dynamodbav:"data"`
	Status                 Status    `json:"status" dynamodbav:"status"`
	ReceiveCount           int       `json:"receive_count" dynamodbav:"receive_count"`
	QueueType              QueueType `json:"queue_type" dynamodbav:"queue_type,omitempty"`
	Version                int       `json:"version" dynamodbav:"version"`
	CreationTimestamp      string    `json:"creation_timestamp" dynamodbav:"creation_timestamp"`
	LastUpdatedTimestamp   string    `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	AddToQueueTimestamp    string    `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	PeekFromQueueTimestamp string    `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
}

func (m *Message[T]) GetSystemInfo() *SystemInfo {
	return &SystemInfo{
		ID:                     m.ID,
		Status:                 m.Status,
		ReceiveCount:           m.ReceiveCount,
		QueueType:              m.QueueType,
		Version:                m.Version,
		CreationTimestamp:      m.CreationTimestamp,
		LastUpdatedTimestamp:   m.LastUpdatedTimestamp,
		AddToQueueTimestamp:    m.AddToQueueTimestamp,
		PeekFromQueueTimestamp: m.PeekFromQueueTimestamp,
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

func (m *Message[T]) IsDLQ() bool {
	return m.QueueType == QueueTypeDLQ
}

func (m *Message[T]) ResetSystemInfo(now time.Time) {
	system := newDefaultSystemInfo(m.ID, now)
	m.Status = system.Status
	m.QueueType = system.QueueType
	m.ReceiveCount = system.ReceiveCount
	m.Version = system.Version
	m.CreationTimestamp = system.CreationTimestamp
	m.LastUpdatedTimestamp = system.LastUpdatedTimestamp
	m.AddToQueueTimestamp = system.AddToQueueTimestamp
	m.PeekFromQueueTimestamp = system.PeekFromQueueTimestamp
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

func (m *Message[T]) Ready(now time.Time) error {
	if m.Status != StatusProcessing {
		return &InvalidStateTransitionError{
			Msg:       "message is currently ready",
			Operation: "Ready",
			Current:   m.Status,
		}
	}
	ts := clock.FormatRFC3339(now)
	m.Status = StatusReady
	m.LastUpdatedTimestamp = ts
	return nil
}

func (m *Message[T]) StartProcessing(now time.Time, visibilityTimeout time.Duration) error {
	if m.IsQueueSelected(now, visibilityTimeout) {
		return &InvalidStateTransitionError{
			Msg:       "message is currently being processed",
			Operation: "StartProcessing",
			Current:   m.Status,
		}
	}
	ts := clock.FormatRFC3339(now)
	m.Status = StatusProcessing
	m.LastUpdatedTimestamp = ts
	m.PeekFromQueueTimestamp = ts
	return nil
}

func (m *Message[T]) MoveToDLQ(now time.Time) error {
	if m.QueueType == QueueTypeDLQ {
		return &InvalidStateTransitionError{
			Msg:       "message is already in DLQ",
			Operation: "MoveToDLQ",
			Current:   m.Status,
		}
	}
	ts := clock.FormatRFC3339(now)
	m.QueueType = QueueTypeDLQ
	m.Status = StatusReady
	m.ReceiveCount = 0
	m.LastUpdatedTimestamp = ts
	m.AddToQueueTimestamp = ts
	m.PeekFromQueueTimestamp = ""
	return nil
}

func (m *Message[T]) RestoreFromDLQ(now time.Time) error {
	if m.QueueType != QueueTypeDLQ {
		return &InvalidStateTransitionError{
			Msg:       "can only redrive messages from DLQ",
			Operation: "RestoreFromDLQ",
			Current:   m.Status,
		}
	}
	if m.Status != StatusReady {
		return &InvalidStateTransitionError{
			Msg:       "can only redrive messages from READY",
			Operation: "RestoreFromDLQ",
			Current:   m.Status,
		}
	}
	ts := clock.FormatRFC3339(now)
	m.QueueType = QueueTypeStandard
	m.Status = StatusReady
	m.LastUpdatedTimestamp = ts
	m.AddToQueueTimestamp = ts
	return nil
}

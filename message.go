package dynamomq

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/dynamomq/internal/clock"
)

func NewMessage[T any](id string, data T, now time.Time) *Message[T] {
	ts := clock.FormatRFC3339Nano(now)
	return &Message[T]{
		ID:                     id,
		Data:                   data,
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

func (m *Message[T]) marshalMap() (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(m)
	if err != nil {
		return nil, MarshalingAttributeError{Cause: err}
	}
	return item, nil
}

func (m *Message[T]) isQueueSelected(now time.Time, visibilityTimeout time.Duration) bool {
	if m.Status != StatusProcessing {
		return false
	}
	peekUTCTimestamp := clock.RFC3339NanoToUnixMilli(m.PeekFromQueueTimestamp)
	timeDifference := now.UnixMilli() - peekUTCTimestamp
	return timeDifference <= visibilityTimeout.Milliseconds()
}

func (m *Message[T]) isDLQ() bool {
	return m.QueueType == QueueTypeDLQ
}

func (m *Message[T]) markAsReady(now time.Time) error {
	if m.Status != StatusProcessing {
		return InvalidStateTransitionError{
			Msg:       "message is currently ready",
			Operation: "mark as ready",
			Current:   m.Status,
		}
	}
	ts := clock.FormatRFC3339Nano(now)
	m.Status = StatusReady
	m.LastUpdatedTimestamp = ts
	return nil
}

func (m *Message[T]) markAsProcessing(now time.Time, visibilityTimeout time.Duration) error {
	if m.isQueueSelected(now, visibilityTimeout) {
		return InvalidStateTransitionError{
			Msg:       "message is currently being processed",
			Operation: "mark as processing",
			Current:   m.Status,
		}
	}
	ts := clock.FormatRFC3339Nano(now)
	m.Status = StatusProcessing
	m.LastUpdatedTimestamp = ts
	m.PeekFromQueueTimestamp = ts
	return nil
}

func (m *Message[T]) markAsMovedToDLQ(now time.Time) error {
	if m.isDLQ() {
		return InvalidStateTransitionError{
			Msg:       "message is already in DLQ",
			Operation: "mark as moved to DLQ",
			Current:   m.Status,
		}
	}
	ts := clock.FormatRFC3339Nano(now)
	m.QueueType = QueueTypeDLQ
	m.Status = StatusReady
	m.ReceiveCount = 0
	m.LastUpdatedTimestamp = ts
	m.AddToQueueTimestamp = ts
	m.PeekFromQueueTimestamp = ""
	return nil
}

func (m *Message[T]) markAsRestoredFromDLQ(now time.Time, visibilityTimeout time.Duration) error {
	if !m.isDLQ() {
		return InvalidStateTransitionError{
			Msg:       "can only redrive messages from DLQ",
			Operation: "mark as restored from DLQ",
			Current:   m.Status,
		}
	}
	if m.isQueueSelected(now, visibilityTimeout) {
		return InvalidStateTransitionError{
			Msg:       "can only redrive messages from READY",
			Operation: "mark as restored from DLQ",
			Current:   m.Status,
		}
	}
	ts := clock.FormatRFC3339Nano(now)
	m.QueueType = QueueTypeStandard
	m.Status = StatusReady
	m.LastUpdatedTimestamp = ts
	m.AddToQueueTimestamp = ts
	return nil
}

package dynamomq

import (
	"time"

	"github.com/vvatanabe/dynamomq/internal/clock"
)

func NewMessage[T any](id string, data T, now time.Time) *Message[T] {
	ts := clock.FormatRFC3339Nano(now)
	return &Message[T]{
		ID:                     id,
		Data:                   data,
		ReceiveCount:           0,
		VisibilityTimeout:      0,
		QueueType:              QueueTypeStandard,
		Version:                1,
		CreationTimestamp:      ts,
		LastUpdatedTimestamp:   ts,
		AddToQueueTimestamp:    ts,
		PeekFromQueueTimestamp: "",
	}
}

type Message[T any] struct {
	ID   string `json:"id" dynamodbav:"id"`
	Data T      `json:"data" dynamodbav:"data"`
	// The new value for the message's visibility timeout (in seconds).
	VisibilityTimeout      int       `json:"visibility_timeout" dynamodbav:"visibility_timeout"`
	ReceiveCount           int       `json:"receive_count" dynamodbav:"receive_count"`
	QueueType              QueueType `json:"queue_type" dynamodbav:"queue_type,omitempty"`
	Version                int       `json:"version" dynamodbav:"version"`
	CreationTimestamp      string    `json:"creation_timestamp" dynamodbav:"creation_timestamp"`
	LastUpdatedTimestamp   string    `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	AddToQueueTimestamp    string    `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	PeekFromQueueTimestamp string    `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
}

func (m *Message[T]) GetStatus(now time.Time) Status {
	peekUTCTime := clock.RFC3339NanoToTime(m.PeekFromQueueTimestamp)
	invisibleTime := peekUTCTime.Add(time.Duration(m.VisibilityTimeout) * time.Second)
	if now.Before(invisibleTime) {
		return StatusProcessing
	}
	return StatusReady
}

func (m *Message[T]) isDLQ() bool {
	return m.QueueType == QueueTypeDLQ
}

func (m *Message[T]) changeVisibilityTimeout(now time.Time, visibilityTimeout int) {
	ts := clock.FormatRFC3339Nano(now)
	m.LastUpdatedTimestamp = ts
	m.VisibilityTimeout = visibilityTimeout
}

func (m *Message[T]) delayToAddQueueTimestamp(delay time.Duration) {
	delayed := clock.RFC3339NanoToTime(m.AddToQueueTimestamp).Add(delay)
	m.AddToQueueTimestamp = clock.FormatRFC3339Nano(delayed)
}

func (m *Message[T]) markAsProcessing(now time.Time, visibilityTimeout int) error {
	status := m.GetStatus(now)
	if status == StatusProcessing {
		return InvalidStateTransitionError{
			Msg:       "message is currently being processed",
			Operation: "mark as processing",
			Current:   status,
		}
	}
	ts := clock.FormatRFC3339Nano(now)
	m.LastUpdatedTimestamp = ts
	m.PeekFromQueueTimestamp = ts
	m.VisibilityTimeout = visibilityTimeout
	return nil
}

func (m *Message[T]) markAsMovedToDLQ(now time.Time) error {
	if m.isDLQ() {
		return InvalidStateTransitionError{
			Msg:       "message is already in DLQ",
			Operation: "mark as moved to DLQ",
			Current:   m.GetStatus(now),
		}
	}
	ts := clock.FormatRFC3339Nano(now)
	m.QueueType = QueueTypeDLQ
	m.ReceiveCount = 0
	m.VisibilityTimeout = 0
	m.LastUpdatedTimestamp = ts
	m.AddToQueueTimestamp = ts
	m.PeekFromQueueTimestamp = ""
	return nil
}

func (m *Message[T]) markAsRestoredFromDLQ(now time.Time) error {
	status := m.GetStatus(now)
	if !m.isDLQ() {
		return InvalidStateTransitionError{
			Msg:       "can only redrive messages from DLQ",
			Operation: "mark as restored from DLQ",
			Current:   status,
		}
	}
	if status == StatusProcessing {
		return InvalidStateTransitionError{
			Msg:       "can only redrive messages from READY",
			Operation: "mark as restored from DLQ",
			Current:   status,
		}
	}
	ts := clock.FormatRFC3339Nano(now)
	m.QueueType = QueueTypeStandard
	m.VisibilityTimeout = 0
	m.ReceiveCount = 0
	m.LastUpdatedTimestamp = ts
	m.AddToQueueTimestamp = ts
	m.PeekFromQueueTimestamp = ""
	return nil
}

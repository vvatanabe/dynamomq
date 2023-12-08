package dynamomq

import (
	"time"

	"github.com/vvatanabe/dynamomq/internal/clock"
)

func NewMessage[T any](id string, data T, now time.Time) *Message[T] {
	ts := clock.FormatRFC3339Nano(now)
	return &Message[T]{
		ID:               id,
		Data:             data,
		ReceiveCount:     0,
		QueueType:        QueueTypeStandard,
		Version:          1,
		CreatedAt:        ts,
		UpdatedAt:        ts,
		SentAt:           ts,
		ReceivedAt:       "",
		InvisibleUntilAt: "",
	}
}

type Message[T any] struct {
	ID               string    `json:"id" dynamodbav:"id"`
	Data             T         `json:"data" dynamodbav:"data"`
	ReceiveCount     int       `json:"receive_count" dynamodbav:"receive_count"`
	QueueType        QueueType `json:"queue_type" dynamodbav:"queue_type,omitempty"`
	Version          int       `json:"version" dynamodbav:"version"`
	CreatedAt        string    `json:"created_at" dynamodbav:"created_at"`
	UpdatedAt        string    `json:"updated_at" dynamodbav:"updated_at"`
	SentAt           string    `json:"sent_at" dynamodbav:"sent_at"`
	ReceivedAt       string    `json:"received_at" dynamodbav:"received_at"`
	InvisibleUntilAt string    `json:"invisible_until_at" dynamodbav:"invisible_until_at"`
}

func (m *Message[T]) GetStatus(now time.Time) Status {
	if m.InvisibleUntilAt == "" {
		return StatusReady
	}
	invisibleUntilAtTime := clock.RFC3339NanoToTime(m.InvisibleUntilAt)
	if now.After(invisibleUntilAtTime) {
		return StatusReady
	}
	return StatusProcessing
}

func (m *Message[T]) isDLQ() bool {
	return m.QueueType == QueueTypeDLQ
}

func (m *Message[T]) changeVisibility(now time.Time, visibilityTimeout time.Duration) {
	ts := clock.FormatRFC3339Nano(now)
	m.UpdatedAt = ts
	m.InvisibleUntilAt = clock.FormatRFC3339Nano(now.Add(visibilityTimeout))
}

func (m *Message[T]) delayToSentAt(delay time.Duration) {
	delayed := clock.RFC3339NanoToTime(m.SentAt).Add(delay)
	m.SentAt = clock.FormatRFC3339Nano(delayed)
}

func (m *Message[T]) markAsProcessing(now time.Time, visibilityTimeout time.Duration) error {
	status := m.GetStatus(now)
	if status == StatusProcessing {
		return InvalidStateTransitionError{
			Msg:       "message is currently being processed",
			Operation: "mark as processing",
			Current:   status,
		}
	}
	ts := clock.FormatRFC3339Nano(now)
	m.UpdatedAt = ts
	m.ReceivedAt = ts
	m.InvisibleUntilAt = clock.FormatRFC3339Nano(now.Add(visibilityTimeout))
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
	m.UpdatedAt = ts
	m.SentAt = ts
	m.ReceivedAt = ""
	m.InvisibleUntilAt = ""
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
	m.ReceiveCount = 0
	m.UpdatedAt = ts
	m.SentAt = ts
	m.ReceivedAt = ""
	m.InvisibleUntilAt = ""
	return nil
}

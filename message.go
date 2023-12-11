package dynamomq

import (
	"time"

	"github.com/vvatanabe/dynamomq/internal/clock"
)

// Status represents the state of a message in a DynamoDB-based queue.
type Status string

// Constants defining various states of a message in the queue.
const (
	// StatusReady indicates that a message is ready to be processed.
	StatusReady Status = "READY"
	// StatusProcessing indicates that a message is currently being processed.
	StatusProcessing Status = "PROCESSING"
)

// QueueType represents the type of queue in a DynamoDB-based messaging system.
type QueueType string

// Constants defining the types of queues available.
const (
	// QueueTypeStandard represents a standard queue.
	QueueTypeStandard QueueType = "STANDARD"
	// QueueTypeDLQ represents a Dead Letter Queue, used for holding messages that failed to process.
	QueueTypeDLQ QueueType = "DLQ"
)

// NewMessage creates a new instance of a Message with the provided data and initializes its timestamps.
// This function is a constructor for Message, setting initial values and preparing the message for use in the queue.
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

// Message represents a message structure in a DynamoDB-based queue system.
// It uses the generic type T for the message content, allowing for flexibility in the data type of the message payload.
// This struct includes tags for JSON serialization (`json:"..."`) and DynamoDB attribute value (`dynamodbav:"..."`) mappings.
type Message[T any] struct {
	// ID is a unique identifier for the message.
	ID string `json:"id" dynamodbav:"id"`
	// Data is the content of the message. The type T defines the format of this data.
	Data T `json:"data" dynamodbav:"data"`
	// ReceiveCount is the number of times the message has been received from the queue.
	ReceiveCount int `json:"receive_count" dynamodbav:"receive_count"`
	// QueueType is the type of queue (standard or DLQ) to which the message belongs.
	QueueType QueueType `json:"queue_type" dynamodbav:"queue_type,omitempty"`
	// Version is the version number of the message, used for optimistic concurrency control.
	Version int `json:"version" dynamodbav:"version"`
	// CreatedAt is the timestamp when the message was created.
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`
	// UpdatedAt is the timestamp when the message was last updated.
	UpdatedAt string `json:"updated_at" dynamodbav:"updated_at"`
	// SentAt is the timestamp when the message was sent to the queue.
	SentAt string `json:"sent_at" dynamodbav:"sent_at"`
	// ReceivedAt is the timestamp when the message was last received from the queue.
	ReceivedAt string `json:"received_at" dynamodbav:"received_at"`
	// InvisibleUntilAt: The deadline until which the message remains invisible in the queue.
	// Until this timestamp, the message will not be visible to other consumers.
	InvisibleUntilAt string `json:"invisible_until_at" dynamodbav:"invisible_until_at"`
}

// GetStatus determines the current status of the message based on the provided time.
// It returns the status as either 'StatusReady' or 'StatusProcessing'.
//
// StatusReady if the message is ready to be processed (either 'InvisibleUntilAt' is empty
// or the current time is after the 'InvisibleUntilAt' time).
// StatusProcessing if the current time is before the 'InvisibleUntilAt' time, indicating
// that the message is currently being processed and is not yet ready for further processing.
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

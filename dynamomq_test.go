package dynamomq_test

import (
	"time"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func MarkAsReady[T any](m *dynamomq.Message[T], now time.Time) {
	ts := clock.FormatRFC3339Nano(now)
	m.Status = dynamomq.StatusReady
	m.LastUpdatedTimestamp = ts
}

func MarkAsProcessing[T any](m *dynamomq.Message[T], now time.Time) {
	ts := clock.FormatRFC3339Nano(now)
	m.Status = dynamomq.StatusProcessing
	m.LastUpdatedTimestamp = ts
	m.PeekFromQueueTimestamp = ts
}

func MarkAsMovedToDLQ[T any](m *dynamomq.Message[T], now time.Time) {
	ts := clock.FormatRFC3339Nano(now)
	m.QueueType = dynamomq.QueueTypeDLQ
	m.Status = dynamomq.StatusReady
	m.ReceiveCount = 0
	m.LastUpdatedTimestamp = ts
	m.AddToQueueTimestamp = ts
	m.PeekFromQueueTimestamp = ""
}

func NewTestMessageItemAsReady(id string, now time.Time) *dynamomq.Message[test.MessageData] {
	return dynamomq.NewMessage[test.MessageData](id, test.NewMessageData(id), now)
}

func NewTestMessageItemAsProcessing(id string, now time.Time) *dynamomq.Message[test.MessageData] {
	m := dynamomq.NewMessage[test.MessageData](id, test.NewMessageData(id), now)
	MarkAsProcessing(m, now)
	return m
}

func NewTestMessageItemAsDLQ(id string, now time.Time) *dynamomq.Message[test.MessageData] {
	m := dynamomq.NewMessage[test.MessageData](id, test.NewMessageData(id), now)
	MarkAsMovedToDLQ(m, now)
	return m
}

func NewMessageFromReadyToProcessing(id string,
	readyTime time.Time, processingTime time.Time) *dynamomq.ReceiveMessageOutput[test.MessageData] {
	m := NewTestMessageItemAsReady(id, readyTime)
	MarkAsProcessing(m, processingTime)
	m.Version = 2
	m.ReceiveCount = 1
	r := &dynamomq.ReceiveMessageOutput[test.MessageData]{
		Result: &dynamomq.Result{
			ID:                   m.ID,
			Status:               m.Status,
			LastUpdatedTimestamp: m.LastUpdatedTimestamp,
			Version:              m.Version,
		},
		PeekFromQueueTimestamp: m.PeekFromQueueTimestamp,
		PeekedMessageObject:    m,
	}
	return r
}

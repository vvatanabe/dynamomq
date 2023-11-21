package test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
)

func NewMessageData(id string) MessageData {
	return MessageData{
		ID:    id,
		Data1: "Data 1",
		Data2: "Data 2",
		Data3: "Data 3",
		Items: []MessageItem{
			{SKU: "Item-1", Packed: true},
			{SKU: "Item-2", Packed: true},
			{SKU: "Item-3", Packed: true},
		},
	}
}

type MessageData struct {
	ID    string        `json:"id" dynamodbav:"id"`
	Items []MessageItem `json:"items" dynamodbav:"items"`
	Data1 string        `json:"data_element_1" dynamodbav:"data_1"`
	Data2 string        `json:"data_element_2" dynamodbav:"data_2"`
	Data3 string        `json:"data_element_3" dynamodbav:"data_3"`
}

type MessageItem struct {
	SKU    string `json:"SKU" dynamodbav:"SKU"`
	Packed bool   `json:"is_packed" dynamodbav:"is_packed"`
}

var (
	ErrorTest       = errors.New("test")
	DefaultTestDate = Date(2023, 12, 1, 0, 0, 0)
)

func Date(year int, month time.Month, day, hour, min, sec int) time.Time {
	return time.Date(year, month, day, hour, min, sec, 0, time.UTC)
}

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

func NewTestMessageItemAsReady(id string, now time.Time) *dynamomq.Message[MessageData] {
	return dynamomq.NewMessage[MessageData](id, NewMessageData(id), now)
}

func NewTestMessageItemAsProcessing(id string, now time.Time) *dynamomq.Message[MessageData] {
	m := dynamomq.NewMessage[MessageData](id, NewMessageData(id), now)
	MarkAsProcessing(m, now)
	return m
}

func NewTestMessageItemAsDLQ(id string, now time.Time) *dynamomq.Message[MessageData] {
	m := dynamomq.NewMessage[MessageData](id, NewMessageData(id), now)
	MarkAsMovedToDLQ(m, now)
	return m
}

func NewMessageFromReadyToProcessing(id string,
	readyTime time.Time, processingTime time.Time) *dynamomq.ReceiveMessageOutput[MessageData] {
	m := NewTestMessageItemAsReady(id, readyTime)
	MarkAsProcessing(m, processingTime)
	m.Version = 2
	m.ReceiveCount = 1
	r := &dynamomq.ReceiveMessageOutput[MessageData]{
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

func AssertError(t *testing.T, got, want error, prefix string) error {
	t.Helper()
	if want != nil {
		if !errors.Is(got, want) {
			t.Errorf("%s error = %v, want %v", prefix, got, want)
			return got
		}
		return nil
	}
	if got != nil {
		t.Errorf("%s unexpected error = %v", prefix, got)
		return got
	}
	return nil
}

func AssertDeepEqual(t *testing.T, got, want any, prefix string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		v1, _ := json.Marshal(got)
		v2, _ := json.Marshal(want)
		t.Errorf("%s got = %v, want %v", prefix, string(v1), string(v2))
	}
}

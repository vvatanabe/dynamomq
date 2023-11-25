package test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
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
	DefaultTestDate = time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)
)

func AssertError(t *testing.T, got, want error, prefix string) {
	t.Helper()
	if want != nil {
		if !errors.Is(got, want) {
			t.Errorf("%s error = %v, want %v", prefix, got, want)
			return
		}
		return
	}
	if got != nil {
		t.Errorf("%s unexpected error = %v", prefix, got)
		return
	}
	return
}

func AssertDeepEqual(t *testing.T, got, want any, prefix string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		v1, _ := json.Marshal(got)
		v2, _ := json.Marshal(want)
		t.Errorf("%s got = %v, want %v", prefix, string(v1), string(v2))
	}
}

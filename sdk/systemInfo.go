package sdk

import (
	"time"

	"github.com/vvatanabe/dynamomq/internal/clock"
)

type SystemInfo struct {
	ID                       string `json:"id" dynamodbav:"id"`
	CreationTimestamp        string `json:"creation_timestamp" dynamodbav:"creation_timestamp"`
	LastUpdatedTimestamp     string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	Status                   Status `json:"status" dynamodbav:"status"`
	Version                  int    `json:"version" dynamodbav:"version"`
	InQueue                  int    `json:"queued" dynamodbav:"queued"`
	SelectedFromQueue        bool   `json:"queue_selected" dynamodbav:"queue_selected"`
	AddToQueueTimestamp      string `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	AddToDLQTimestamp        string `json:"dlq_add_timestamp" dynamodbav:"dlq_add_timestamp"`
	PeekFromQueueTimestamp   string `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
	RemoveFromQueueTimestamp string `json:"queue_remove_timestamp" dynamodbav:"queue_remove_timestamp"`

	PeekUTCTimestamp int64 `json:"peek_utc_timestamp" dynamodbav:"peek_utc_timestamp,omitempty"`
}

func NewDefaultSystemInfo(id string, now time.Time) *SystemInfo {
	ts := clock.FormatRFC3339(now)
	return &SystemInfo{
		ID:                       id,
		CreationTimestamp:        ts,
		LastUpdatedTimestamp:     ts,
		Status:                   StatusUnderConstruction,
		Version:                  1,
		InQueue:                  0,
		SelectedFromQueue:        false,
		AddToQueueTimestamp:      "",
		AddToDLQTimestamp:        "",
		PeekFromQueueTimestamp:   "",
		RemoveFromQueueTimestamp: "",
		PeekUTCTimestamp:         0,
	}
}

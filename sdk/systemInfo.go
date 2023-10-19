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
	ReceiveCount             int    `json:"receive_count" dynamodbav:"receive_count"`
	Version                  int    `json:"version" dynamodbav:"version"`
	InQueue                  int    `json:"queued" dynamodbav:"queued"`
	AddToQueueTimestamp      string `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	AddToDLQTimestamp        string `json:"dlq_add_timestamp" dynamodbav:"dlq_add_timestamp"`
	PeekFromQueueTimestamp   string `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
	RemoveFromQueueTimestamp string `json:"queue_remove_timestamp" dynamodbav:"queue_remove_timestamp"`
}

func NewDefaultSystemInfo(id string, now time.Time) *SystemInfo {
	ts := clock.FormatRFC3339(now)
	return &SystemInfo{
		ID:                       id,
		CreationTimestamp:        ts,
		LastUpdatedTimestamp:     ts,
		Status:                   StatusPending,
		ReceiveCount:             0,
		Version:                  1,
		InQueue:                  0,
		AddToQueueTimestamp:      "",
		AddToDLQTimestamp:        "",
		PeekFromQueueTimestamp:   "",
		RemoveFromQueueTimestamp: "",
	}
}

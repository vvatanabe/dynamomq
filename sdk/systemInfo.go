package sdk

import (
	"time"

	"github.com/vvatanabe/dynamomq/internal/clock"
)

type SystemInfo struct {
	ID                       string `json:"id" dynamodbav:"id"`
	Status                   Status `json:"status" dynamodbav:"status"`
	ReceiveCount             int    `json:"receive_count" dynamodbav:"receive_count"`
	InQueue                  int    `json:"queued" dynamodbav:"queued"`
	Version                  int    `json:"version" dynamodbav:"version"`
	CreationTimestamp        string `json:"creation_timestamp" dynamodbav:"creation_timestamp"`
	LastUpdatedTimestamp     string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	AddToQueueTimestamp      string `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	PeekFromQueueTimestamp   string `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
	RemoveFromQueueTimestamp string `json:"queue_remove_timestamp" dynamodbav:"queue_remove_timestamp"`
	AddToDLQTimestamp        string `json:"dlq_add_timestamp" dynamodbav:"dlq_add_timestamp"`
}

func NewDefaultSystemInfo(id string, now time.Time) *SystemInfo {
	ts := clock.FormatRFC3339(now)
	return &SystemInfo{
		ID:                       id,
		Status:                   StatusPending,
		ReceiveCount:             0,
		InQueue:                  0,
		Version:                  1,
		CreationTimestamp:        ts,
		LastUpdatedTimestamp:     ts,
		AddToQueueTimestamp:      "",
		PeekFromQueueTimestamp:   "",
		RemoveFromQueueTimestamp: "",
		AddToDLQTimestamp:        "",
	}
}

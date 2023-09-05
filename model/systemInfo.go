package model

import (
	"time"
)

type SystemInfo struct {
	ID                       string     `json:"id" dynamodbav:"id"`
	CreationTimestamp        string     `json:"creation_timestamp" dynamodbav:"creation_timestamp"`
	LastUpdatedTimestamp     string     `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	Status                   StatusEnum `json:"status" dynamodbav:"status"`
	Version                  int        `json:"version" dynamodbav:"version"`
	InQueue                  bool       `json:"queued" dynamodbav:"queued"`
	SelectedFromQueue        bool       `json:"queue_selected" dynamodbav:"queue_selected"`
	AddToQueueTimestamp      string     `json:"queue_add_timestamp" dynamodbav:"queue_add_timestamp"`
	AddToDlqTimestamp        string     `json:"dlq_add_timestamp" dynamodbav:"dlq_add_timestamp"`
	PeekFromQueueTimestamp   string     `json:"queue_peek_timestamp" dynamodbav:"queue_peek_timestamp"`
	RemoveFromQueueTimestamp string     `json:"queue_remove_timestamp" dynamodbav:"queue_remove_timestamp"`
}

func NewSystemInfo() *SystemInfo {
	odt := time.Now().UTC().Format(time.RFC3339)
	return &SystemInfo{
		CreationTimestamp:    odt,
		LastUpdatedTimestamp: odt,
	}
}

func NewSystemInfoWithID(id string) *SystemInfo {
	odt := time.Now().UTC().Format(time.RFC3339)
	return &SystemInfo{
		ID:                   id,
		CreationTimestamp:    odt,
		LastUpdatedTimestamp: odt,
	}
}

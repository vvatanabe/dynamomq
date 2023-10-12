package sdk

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/dynamomq/internal/clock"
)

func NewDefaultShipment[T any](id string, data *T, now time.Time) *Shipment[T] {
	return &Shipment[T]{
		ID:                   id,
		Data:                 data,
		SystemInfo:           NewDefaultSystemInfo(id, now),
		Queued:               0,
		LastUpdatedTimestamp: clock.FormatRFC3339(now),
		DLQ:                  0,
	}
}

type Shipment[T any] struct {
	ID         string      `json:"id" dynamodbav:"id"`
	Data       *T          `json:"data" dynamodbav:"data"`
	SystemInfo *SystemInfo `json:"system_info" dynamodbav:"system_info"`

	Queued               int    `json:"queued" dynamodbav:"queued,omitempty"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp,omitempty"`
	DLQ                  int    `json:"DLQ" dynamodbav:"DLQ,omitempty"`
}

func (s *Shipment[T]) IsQueueSelected(now time.Time, visibilityTimeout time.Duration) bool {
	if !s.SystemInfo.SelectedFromQueue {
		return false
	}
	timeDifference := now.UnixMilli() - s.SystemInfo.PeekUTCTimestamp
	return timeDifference <= visibilityTimeout.Milliseconds()
}

func (s *Shipment[T]) IsRemoved() bool {
	return s.Queued == 0 &&
		s.DLQ == 0 &&
		s.SystemInfo.InQueue == 0 &&
		s.SystemInfo.SelectedFromQueue == false &&
		s.SystemInfo.RemoveFromQueueTimestamp != ""
}

func (s *Shipment[T]) IsEnqueued() bool {
	return s.Queued == 1 &&
		s.DLQ == 0 &&
		s.SystemInfo.InQueue == 1 &&
		s.SystemInfo.SelectedFromQueue == false &&
		s.SystemInfo.Status == StatusReady &&
		s.SystemInfo.AddToQueueTimestamp != "" &&
		s.SystemInfo.RemoveFromQueueTimestamp == ""
}

func (s *Shipment[T]) IsDLQ() bool {
	return s.Queued == 0 &&
		s.DLQ == 1 &&
		s.SystemInfo.InQueue == 0 &&
		s.SystemInfo.SelectedFromQueue == false &&
		s.SystemInfo.AddToDLQTimestamp != "" &&
		s.SystemInfo.Status == StatusInDLQ
}

func (s *Shipment[T]) MarkAsReadyForShipment(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.Status = StatusReady
}

func (s *Shipment[T]) MarkAsEnqueued(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.Queued = 1
	s.DLQ = 0
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.InQueue = 1
	s.SystemInfo.SelectedFromQueue = false
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.AddToQueueTimestamp = ts
	s.SystemInfo.Status = StatusReady
}

func (s *Shipment[T]) MarkAsPeeked(now time.Time) {
	ts := clock.FormatRFC3339(now)
	unixTime := now.UnixMilli()
	s.Queued = 1
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.InQueue = 1
	s.SystemInfo.SelectedFromQueue = true
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.PeekFromQueueTimestamp = ts
	s.SystemInfo.PeekUTCTimestamp = unixTime
	s.SystemInfo.Status = StatusProcessing
}

func (s *Shipment[T]) MarkAsRemoved(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.Queued = 0
	s.DLQ = 0
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.InQueue = 0
	s.SystemInfo.SelectedFromQueue = false
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.RemoveFromQueueTimestamp = ts
}

func (s *Shipment[T]) MarkAsDLQ(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.Queued = 0
	s.DLQ = 1
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.InQueue = 0
	s.SystemInfo.SelectedFromQueue = false
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.AddToDLQTimestamp = ts
	s.SystemInfo.Status = StatusInDLQ
}

func (s *Shipment[T]) ResetSystemInfo(now time.Time) {
	s.SystemInfo = NewDefaultSystemInfo(s.ID, now)
}

func (s *Shipment[T]) Touch(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.LastUpdatedTimestamp = ts
}

func (s *Shipment[T]) Update(shipment *Shipment[T], now time.Time) {
	formatted := clock.FormatRFC3339(now)
	nextVersion := s.SystemInfo.Version + 1

	s.Data = shipment.Data
	s.SystemInfo = shipment.SystemInfo
	s.SystemInfo.Version = nextVersion
	s.SystemInfo.LastUpdatedTimestamp = formatted

	s.Queued = shipment.Queued
	s.LastUpdatedTimestamp = formatted
	s.DLQ = shipment.DLQ
}

func (s *Shipment[T]) ChangeStatus(status Status, now time.Time) {
	formatted := clock.FormatRFC3339(now)

	s.SystemInfo.Status = status
	s.SystemInfo.LastUpdatedTimestamp = formatted
	s.LastUpdatedTimestamp = formatted
}

func (s *Shipment[T]) MarshalMap() (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(s)
	if err != nil {
		return nil, &MarshalingAttributeError{Cause: err}
	}
	return item, nil
}

func (s *Shipment[T]) MarshalMapUnsafe() map[string]types.AttributeValue {
	item, _ := attributevalue.MarshalMap(s)
	return item
}

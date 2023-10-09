package sdk

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/go82f46979/internal/clock"
)

func NewDefaultShipment(id string, data *ShipmentData, now time.Time) *Shipment {
	return &Shipment{
		ID:         id,
		Data:       data,
		SystemInfo: NewDefaultSystemInfo(id, now),
	}
}

type Shipment struct {
	ID         string        `json:"id" dynamodbav:"id"`
	Data       *ShipmentData `json:"data" dynamodbav:"data"`
	SystemInfo *SystemInfo   `json:"system_info" dynamodbav:"system_info"`

	Queued               int    `json:"queued" dynamodbav:"queued,omitempty"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp,omitempty"`
	DLQ                  int    `json:"DLQ" dynamodbav:"DLQ,omitempty"`
}

func (s *Shipment) IsQueueSelected(now time.Time, visibilityTimeout time.Duration) bool {
	if !s.SystemInfo.SelectedFromQueue {
		return false
	}
	timeDifference := now.UnixMilli() - s.SystemInfo.PeekUTCTimestamp
	return timeDifference <= visibilityTimeout.Milliseconds()
}

func (s *Shipment) IsRemoved() bool {
	return s.Queued == 0 &&
		s.DLQ == 0 &&
		s.SystemInfo.InQueue == 0 &&
		s.SystemInfo.SelectedFromQueue == false &&
		s.SystemInfo.RemoveFromQueueTimestamp != ""
}

func (s *Shipment) IsEnqueued() bool {
	return s.Queued == 1 &&
		s.DLQ == 0 &&
		s.SystemInfo.InQueue == 1 &&
		s.SystemInfo.SelectedFromQueue == false &&
		s.SystemInfo.Status == StatusReadyToShip &&
		s.SystemInfo.AddToQueueTimestamp != "" &&
		s.SystemInfo.RemoveFromQueueTimestamp == ""
}

func (s *Shipment) IsDLQ() bool {
	return s.Queued == 0 &&
		s.DLQ == 1 &&
		s.SystemInfo.InQueue == 0 &&
		s.SystemInfo.SelectedFromQueue == false &&
		s.SystemInfo.AddToDLQTimestamp != "" &&
		s.SystemInfo.Status == StatusInDLQ
}

func (s *Shipment) MarkAsReadyForShipment(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.Status = StatusReadyToShip
}

func (s *Shipment) MarkAsEnqueued(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.Queued = 1
	s.DLQ = 0
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.InQueue = 1
	s.SystemInfo.SelectedFromQueue = false
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.AddToQueueTimestamp = ts
	s.SystemInfo.Status = StatusReadyToShip
}

func (s *Shipment) MarkAsPeeked(now time.Time) {
	ts := clock.FormatRFC3339(now)
	unixTime := now.UnixMilli()
	s.Queued = 1
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.InQueue = 1
	s.SystemInfo.SelectedFromQueue = true
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.PeekFromQueueTimestamp = ts
	s.SystemInfo.PeekUTCTimestamp = unixTime
	s.SystemInfo.Status = StatusProcessingShipment
}

func (s *Shipment) MarkAsRemoved(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.Queued = 0
	s.DLQ = 0
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.InQueue = 0
	s.SystemInfo.SelectedFromQueue = false
	s.SystemInfo.LastUpdatedTimestamp = ts
	s.SystemInfo.RemoveFromQueueTimestamp = ts
}

func (s *Shipment) MarkAsDLQ(now time.Time) {
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

func (s *Shipment) ResetSystemInfo(now time.Time) {
	s.SystemInfo = NewDefaultSystemInfo(s.ID, now)
}

func (s *Shipment) Touch(now time.Time) {
	ts := clock.FormatRFC3339(now)
	s.LastUpdatedTimestamp = ts
	s.SystemInfo.LastUpdatedTimestamp = ts
}

func (s *Shipment) Update(shipment *Shipment, now time.Time) {
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

func (s *Shipment) ChangeStatus(status Status, now time.Time) {
	formatted := clock.FormatRFC3339(now)

	s.SystemInfo.Status = status
	s.SystemInfo.LastUpdatedTimestamp = formatted
	s.LastUpdatedTimestamp = formatted
}

func (s *Shipment) MarshalMap() (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(s)
	if err != nil {
		return nil, &MarshalingAttributeError{Cause: err}
	}
	return item, nil
}

func (s *Shipment) MarshalMapUnsafe() map[string]types.AttributeValue {
	item, _ := attributevalue.MarshalMap(s)
	return item
}

type ShipmentData struct {
	ID    string         `json:"id" dynamodbav:"id"`
	Items []ShipmentItem `json:"items" dynamodbav:"items"`
	Data1 string         `json:"data_element_1" dynamodbav:"data_1"`
	Data2 string         `json:"data_element_2" dynamodbav:"data_2"`
	Data3 string         `json:"data_element_3" dynamodbav:"data_3"`
}

type ShipmentItem struct {
	SKU    string `json:"SKU" dynamodbav:"SKU"`
	Packed bool   `json:"is_packed" dynamodbav:"is_packed"`
}

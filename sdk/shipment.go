package sdk

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewShipmentWithIDAndData(id string, data *ShipmentData) *Shipment {
	return &Shipment{
		ID:         id,
		Data:       data,
		SystemInfo: NewSystemInfoWithID(id),
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

func (s *Shipment) MarkAsReadyForShipment() {
	s.SystemInfo.Status = StatusReadyToShip
}

func (s *Shipment) MarkAsEnqueued() {
	now := formattedCurrentTime()
	s.Queued = 1
	s.LastUpdatedTimestamp = now
	s.SystemInfo.InQueue = 1
	s.SystemInfo.SelectedFromQueue = false
	s.SystemInfo.LastUpdatedTimestamp = now
	s.SystemInfo.AddToQueueTimestamp = now
	s.SystemInfo.Status = StatusReadyToShip
}

func (s *Shipment) MarkAsPeeked() {
	now := now()
	unixTime := now.UnixMilli()
	formattedTime := formattedTime(now)
	s.Queued = 1
	s.LastUpdatedTimestamp = formattedTime
	s.SystemInfo.InQueue = 1
	s.SystemInfo.SelectedFromQueue = true
	s.SystemInfo.LastUpdatedTimestamp = formattedTime
	s.SystemInfo.AddToQueueTimestamp = formattedTime
	s.SystemInfo.PeekUTCTimestamp = unixTime
	s.SystemInfo.Status = StatusProcessingShipment
}

func (s *Shipment) ResetSystemInfo() {
	s.SystemInfo = NewSystemInfoWithID(s.ID)
}

func WithData(data *ShipmentData) func(s *Shipment) {
	return func(s *Shipment) {
		s.Data = data
	}
}

func WithStatus(status Status) func(s *Shipment) {
	return func(s *Shipment) {
		s.SystemInfo.Status = status
	}
}

func (s *Shipment) Update(opts ...func(s *Shipment)) {
	for _, opt := range opts {
		opt(s)
	}
	s.SystemInfo.LastUpdatedTimestamp = formattedCurrentTime()
	s.SystemInfo.Version = s.SystemInfo.Version + 1
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

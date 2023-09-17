package model

type Shipment struct {
	ID         string        `json:"id" dynamodbav:"id"`
	Data       *ShipmentData `json:"data" dynamodbav:"data"`
	SystemInfo *SystemInfo   `json:"system_info" dynamodbav:"system_info"`

	Queued               int    `json:"queued" dynamodbav:"queued,omitempty"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp,omitempty"`
	DLQ                  int    `json:"DLQ" dynamodbav:"DLQ,omitempty"`
}

func NewShipmentWithID(id string) *Shipment {
	if id == "" {
		panic("Shipment ID cannot be null!")
	}
	return &Shipment{
		ID:         id,
		SystemInfo: NewSystemInfoWithID(id),
		Data:       NewShipmentDataWithID(id),
	}
}

func (s *Shipment) MarkAsReadyForShipment() {
	s.SystemInfo.Status = StatusReadyToShip
}

func (s *Shipment) ResetSystemInfo() {
	s.SystemInfo = NewSystemInfoWithID(s.ID)
}

type ShipmentData struct {
	ID    string         `json:"id" dynamodbav:"id"`
	Items []ShipmentItem `json:"items" dynamodbav:"items"`
	Data1 string         `json:"data_element_1" dynamodbav:"data_1"`
	Data2 string         `json:"data_element_2" dynamodbav:"data_2"`
	Data3 string         `json:"data_element_3" dynamodbav:"data_3"`
}

func NewShipmentDataWithID(id string) *ShipmentData {
	return &ShipmentData{
		ID:    id,
		Items: make([]ShipmentItem, 0),
	}
}

type ShipmentItem struct {
	SKU    string `json:"SKU" dynamodbav:"SKU"`
	Packed bool   `json:"is_packed" dynamodbav:"is_packed"`
}

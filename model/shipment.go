package model

type Shipment struct {
	ID         string        `json:"id" dynamodbav:"id"`
	Data       *ShipmentData `json:"data" dynamodbav:"data"`
	SystemInfo *SystemInfo   `json:"system_info" dynamodbav:"system_info"`

	LastUpdatedTimestamp string `json:"last_updated_timestamp" dynamodbav:"last_updated_timestamp"`
	DLQ                  int    `json:"DLQ" dynamodbav:"DLQ"`
}

func NewShipment() *Shipment {
	return &Shipment{
		SystemInfo: NewSystemInfo(),
		Data:       NewShipmentData(),
	}
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

func (s *Shipment) SetID(id string) {
	if id == "" {
		panic("Shipment ID cannot be null!")
	}

	s.ID = id
	s.SystemInfo.ID = id
	s.Data.ID = id
}

func (s *Shipment) MarkAsPartiallyConstructed() {
	s.SystemInfo.Status = StatusEnumUnderConstruction
}

func (s *Shipment) MarkAsReadyForShipment() {
	s.SystemInfo.Status = StatusEnumReadyToShip
}

func (s *Shipment) IsQueued() bool {
	return s.SystemInfo.InQueue
}

func (s *Shipment) GetLastUpdatedTimestamp() string {
	return s.SystemInfo.LastUpdatedTimestamp
}

func (s *Shipment) SetLastUpdatedTimestamp(timestamp string) {
	s.SystemInfo.LastUpdatedTimestamp = timestamp
}

func (s *Shipment) ResetSystemInfo() {
	s.SystemInfo = NewSystemInfoWithID(s.ID)
}

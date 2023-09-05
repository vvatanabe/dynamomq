package model

type StatusEnum string

const (
	StatusEnumNone               StatusEnum = "NONE"
	StatusEnumUnderConstruction  StatusEnum = "UNDER_CONSTRUCTION"
	StatusEnumReadyToShip        StatusEnum = "READY_TO_SHIP"
	StatusEnumProcessingShipment StatusEnum = "PROCESSING_SHIPMENT"
	StatusEnumCompleted          StatusEnum = "COMPLETED"
	StatusEnumInDLQ              StatusEnum = "IN_DLQ"
)

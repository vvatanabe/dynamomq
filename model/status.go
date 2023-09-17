package model

type Status string

const (
	StatusNone               Status = "NONE"
	StatusUnderConstruction  Status = "UNDER_CONSTRUCTION"
	StatusReadyToShip        Status = "READY_TO_SHIP"
	StatusProcessingShipment Status = "PROCESSING_SHIPMENT"
	StatusCompleted          Status = "COMPLETED"
	StatusInDLQ              Status = "IN_DLQ"
)

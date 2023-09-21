package model

type Result struct {
	ID                   string `json:"id"`
	Status               Status `json:"status"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp"`
	Version              int    `json:"version"`
}

type DequeueResult struct {
	*Result
	DequeuedShipmentObject *Shipment `json:"-"`
}

// EnqueueResult represents the result for the enqueue() API call.
type EnqueueResult struct {
	*Result            // Embedded type for inheritance-like behavior in Go
	Shipment *Shipment `json:"-"`
}

// PeekResult represents the result for the peek() API call.
type PeekResult struct {
	*Result                        // Embedded type for inheritance-like behavior in Go
	TimestampMillisUTC   int64     `json:"timestamp_milliseconds_utc"`
	PeekedShipmentObject *Shipment `json:"-"`
}

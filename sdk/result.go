package sdk

type Result struct {
	ID                   string `json:"id"`
	Status               Status `json:"status"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp"`
	Version              int    `json:"version"`
}

type DequeueResult[T any] struct {
	*Result
	DequeuedShipmentObject *Shipment[T] `json:"-"`
}

// EnqueueResult represents the result for the enqueue() API call.
type EnqueueResult[T any] struct {
	*Result               // Embedded type for inheritance-like behavior in Go
	Shipment *Shipment[T] `json:"-"`
}

// PeekResult represents the result for the peek() API call.
type PeekResult[T any] struct {
	*Result                           // Embedded type for inheritance-like behavior in Go
	TimestampMillisUTC   int64        `json:"timestamp_milliseconds_utc"`
	PeekedShipmentObject *Shipment[T] `json:"-"`
}

package model

// Utils function for null check (assuming it checks if the object is not null)
func checkIfNotNullObject(obj interface{}) bool {
	return obj != nil
}

// PeekResult represents the result for the peek() API call.
type PeekResult struct {
	*ReturnResult                  // Embedded type for inheritance-like behavior in Go
	TimestampMillisUTC   int64     `json:"timestamp_milliseconds_utc"`
	PeekedShipmentObject *Shipment `json:"-"`
}

// NewPeekResult constructs a new PeekResult with the default values.
func NewPeekResult() *PeekResult {
	return &PeekResult{
		ReturnResult: &ReturnResult{},
	}
}

// NewPeekResultWithID constructs a new PeekResult with the provided ID.
func NewPeekResultWithID(id string) *PeekResult {
	return &PeekResult{
		ReturnResult: &ReturnResult{
			ID: id,
		},
	}
}

// GetPeekedShipmentID returns the ID of the peeked shipment, or "NOT FOUND" if it's null.
func (pr *PeekResult) GetPeekedShipmentID() string {
	if checkIfNotNullObject(pr.PeekedShipmentObject) {
		return pr.PeekedShipmentObject.ID
	}
	return "NOT FOUND"
}

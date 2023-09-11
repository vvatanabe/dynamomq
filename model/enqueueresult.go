package model

// EnqueueResult represents the result for the enqueue() API call.
type EnqueueResult struct {
	*ReturnResult           // Embedded type for inheritance-like behavior in Go
	Shipment      *Shipment `json:"-"`
}

// NewEnqueueResult constructs a new EnqueueResult with the default values.
func NewEnqueueResult() *EnqueueResult {
	return &EnqueueResult{}
}

// NewEnqueueResultWithID constructs a new EnqueueResult with the provided ID.
func NewEnqueueResultWithID(id string) *EnqueueResult {
	return &EnqueueResult{
		ReturnResult: &ReturnResult{
			ID: id,
		},
	}
}

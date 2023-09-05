package model

import "github.com/vvatanabe/go82f46979/appdata"

// EnqueueResult represents the result for the enqueue() API call.
type EnqueueResult struct {
	*ReturnResult                   // Embedded type for inheritance-like behavior in Go
	Shipment      *appdata.Shipment `json:"-"`
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

package model

type Result struct {
	ID                   string       `json:"id"`
	ReturnValue          ReturnStatus `json:"return_value"`
	Status               Status       `json:"status"`
	LastUpdatedTimestamp string       `json:"last_updated_timestamp"`
	Version              int          `json:"version"`
}

func NewReturnResultWithID(id string) *Result {
	return &Result{
		ID:          id,
		ReturnValue: ReturnStatusNone,
		Status:      StatusNone,
	}
}

func (rr *Result) IsSuccessful() bool {
	return rr.ReturnValue == ReturnStatusSUCCESS
}

func (rr *Result) GetErrorMessage() string {
	return rr.ReturnValue.GetErrorMessage()
}

// ReturnStatus defines the statuses for queue operations.
type ReturnStatus int

const (
	ReturnStatusNone ReturnStatus = iota
	ReturnStatusSUCCESS
	ReturnStatusFailedIDNotProvided
	ReturnStatusFailedIDNotFound
	ReturnStatusFailedRecordNotConstructed
	ReturnStatusFailedOnCondition
	ReturnStatusFailedEmptyQueue
	ReturnStatusFailedIllegalState
	ReturnStatusFailedDynamoError
)

// String maps ReturnStatus values to their string representations.
func (e ReturnStatus) String() string {
	switch e {
	case ReturnStatusNone:
		return "NONE"
	case ReturnStatusSUCCESS:
		return "SUCCESS"
	case ReturnStatusFailedIDNotProvided:
		return "FAILED_ID_NOT_PROVIDED"
	case ReturnStatusFailedIDNotFound:
		return "FAILED_ID_NOT_FOUND"
	case ReturnStatusFailedRecordNotConstructed:
		return "FAILED_RECORD_NOT_CONSTRUCTED"
	case ReturnStatusFailedOnCondition:
		return "FAILED_ON_CONDITION"
	case ReturnStatusFailedEmptyQueue:
		return "FAILED_EMPTY_QUEUE"
	case ReturnStatusFailedIllegalState:
		return "FAILED_ILLEGAL_STATE"
	case ReturnStatusFailedDynamoError:
		return "FAILED_DYNAMO_ERROR"
	default:
		return "UNKNOWN"
	}
}

// GetErrorMessage returns the associated error message for the ReturnStatus value.
func (e ReturnStatus) GetErrorMessage() string {
	switch e {
	case ReturnStatusNone:
		return "No error"
	case ReturnStatusFailedIDNotProvided:
		return "ID was not provided!"
	case ReturnStatusFailedIDNotFound:
		return "Provided Shipment ID was not found in the Dynamo DB!"
	case ReturnStatusFailedRecordNotConstructed:
		return "Shipment record not yet fully constructed .. cannot execute API!"
	case ReturnStatusFailedOnCondition:
		return "Condition on the 'version' attribute has failed!"
	case ReturnStatusFailedEmptyQueue:
		return "Cannot proceed, queue is empty!"
	case ReturnStatusFailedIllegalState:
		return "Illegal state, cannot proceed!"
	case ReturnStatusFailedDynamoError:
		return "Unspecified DynamoDB error is encountered!"
	default:
		return "API was not called!"
	}
}

type DequeueResult struct {
	*Result
	DequeuedShipmentObject *Shipment `json:"-"`
}

func NewDequeueResultFromReturnResult(result *Result) *DequeueResult {
	return &DequeueResult{
		Result: result,
	}
}

// EnqueueResult represents the result for the enqueue() API call.
type EnqueueResult struct {
	*Result            // Embedded type for inheritance-like behavior in Go
	Shipment *Shipment `json:"-"`
}

// NewEnqueueResultWithID constructs a new EnqueueResult with the provided ID.
func NewEnqueueResultWithID(id string) *EnqueueResult {
	return &EnqueueResult{
		Result: &Result{
			ID: id,
		},
	}
}

// PeekResult represents the result for the peek() API call.
type PeekResult struct {
	*Result                        // Embedded type for inheritance-like behavior in Go
	TimestampMillisUTC   int64     `json:"timestamp_milliseconds_utc"`
	PeekedShipmentObject *Shipment `json:"-"`
}

// NewPeekResult constructs a new PeekResult with the default values.
func NewPeekResult() *PeekResult {
	return &PeekResult{
		Result: &Result{},
	}
}

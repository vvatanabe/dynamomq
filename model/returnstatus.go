package model

// ReturnStatusEnum defines the statuses for queue operations.
type ReturnStatusEnum int

const (
	ReturnStatusEnumNone ReturnStatusEnum = iota
	ReturnStatusEnumSUCCESS
	ReturnStatusEnumFailedIDNotProvided
	ReturnStatusEnumFailedIDNotFound
	ReturnStatusEnumFailedRecordNotConstructed
	ReturnStatusEnumFailedOnCondition
	ReturnStatusEnumFailedEmptyQueue
	ReturnStatusEnumFailedIllegalState
	ReturnStatusEnumFailedDynamoError
)

// String maps ReturnStatusEnum values to their string representations.
func (e ReturnStatusEnum) String() string {
	switch e {
	case ReturnStatusEnumNone:
		return "NONE"
	case ReturnStatusEnumSUCCESS:
		return "SUCCESS"
	case ReturnStatusEnumFailedIDNotProvided:
		return "FAILED_ID_NOT_PROVIDED"
	case ReturnStatusEnumFailedIDNotFound:
		return "FAILED_ID_NOT_FOUND"
	case ReturnStatusEnumFailedRecordNotConstructed:
		return "FAILED_RECORD_NOT_CONSTRUCTED"
	case ReturnStatusEnumFailedOnCondition:
		return "FAILED_ON_CONDITION"
	case ReturnStatusEnumFailedEmptyQueue:
		return "FAILED_EMPTY_QUEUE"
	case ReturnStatusEnumFailedIllegalState:
		return "FAILED_ILLEGAL_STATE"
	case ReturnStatusEnumFailedDynamoError:
		return "FAILED_DYNAMO_ERROR"
	default:
		return "UNKNOWN"
	}
}

// GetErrorMessage returns the associated error message for the ReturnStatusEnum value.
func (e ReturnStatusEnum) GetErrorMessage() string {
	switch e {
	case ReturnStatusEnumNone:
		return "No error"
	case ReturnStatusEnumFailedIDNotProvided:
		return "ID was not provided!"
	case ReturnStatusEnumFailedIDNotFound:
		return "Provided Shipment ID was not found in the Dynamo DB!"
	case ReturnStatusEnumFailedRecordNotConstructed:
		return "Shipment record not yet fully constructed .. cannot execute API!"
	case ReturnStatusEnumFailedOnCondition:
		return "Condition on the 'version' attribute has failed!"
	case ReturnStatusEnumFailedEmptyQueue:
		return "Cannot proceed, queue is empty!"
	case ReturnStatusEnumFailedIllegalState:
		return "Illegal state, cannot proceed!"
	case ReturnStatusEnumFailedDynamoError:
		return "Unspecified DynamoDB error is encountered!"
	default:
		return "API was not called!"
	}
}

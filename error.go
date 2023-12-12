package dynamomq

import "fmt"

// IDNotProvidedError represents an error when an ID is not provided where it is required.
type IDNotProvidedError struct{}

// Error returns a standard error message for IDNotProvidedError.
func (e IDNotProvidedError) Error() string {
	return "ID was not provided."
}

// IDNotFoundError represents an error when a provided ID is not found in DynamoDB.
type IDNotFoundError struct{}

// Error returns a standard error message for IDNotFoundError.
func (e IDNotFoundError) Error() string {
	return "Provided ID was not found in the Dynamo DB."
}

// IDDuplicatedError represents an error when a provided ID is duplicated in the system.
type IDDuplicatedError struct{}

// Error returns a standard error message for IDDuplicatedError.
func (e IDDuplicatedError) Error() string {
	return "Provided ID was duplicated."
}

// ConditionalCheckFailedError represents an error when a condition check on the 'version' attribute fails.
type ConditionalCheckFailedError struct {
	Cause error
}

// Error returns a detailed error message including the underlying cause for ConditionalCheckFailedError.
func (e ConditionalCheckFailedError) Error() string {
	return fmt.Sprintf("Condition on the 'version' attribute has failed: %v.", e.Cause)
}

// BuildingExpressionError represents an error during the building of a DynamoDB expression.
type BuildingExpressionError struct {
	Cause error
}

// Error returns a detailed error message including the underlying cause for BuildingExpressionError.
func (e BuildingExpressionError) Error() string {
	return fmt.Sprintf("Failed to build expression: %v.", e.Cause)
}

// DynamoDBAPIError represents a generic error encountered when making a DynamoDB API call.
type DynamoDBAPIError struct {
	Cause error
}

// Error returns a detailed error message including the underlying cause for DynamoDBAPIError.
func (e DynamoDBAPIError) Error() string {
	return fmt.Sprintf("Failed DynamoDB API: %v.", e.Cause)
}

// UnmarshalingAttributeError represents an error during the unmarshaling of DynamoDB attributes.
type UnmarshalingAttributeError struct {
	Cause error
}

// Error returns a detailed error message including the underlying cause for UnmarshalingAttributeError.
func (e UnmarshalingAttributeError) Error() string {
	return fmt.Sprintf("Failed to unmarshal: %v.", e.Cause)
}

// MarshalingAttributeError represents an error during the marshaling of DynamoDB attributes.
type MarshalingAttributeError struct {
	Cause error
}

// Error returns a detailed error message including the underlying cause for MarshalingAttributeError.
func (e MarshalingAttributeError) Error() string {
	return fmt.Sprintf("Failed to marshal: %v.", e.Cause)
}

// EmptyQueueError represents an error when an operation cannot proceed due to an empty queue.
type EmptyQueueError struct{}

// Error returns a standard error message for EmptyQueueError.
func (e EmptyQueueError) Error() string {
	return "Cannot proceed, queue is empty."
}

// InvalidStateTransitionError represents an error for invalid state transitions during operations.
type InvalidStateTransitionError struct {
	Msg       string
	Operation string
	Current   Status
}

// Error returns a detailed error message explaining the invalid state transition.
func (e InvalidStateTransitionError) Error() string {
	return fmt.Sprintf("operation %s failed for status %s: %s.", e.Operation, e.Current, e.Msg)
}

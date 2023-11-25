package dynamomq

import "fmt"

type IDNotProvidedError struct{}

func (e IDNotProvidedError) Error() string {
	return "ID was not provided."
}

type IDNotFoundError struct{}

func (e IDNotFoundError) Error() string {
	return "Provided ID was not found in the Dynamo DB."
}

type IDDuplicatedError struct{}

func (e IDDuplicatedError) Error() string {
	return fmt.Sprintf("Provided ID was duplicated.")
}

type ConditionalCheckFailedError struct {
	Cause error
}

func (e ConditionalCheckFailedError) Error() string {
	return fmt.Sprintf("Condition on the 'version' attribute has failed: %v.", e.Cause)
}

type BuildingExpressionError struct {
	Cause error
}

func (e BuildingExpressionError) Error() string {
	return fmt.Sprintf("Failed to build expression: %v.", e.Cause)
}

type DynamoDBAPIError struct {
	Cause error
}

func (e DynamoDBAPIError) Error() string {
	return fmt.Sprintf("Failed DynamoDB API: %v.", e.Cause)
}

type UnmarshalingAttributeError struct {
	Cause error
}

func (e UnmarshalingAttributeError) Error() string {
	return fmt.Sprintf("Failed to unmarshal: %v.", e.Cause)
}

type MarshalingAttributeError struct {
	Cause error
}

func (e MarshalingAttributeError) Error() string {
	return fmt.Sprintf("Failed to marshal: %v.", e.Cause)
}

type EmptyQueueError struct{}

func (e EmptyQueueError) Error() string {
	return "Cannot proceed, queue is empty."
}

type InvalidStateTransitionError struct {
	Msg       string
	Operation string
	Current   Status
}

func (e InvalidStateTransitionError) Error() string {
	return fmt.Sprintf("operation %s failed for status %s: %s.", e.Operation, e.Current, e.Msg)
}

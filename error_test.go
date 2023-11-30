package dynamomq_test

import (
	"errors"
	"testing"

	"github.com/vvatanabe/dynamomq"
)

func TestErrors(t *testing.T) {
	type testCase struct {
		err      error
		expected string
	}
	tests := []testCase{
		{dynamomq.IDNotProvidedError{}, "ID was not provided."},
		{dynamomq.IDNotFoundError{}, "Provided ID was not found in the Dynamo DB."},
		{dynamomq.IDDuplicatedError{}, "Provided ID was duplicated."},
		{dynamomq.ConditionalCheckFailedError{Cause: errors.New("sample cause")}, "Condition on the 'version' attribute has failed: sample cause."},
		{dynamomq.BuildingExpressionError{Cause: errors.New("sample cause")}, "Failed to build expression: sample cause."},
		{dynamomq.DynamoDBAPIError{Cause: errors.New("sample cause")}, "Failed DynamoDB API: sample cause."},
		{dynamomq.UnmarshalingAttributeError{Cause: errors.New("sample cause")}, "Failed to unmarshal: sample cause."},
		{dynamomq.MarshalingAttributeError{Cause: errors.New("sample cause")}, "Failed to marshal: sample cause."},
		{dynamomq.EmptyQueueError{}, "Cannot proceed, queue is empty."},
		{dynamomq.InvalidStateTransitionError{Msg: "sample message", Operation: "sample operation", Current: "sample status"}, "operation sample operation failed for status sample status: sample message."},
	}
	for _, tc := range tests {
		if tc.err.Error() != tc.expected {
			t.Errorf("Unexpected error message. Expected: %v, got: %v", tc.expected, tc.err.Error())
		}
	}
}

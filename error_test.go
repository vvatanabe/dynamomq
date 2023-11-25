package dynamomq_test

import (
	"errors"
	"testing"

	. "github.com/vvatanabe/dynamomq"
)

func TestErrors(t *testing.T) {
	type testCase struct {
		err      error
		expected string
	}
	tests := []testCase{
		{IDNotProvidedError{}, "ID was not provided."},
		{IDNotFoundError{}, "Provided ID was not found in the Dynamo DB."},
		{IDDuplicatedError{}, "Provided ID was duplicated."},
		{ConditionalCheckFailedError{Cause: errors.New("sample cause")}, "Condition on the 'version' attribute has failed: sample cause."},
		{BuildingExpressionError{Cause: errors.New("sample cause")}, "Failed to build expression: sample cause."},
		{DynamoDBAPIError{Cause: errors.New("sample cause")}, "Failed DynamoDB API: sample cause."},
		{UnmarshalingAttributeError{Cause: errors.New("sample cause")}, "Failed to unmarshal: sample cause."},
		{MarshalingAttributeError{Cause: errors.New("sample cause")}, "Failed to marshal: sample cause."},
		{EmptyQueueError{}, "Cannot proceed, queue is empty."},
		{InvalidStateTransitionError{Msg: "sample message", Operation: "sample operation", Current: "sample status"}, "operation sample operation failed for status sample status: sample message."},
	}
	for _, tc := range tests {
		if tc.err.Error() != tc.expected {
			t.Errorf("Unexpected error message. Expected: %v, got: %v", tc.expected, tc.err.Error())
		}
	}

}

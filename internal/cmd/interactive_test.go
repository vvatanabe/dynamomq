package cmd_test

import (
	"context"
	"testing"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/cmd"
	"github.com/vvatanabe/dynamomq/internal/mock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func testRunInteractiveAll(t *testing.T, client dynamomq.Client[any], wantErr bool) {
	defaultTestMessage := dynamomq.NewMessage[any]("A-101", test.NewMessageData("A-101"), clock.Now())
	tests := []struct {
		name    string
		command string
		params  []string
		message *dynamomq.Message[any]
	}{
		{
			name:    "run ls",
			command: "ls",
		},
		{
			name:    "run purge",
			command: "purge",
		},
		{
			name:    "run enqueue-test",
			command: "enqueue-test",
		},
		{
			name:    "run qstat",
			command: "qstat",
		},
		{
			name:    "run dlq",
			command: "dlq",
		},
		{
			name:    "run receive",
			command: "receive",
		},
		{
			name:    "run id",
			command: "id",
			params:  []string{"A-101"},
		},
		{
			name:    "run reset",
			command: "reset",
			message: defaultTestMessage,
		},
		{
			name:    "run redrive",
			command: "redrive",
			message: defaultTestMessage,
		},
		{
			name:    "run delete",
			command: "delete",
			message: defaultTestMessage,
		},
		{
			name:    "run fail",
			command: "fail",
			message: defaultTestMessage,
		},
		{
			name:    "run invalid",
			command: "invalid",
			message: defaultTestMessage,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cmd.Interactive{
				Client:  client,
				Message: tt.message,
			}
			err := c.Run(context.Background(), tt.command, tt.params)
			if wantErr {
				if err == nil {
					t.Errorf("Run() error = %v, wantErr %v", err, wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		})
	}
}

func TestRunInteractiveAllShouldDynamoMQClientError(t *testing.T) {
	testRunInteractiveAll(t, &mock.Client[any]{}, true)
}

func TestRunInteractiveAllShouldDynamoMQClientSucceed(t *testing.T) {
	testRunInteractiveAll(t, successfulMockClient, false)
}

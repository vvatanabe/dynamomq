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
					t.Error("Run() error is not nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		})
	}
}

func TestRunInteractiveAllShouldReturnError(t *testing.T) {
	testRunInteractiveAll(t, &mock.Client[any]{}, true)
}

func TestRunInteractiveAllShouldSucceed(t *testing.T) {
	testRunInteractiveAll(t, successfulMockClient, false)
}

func TestRunInteractiveSelectedMessageID(t *testing.T) {
	defaultTestMessage := dynamomq.NewMessage[any]("A-101", test.NewMessageData("A-101"), clock.Now())
	tests := []struct {
		name    string
		command string
		message *dynamomq.Message[any]
		wantErr bool
	}{
		{
			name:    "run info",
			command: "info",
			message: defaultTestMessage,
		},
		{
			name:    "run info should return error when message is nil",
			command: "info",
			wantErr: true,
		},
		{
			name:    "run data",
			command: "data",
			message: defaultTestMessage,
		},
		{
			name:    "run data should return error when message is nil",
			command: "data",
			wantErr: true,
		},
		{
			name:    "run system",
			command: "system",
			message: defaultTestMessage,
		},
		{
			name:    "run system should return error when message is nil",
			command: "system",
			wantErr: true,
		},
		{
			name:    "run reset should return error when message is nil",
			command: "reset",
			wantErr: true,
		},
		{
			name:    "run redrive should return error when message is nil",
			command: "redrive",
			wantErr: true,
		},
		{
			name:    "run delete should return error when message is nil",
			command: "delete",
			wantErr: true,
		},
		{
			name:    "run fail should return error when message is nil",
			command: "fail",
			wantErr: true,
		},
		{
			name:    "run invalid should return error when message is nil",
			command: "invalid",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cmd.Interactive{
				Client:  successfulMockClient,
				Message: tt.message,
			}
			err := c.Run(context.Background(), tt.command, []string{})
			if tt.wantErr {
				if err == nil {
					t.Error("Run() error should not nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		})
	}
}

func TestRunInteractiveHelp(t *testing.T) {
	c := &cmd.Interactive{}
	if err := c.Run(context.Background(), "help", nil); err != nil {
		t.Errorf("Run() error = %v", err)
	}
}

func TestRunInteractiveUnrecognizedCommand(t *testing.T) {
	c := &cmd.Interactive{}
	if err := c.Run(context.Background(), "foo", nil); err == nil {
		t.Error("Run() error is not nil")
	}
}
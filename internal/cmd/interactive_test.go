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
	testRunInteractiveAll(t, mock.SuccessfulMockClient, false)
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
				Client:  mock.SuccessfulMockClient,
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

var defaultTestMessages = []*dynamomq.Message[any]{
	dynamomq.NewMessage[any]("A-101", test.NewMessageData("A-101"), clock.Now()),
	dynamomq.NewMessage[any]("A-102", test.NewMessageData("A-102"), clock.Now()),
	dynamomq.NewMessage[any]("A-103", test.NewMessageData("A-103"), clock.Now()),
}

func TestRunInteractiveLS(t *testing.T) {
	c := &cmd.Interactive{
		Client: mock.Client[any]{
			ListMessagesFunc: func(ctx context.Context, params *dynamomq.ListMessagesInput) (*dynamomq.ListMessagesOutput[any], error) {
				return &dynamomq.ListMessagesOutput[any]{
					Messages: defaultTestMessages,
				}, nil
			},
		},
	}
	if err := c.Run(context.Background(), "ls", nil); err != nil {
		t.Errorf("Run() error = %v", err)
	}
}

func TestRunInteractivePurge(t *testing.T) {
	tests := []struct {
		name    string
		client  dynamomq.Client[any]
		wantErr bool
	}{
		{
			name: "should purge messages successfully",
			client: mock.Client[any]{
				ListMessagesFunc: func(ctx context.Context, params *dynamomq.ListMessagesInput) (*dynamomq.ListMessagesOutput[any], error) {
					return &dynamomq.ListMessagesOutput[any]{
						Messages: defaultTestMessages,
					}, nil
				},
				DeleteMessageFunc: func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
					return &dynamomq.DeleteMessageOutput{}, nil
				},
			},
			wantErr: false,
		},
		{
			name: "should purge messages failed",
			client: mock.Client[any]{
				ListMessagesFunc: func(ctx context.Context, params *dynamomq.ListMessagesInput) (*dynamomq.ListMessagesOutput[any], error) {
					return &dynamomq.ListMessagesOutput[any]{
						Messages: defaultTestMessages,
					}, nil
				},
				DeleteMessageFunc: func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
					return &dynamomq.DeleteMessageOutput{}, test.ErrorTest
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cmd.Interactive{
				Client: tt.client,
			}
			err := c.Run(context.Background(), "purge", nil)
			if tt.wantErr {
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

func TestRunInteractiveEnqueueTestShouldReturnError(t *testing.T) {
	c := &cmd.Interactive{
		Client: mock.Client[any]{
			DeleteMessageFunc: func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
				return &dynamomq.DeleteMessageOutput{}, nil
			},
			SendMessageFunc: func(ctx context.Context, params *dynamomq.SendMessageInput[any]) (*dynamomq.SendMessageOutput[any], error) {
				return &dynamomq.SendMessageOutput[any]{}, test.ErrorTest
			},
		},
	}
	if err := c.Run(context.Background(), "enqueue-test", nil); err == nil {
		t.Error("Run() error is not nil")
	}
}

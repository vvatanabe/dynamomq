package cmd_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/cmd"
	"github.com/vvatanabe/dynamomq/internal/mock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func testRunAllCommand(t *testing.T, f cmd.CommandFactory, wantErr error) {
	type testCase struct {
		name string
		cmd  *cobra.Command
	}
	tests := []testCase{
		{
			name: "delete command",
			cmd:  f.CreateDeleteCommand(&cmd.Flags{}),
		},
		{
			name: "dlq command",
			cmd:  f.CreateDLQCommand(&cmd.Flags{}),
		},
		{
			name: "enqueue test command",
			cmd:  f.CreateEnqueueTestCommand(&cmd.Flags{}),
		},
		{
			name: "fail command",
			cmd:  f.CreateFailCommand(&cmd.Flags{}),
		},
		{
			name: "get command",
			cmd:  f.CreateGetCommand(&cmd.Flags{}),
		},
		{
			name: "invalid command",
			cmd:  f.CreateInvalidCommand(&cmd.Flags{}),
		},
		{
			name: "ls command",
			cmd:  f.CreateLSCommand(&cmd.Flags{}),
		},
		{
			name: "purge command",
			cmd:  f.CreatePurgeCommand(&cmd.Flags{}),
		},
		{
			name: "qstat command",
			cmd:  f.CreateQueueStatCommand(&cmd.Flags{}),
		},
		{
			name: "receive command",
			cmd:  f.CreatReceiveCommand(&cmd.Flags{}),
		},
		{
			name: "redrive command",
			cmd:  f.CreateRedriveCommand(&cmd.Flags{}),
		},
		{
			name: "reset command",
			cmd:  f.CreateResetCommand(&cmd.Flags{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cmd.RunE(&cobra.Command{}, []string{}); !errors.Is(err, wantErr) {
				t.Errorf("RunE() error = %v, wantErr %v", err, wantErr)
			}
		})
	}
}

func TestRunAllCommandShouldSucceed(t *testing.T) {
	testRunAllCommand(t, cmd.CommandFactory{
		CreateDynamoMQClient: func(ctx context.Context, flags *cmd.Flags) (dynamomq.Client[any], aws.Config, error) {
			return &mock.Client[any]{
				SendMessageFunc: func(ctx context.Context, params *dynamomq.SendMessageInput[any]) (*dynamomq.SendMessageOutput[any], error) {
					return &dynamomq.SendMessageOutput[any]{
						Result:  &dynamomq.Result{},
						Message: &dynamomq.Message[any]{},
					}, nil
				},
				ReceiveMessageFunc: func(ctx context.Context, params *dynamomq.ReceiveMessageInput) (*dynamomq.ReceiveMessageOutput[any], error) {
					return &dynamomq.ReceiveMessageOutput[any]{
						Result:              &dynamomq.Result{},
						PeekedMessageObject: &dynamomq.Message[any]{},
					}, nil
				},
				UpdateMessageAsVisibleFunc: func(ctx context.Context, params *dynamomq.UpdateMessageAsVisibleInput) (*dynamomq.UpdateMessageAsVisibleOutput[any], error) {
					return &dynamomq.UpdateMessageAsVisibleOutput[any]{
						Result:  &dynamomq.Result{},
						Message: &dynamomq.Message[any]{},
					}, nil
				},
				DeleteMessageFunc: func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
					return &dynamomq.DeleteMessageOutput{}, nil
				},
				MoveMessageToDLQFunc: func(ctx context.Context, params *dynamomq.MoveMessageToDLQInput) (*dynamomq.MoveMessageToDLQOutput, error) {
					return &dynamomq.MoveMessageToDLQOutput{}, nil
				},
				RedriveMessageFunc: func(ctx context.Context, params *dynamomq.RedriveMessageInput) (*dynamomq.RedriveMessageOutput, error) {
					return &dynamomq.RedriveMessageOutput{}, nil
				},
				GetMessageFunc: func(ctx context.Context, params *dynamomq.GetMessageInput) (*dynamomq.GetMessageOutput[any], error) {
					return &dynamomq.GetMessageOutput[any]{
						Message: &dynamomq.Message[any]{},
					}, nil
				},
				GetQueueStatsFunc: func(ctx context.Context, params *dynamomq.GetQueueStatsInput) (*dynamomq.GetQueueStatsOutput, error) {
					return &dynamomq.GetQueueStatsOutput{}, nil
				},
				GetDLQStatsFunc: func(ctx context.Context, params *dynamomq.GetDLQStatsInput) (*dynamomq.GetDLQStatsOutput, error) {
					return &dynamomq.GetDLQStatsOutput{}, nil
				},
				ListMessagesFunc: func(ctx context.Context, params *dynamomq.ListMessagesInput) (*dynamomq.ListMessagesOutput[any], error) {
					return &dynamomq.ListMessagesOutput[any]{}, nil
				},
				ReplaceMessageFunc: func(ctx context.Context, params *dynamomq.ReplaceMessageInput[any]) (*dynamomq.ReplaceMessageOutput, error) {
					return &dynamomq.ReplaceMessageOutput{}, nil
				},
			}, aws.Config{}, nil
		},
	}, nil)
}

func TestRunAllCommandShouldReturnCommandFacrotyError(t *testing.T) {
	testRunAllCommand(t, cmd.CommandFactory{
		CreateDynamoMQClient: func(ctx context.Context, flags *cmd.Flags) (dynamomq.Client[any], aws.Config, error) {
			return nil, aws.Config{}, test.ErrorTest
		},
	}, test.ErrorTest)
}

func TestRunAllCommandShouldDynamoMQClientError(t *testing.T) {
	testRunAllCommand(t, cmd.CommandFactory{
		CreateDynamoMQClient: func(ctx context.Context, flags *cmd.Flags) (dynamomq.Client[any], aws.Config, error) {
			return &mock.Client[any]{}, aws.Config{}, nil
		},
	}, mock.ErrNotImplemented)
}

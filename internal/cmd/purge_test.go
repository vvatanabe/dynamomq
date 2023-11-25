package cmd_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/cmd"
	"github.com/vvatanabe/dynamomq/internal/mock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func TestPurgeCommand(t *testing.T) {
	type fields struct {
		CreateDynamoMQClient func(ctx context.Context, flags *cmd.Flags) (dynamomq.Client[any], aws.Config, error)
	}
	type args struct {
		flgs *cmd.Flags
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "should purge messages",
			fields: fields{
				CreateDynamoMQClient: func(ctx context.Context, flags *cmd.Flags) (dynamomq.Client[any], aws.Config, error) {
					return mock.Client[any]{
						ListMessagesFunc: func(ctx context.Context, params *dynamomq.ListMessagesInput) (*dynamomq.ListMessagesOutput[any], error) {
							return &dynamomq.ListMessagesOutput[any]{
								Messages: []*dynamomq.Message[any]{
									dynamomq.NewMessage[any]("A-101", nil, clock.Now()),
									dynamomq.NewMessage[any]("A-102", nil, clock.Now()),
									dynamomq.NewMessage[any]("A-103", nil, clock.Now()),
								},
							}, nil
						},
						DeleteMessageFunc: func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
							if params.ID == "A-101" {
								return nil, test.ErrorTest
							}
							return &dynamomq.DeleteMessageOutput{}, nil
						},
					}, aws.Config{}, nil
				},
			},
			args: args{
				flgs: &cmd.Flags{
					ID: "A-101",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := cmd.CommandFactory{
				CreateDynamoMQClient: tt.fields.CreateDynamoMQClient,
			}
			c := f.CreatePurgeCommand(tt.args.flgs)
			if err := c.RunE(&cobra.Command{}, []string{}); (err != nil) != tt.wantErr {
				t.Errorf("Purge() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

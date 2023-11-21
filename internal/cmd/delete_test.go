package cmd_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/cmd"
	"github.com/vvatanabe/dynamomq/internal/mock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func TestCommandFactoryCreatDeleteCommand(t *testing.T) {
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
			name: "should success to delete a message",
			fields: fields{
				CreateDynamoMQClient: func(ctx context.Context, flags *cmd.Flags) (dynamomq.Client[any], aws.Config, error) {
					return mock.Client[any]{
						DeleteMessageFunc: func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
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
		{
			name: "should return error when DynamoMQClient return error",
			fields: fields{
				CreateDynamoMQClient: func(ctx context.Context, flags *cmd.Flags) (dynamomq.Client[any], aws.Config, error) {
					return mock.Client[any]{
						DeleteMessageFunc: func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
							return nil, test.ErrorTest
						},
					}, aws.Config{}, nil
				},
			},
			args: args{
				flgs: &cmd.Flags{
					ID: "A-101",
				},
			},
			wantErr: true,
		},
		{
			name: "should return error when CreateDynamoMQClient func return error",
			fields: fields{
				CreateDynamoMQClient: func(ctx context.Context, flags *cmd.Flags) (dynamomq.Client[any], aws.Config, error) {
					return nil, aws.Config{}, test.ErrorTest
				},
			},
			args: args{
				flgs: &cmd.Flags{
					ID: "A-101",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := cmd.CommandFactory{
				CreateDynamoMQClient: tt.fields.CreateDynamoMQClient,
			}
			c := f.CreateDeleteCommand(tt.args.flgs)
			if err := c.RunE(&cobra.Command{}, []string{}); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

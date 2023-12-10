package dynamomq_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/mock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func TestProducerProduce(t *testing.T) {
	type args[T any] struct {
		params *dynamomq.ProduceInput[T]
	}
	type testCase[T any] struct {
		name    string
		c       *dynamomq.Producer[T]
		args    args[T]
		want    *dynamomq.ProduceOutput[T]
		wantErr bool
	}
	defaultMockClient := &mock.Client[test.MessageData]{
		SendMessageFunc: func(ctx context.Context,
			params *dynamomq.SendMessageInput[test.MessageData]) (*dynamomq.SendMessageOutput[test.MessageData], error) {
			return &dynamomq.SendMessageOutput[test.MessageData]{
				SentMessage: &dynamomq.Message[test.MessageData]{
					ID:   params.ID,
					Data: params.Data,
				},
			}, nil
		},
	}
	defaultTestProducer := dynamomq.NewProducer[test.MessageData](defaultMockClient, dynamomq.WithIDGenerator(func() string {
		return "A-101"
	}))
	tests := []testCase[test.MessageData]{
		{
			name: "should success to produce a message",
			c:    defaultTestProducer,
			args: args[test.MessageData]{
				params: &dynamomq.ProduceInput[test.MessageData]{
					Data:         test.NewMessageData("A-101"),
					DelaySeconds: 10,
				},
			},
			want: &dynamomq.ProduceOutput[test.MessageData]{
				Message: &dynamomq.Message[test.MessageData]{
					ID:   "A-101",
					Data: test.NewMessageData("A-101"),
				},
			},
			wantErr: false,
		},
		{
			name: "should success to produce a message when params is nil",
			c:    defaultTestProducer,
			args: args[test.MessageData]{
				params: nil,
			},
			want: &dynamomq.ProduceOutput[test.MessageData]{
				Message: &dynamomq.Message[test.MessageData]{
					ID: "A-101",
				},
			},
			wantErr: false,
		},
		{
			name: "should fail to produce a message when client.SendMessage returns error",
			c: dynamomq.NewProducer[test.MessageData](&mock.Client[test.MessageData]{
				SendMessageFunc: func(ctx context.Context,
					params *dynamomq.SendMessageInput[test.MessageData]) (*dynamomq.SendMessageOutput[test.MessageData], error) {
					return nil, errors.New("for error case")
				},
			}),
			args: args[test.MessageData]{
				params: nil,
			},
			want:    &dynamomq.ProduceOutput[test.MessageData]{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Produce(context.Background(), tt.args.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("Produce() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Produce() got = %v, want %v", got, tt.want)
			}
		})
	}
}

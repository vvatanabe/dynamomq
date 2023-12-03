package mock_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/mock"
)

func TestMockClient(t *testing.T) {
	ctx := context.Background()
	notImplementedClient := &mock.Client[any]{}
	tests := []struct {
		name   string
		method func(client *mock.Client[any]) (any, error)
	}{
		{
			name: "SendMessage",
			method: func(client *mock.Client[any]) (any, error) {
				return client.SendMessage(ctx, nil)
			},
		},
		{
			name: "ReceiveMessage",
			method: func(client *mock.Client[any]) (any, error) {
				return client.ReceiveMessage(ctx, nil)
			},
		},
		{
			name: "ChangeMessageVisibility",
			method: func(client *mock.Client[any]) (any, error) {
				return client.ChangeMessageVisibility(ctx, nil)
			},
		},
		{
			name: "DeleteMessage",
			method: func(client *mock.Client[any]) (any, error) {
				return client.DeleteMessage(ctx, nil)
			},
		},
		{
			name: "MoveMessageToDLQ",
			method: func(client *mock.Client[any]) (any, error) {
				return client.MoveMessageToDLQ(ctx, nil)
			},
		},
		{
			name: "RedriveMessage",
			method: func(client *mock.Client[any]) (any, error) {
				return client.RedriveMessage(ctx, nil)
			},
		},
		{
			name: "GetMessage",
			method: func(client *mock.Client[any]) (any, error) {
				return client.GetMessage(ctx, nil)
			},
		},
		{
			name: "GetQueueStats",
			method: func(client *mock.Client[any]) (any, error) {
				return client.GetQueueStats(ctx, nil)
			},
		},
		{
			name: "GetDLQStats",
			method: func(client *mock.Client[any]) (any, error) {
				return client.GetDLQStats(ctx, nil)
			},
		},
		{
			name: "ListMessages",
			method: func(client *mock.Client[any]) (any, error) {
				return client.ListMessages(ctx, nil)
			},
		},
		{
			name: "ReplaceMessage",
			method: func(client *mock.Client[any]) (any, error) {
				return client.ReplaceMessage(ctx, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.method(mock.SuccessfulMockClient)
			if err != nil {
				t.Errorf("with implementation: error %v", err)
			}
			_, err = tt.method(notImplementedClient)
			if !errors.Is(err, mock.ErrNotImplemented) {
				t.Errorf("without implementation: got error %v, want %v", err, mock.ErrNotImplemented)
			}
		})
	}
}

func TestMockClockNow(t *testing.T) {
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	m := mock.Clock{
		T: now,
	}
	if got := m.Now(); !reflect.DeepEqual(got, now) {
		t.Errorf("Now() = %v, want %v", got, now)
	}
	opt := &dynamomq.ClientOptions{}
	mock.WithClock(m)(opt)
	if got := opt.Clock.Now(); !reflect.DeepEqual(got, now) {
		t.Errorf("Now() = %v, want %v", got, now)
	}
}

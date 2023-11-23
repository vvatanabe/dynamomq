package mock

import (
	"context"
	"errors"
	"time"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
)

var ErrNotImplemented = errors.New("not implemented")

type Client[T any] struct {
	SendMessageFunc            func(ctx context.Context, params *dynamomq.SendMessageInput[T]) (*dynamomq.SendMessageOutput[T], error)
	ReceiveMessageFunc         func(ctx context.Context, params *dynamomq.ReceiveMessageInput) (*dynamomq.ReceiveMessageOutput[T], error)
	UpdateMessageAsVisibleFunc func(ctx context.Context, params *dynamomq.UpdateMessageAsVisibleInput) (*dynamomq.UpdateMessageAsVisibleOutput[T], error)
	DeleteMessageFunc          func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error)
	MoveMessageToDLQFunc       func(ctx context.Context, params *dynamomq.MoveMessageToDLQInput) (*dynamomq.MoveMessageToDLQOutput, error)
	RedriveMessageFunc         func(ctx context.Context, params *dynamomq.RedriveMessageInput) (*dynamomq.RedriveMessageOutput, error)
	GetMessageFunc             func(ctx context.Context, params *dynamomq.GetMessageInput) (*dynamomq.GetMessageOutput[T], error)
	GetQueueStatsFunc          func(ctx context.Context, params *dynamomq.GetQueueStatsInput) (*dynamomq.GetQueueStatsOutput, error)
	GetDLQStatsFunc            func(ctx context.Context, params *dynamomq.GetDLQStatsInput) (*dynamomq.GetDLQStatsOutput, error)
	ListMessagesFunc           func(ctx context.Context, params *dynamomq.ListMessagesInput) (*dynamomq.ListMessagesOutput[T], error)
	ReplaceMessageFunc         func(ctx context.Context, params *dynamomq.ReplaceMessageInput[T]) (*dynamomq.ReplaceMessageOutput, error)
}

func (m Client[T]) SendMessage(ctx context.Context, params *dynamomq.SendMessageInput[T]) (*dynamomq.SendMessageOutput[T], error) {
	if m.SendMessageFunc != nil {
		return m.SendMessageFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) ReceiveMessage(ctx context.Context, params *dynamomq.ReceiveMessageInput) (*dynamomq.ReceiveMessageOutput[T], error) {
	if m.ReceiveMessageFunc != nil {
		return m.ReceiveMessageFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) UpdateMessageAsVisible(ctx context.Context, params *dynamomq.UpdateMessageAsVisibleInput) (*dynamomq.UpdateMessageAsVisibleOutput[T], error) {
	if m.UpdateMessageAsVisibleFunc != nil {
		return m.UpdateMessageAsVisibleFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) DeleteMessage(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
	if m.DeleteMessageFunc != nil {
		return m.DeleteMessageFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) MoveMessageToDLQ(ctx context.Context, params *dynamomq.MoveMessageToDLQInput) (*dynamomq.MoveMessageToDLQOutput, error) {
	if m.MoveMessageToDLQFunc != nil {
		return m.MoveMessageToDLQFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) RedriveMessage(ctx context.Context, params *dynamomq.RedriveMessageInput) (*dynamomq.RedriveMessageOutput, error) {
	if m.RedriveMessageFunc != nil {
		return m.RedriveMessageFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) GetMessage(ctx context.Context, params *dynamomq.GetMessageInput) (*dynamomq.GetMessageOutput[T], error) {
	if m.GetMessageFunc != nil {
		return m.GetMessageFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) GetQueueStats(ctx context.Context, params *dynamomq.GetQueueStatsInput) (*dynamomq.GetQueueStatsOutput, error) {
	if m.GetQueueStatsFunc != nil {
		return m.GetQueueStatsFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) GetDLQStats(ctx context.Context, params *dynamomq.GetDLQStatsInput) (*dynamomq.GetDLQStatsOutput, error) {
	if m.GetDLQStatsFunc != nil {
		return m.GetDLQStatsFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) ListMessages(ctx context.Context, params *dynamomq.ListMessagesInput) (*dynamomq.ListMessagesOutput[T], error) {
	if m.ListMessagesFunc != nil {
		return m.ListMessagesFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

func (m Client[T]) ReplaceMessage(ctx context.Context, params *dynamomq.ReplaceMessageInput[T]) (*dynamomq.ReplaceMessageOutput, error) {
	if m.ReplaceMessageFunc != nil {
		return m.ReplaceMessageFunc(ctx, params)
	}
	return nil, ErrNotImplemented
}

type Clock struct {
	T time.Time
}

func (m Clock) Now() time.Time {
	return m.T
}

func WithClock(clock clock.Clock) func(s *dynamomq.ClientOptions) {
	return func(s *dynamomq.ClientOptions) {
		if clock != nil {
			s.Clock = clock
		}
	}
}

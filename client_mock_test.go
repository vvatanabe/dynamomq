package dynamomq

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type MockClient[T any] struct {
	SendMessageFunc            func(ctx context.Context, params *SendMessageInput[T]) (*SendMessageOutput[T], error)
	ReceiveMessageFunc         func(ctx context.Context, params *ReceiveMessageInput) (*ReceiveMessageOutput[T], error)
	UpdateMessageAsVisibleFunc func(ctx context.Context, params *UpdateMessageAsVisibleInput) (*UpdateMessageAsVisibleOutput[T], error)
	DeleteMessageFunc          func(ctx context.Context, params *DeleteMessageInput) (*DeleteMessageOutput, error)
	MoveMessageToDLQFunc       func(ctx context.Context, params *MoveMessageToDLQInput) (*MoveMessageToDLQOutput, error)
	RedriveMessageFunc         func(ctx context.Context, params *RedriveMessageInput) (*RedriveMessageOutput, error)
	GetMessageFunc             func(ctx context.Context, params *GetMessageInput) (*GetMessageOutput[T], error)
	GetQueueStatsFunc          func(ctx context.Context, params *GetQueueStatsInput) (*GetQueueStatsOutput, error)
	GetDLQStatsFunc            func(ctx context.Context, params *GetDLQStatsInput) (*GetDLQStatsOutput, error)
	ListMessagesFunc           func(ctx context.Context, params *ListMessagesInput) (*ListMessagesOutput[T], error)
	ReplaceMessageFunc         func(ctx context.Context, params *ReplaceMessageInput[T]) (*ReplaceMessageOutput, error)
	GetDynamodbClientFunc      func() *dynamodb.Client
}

func (m MockClient[T]) SendMessage(ctx context.Context, params *SendMessageInput[T]) (*SendMessageOutput[T], error) {
	if m.SendMessageFunc != nil {
		return m.SendMessageFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) ReceiveMessage(ctx context.Context, params *ReceiveMessageInput) (*ReceiveMessageOutput[T], error) {
	if m.ReceiveMessageFunc != nil {
		return m.ReceiveMessageFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) UpdateMessageAsVisible(ctx context.Context, params *UpdateMessageAsVisibleInput) (*UpdateMessageAsVisibleOutput[T], error) {
	if m.UpdateMessageAsVisibleFunc != nil {
		return m.UpdateMessageAsVisibleFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) DeleteMessage(ctx context.Context, params *DeleteMessageInput) (*DeleteMessageOutput, error) {
	if m.DeleteMessageFunc != nil {
		return m.DeleteMessageFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) MoveMessageToDLQ(ctx context.Context, params *MoveMessageToDLQInput) (*MoveMessageToDLQOutput, error) {
	if m.MoveMessageToDLQFunc != nil {
		return m.MoveMessageToDLQFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) RedriveMessage(ctx context.Context, params *RedriveMessageInput) (*RedriveMessageOutput, error) {
	if m.RedriveMessageFunc != nil {
		return m.RedriveMessageFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) GetMessage(ctx context.Context, params *GetMessageInput) (*GetMessageOutput[T], error) {
	if m.GetMessageFunc != nil {
		return m.GetMessageFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) GetQueueStats(ctx context.Context, params *GetQueueStatsInput) (*GetQueueStatsOutput, error) {
	if m.GetQueueStatsFunc != nil {
		return m.GetQueueStatsFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) GetDLQStats(ctx context.Context, params *GetDLQStatsInput) (*GetDLQStatsOutput, error) {
	if m.GetDLQStatsFunc != nil {
		return m.GetDLQStatsFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) ListMessages(ctx context.Context, params *ListMessagesInput) (*ListMessagesOutput[T], error) {
	if m.ListMessagesFunc != nil {
		return m.ListMessagesFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) ReplaceMessage(ctx context.Context, params *ReplaceMessageInput[T]) (*ReplaceMessageOutput, error) {
	if m.ReplaceMessageFunc != nil {
		return m.ReplaceMessageFunc(ctx, params)
	}
	return nil, errors.New("not implemented")
}

func (m MockClient[T]) GetDynamodbClient() *dynamodb.Client {
	if m.GetDynamodbClientFunc != nil {
		return m.GetDynamodbClientFunc()
	}
	return nil
}

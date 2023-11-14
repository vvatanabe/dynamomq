package dynamomq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	uuid "github.com/satori/go.uuid"
	"github.com/upsidr/dynamotest"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func setupDynamoDB(t *testing.T, initialData ...*types.PutRequest) (tableName string, client *dynamodb.Client, clean func()) {
	client, clean = dynamotest.NewDynamoDB(t)
	tableName = DefaultTableName + "-" + uuid.NewV4().String()
	dynamotest.PrepTable(t, client, dynamotest.InitialTableSetup{
		Table: &dynamodb.CreateTableInput{
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("queue_type"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("queue_add_timestamp"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			BillingMode:               types.BillingModePayPerRequest,
			DeletionProtectionEnabled: aws.Bool(false),
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String("dynamo-mq-index-queue_type-queue_add_timestamp"),
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("queue_type"),
							KeyType:       types.KeyTypeHash,
						},
						{
							AttributeName: aws.String("queue_add_timestamp"),
							KeyType:       types.KeyTypeRange,
						},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       types.KeyTypeHash,
				},
			},
			TableName: aws.String(tableName),
		},
		InitialData: initialData,
	})
	return
}

const (
	testNameShouldReturnIDNotFoundError    = "should return IDNotFoundError"
	testNameShouldReturnIDNotProvidedError = "should return IDNotProvidedError"
)

func TestDynamoMQClientSendMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    func(*testing.T) (string, *dynamodb.Client, func())
		sdkClock clock.Clock
		args     *SendMessageInput[test.MessageData]
		want     *SendMessageOutput[test.MessageData]
		wantErr  error
	}{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: &SendMessageInput[test.MessageData]{
				ID:   "",
				Data: test.MessageData{},
			},
			want:    nil,
			wantErr: &IDNotProvidedError{},
		},
		{
			name: "should return IDDuplicatedError when id is duplicated",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: &SendMessageInput[test.MessageData]{
				ID:   "A-101",
				Data: test.NewMessageData("A-101"),
			},
			want:    nil,
			wantErr: &IDDuplicatedError{},
		},
		{
			name: "should succeed when id is not duplicated",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t)
			},
			sdkClock: mockClock{
				t: date(2023, 12, 1, 0, 0, 10),
			},
			args: &SendMessageInput[test.MessageData]{
				ID:   "A-101",
				Data: test.NewMessageData("A-101"),
			},
			want: &SendMessageOutput[test.MessageData]{
				Result: &Result{
					ID:                   "A-101",
					Status:               StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339Nano(date(2023, 12, 1, 0, 0, 10)),
					Version:              1,
				},
				Message: func() *Message[test.MessageData] {
					s := newTestMessageItemAsReady("A-101", date(2023, 12, 1, 0, 0, 10))
					return s
				}(),
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, tt.sdkClock, false)
			defer clean()
			result, err := client.SendMessage(ctx, tt.args)
			err = checkExpectedError(t, err, tt.wantErr, "SendMessage()")
			if err != nil || tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("SendMessage() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestDynamoMQClientReceiveMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    func(*testing.T) (string, *dynamodb.Client, func())
		sdkClock clock.Clock
		want     *ReceiveMessageOutput[test.MessageData]
		wantErr  error
	}{
		{
			name: "should return EmptyQueueError when queue is empty",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("A-202", clock.Now()).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("A-303", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			want:    nil,
			wantErr: &EmptyQueueError{},
		},
		{
			name: "should return message when exists ready message",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202",
							date(2023, 12, 1, 0, 0, 0)).
							marshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			want: func() *ReceiveMessageOutput[test.MessageData] {
				s := newTestMessageItemAsReady("B-202", date(2023, 12, 1, 0, 0, 0))
				_ = s.markAsProcessing(date(2023, 12, 1, 0, 0, 10), 0)
				s.Version = 2
				s.ReceiveCount = 1
				r := &ReceiveMessageOutput[test.MessageData]{
					Result: &Result{
						ID:                   s.ID,
						Status:               s.Status,
						LastUpdatedTimestamp: s.LastUpdatedTimestamp,
						Version:              s.Version,
					},
					PeekFromQueueTimestamp: s.PeekFromQueueTimestamp,
					PeekedMessageObject:    s,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "should return message when exists message expired visibility timeout",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("B-202",
							date(2023, 12, 1, 0, 0, 0)).
							marshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: date(2023, 12, 1, 0, 1, 1),
			},
			want: func() *ReceiveMessageOutput[test.MessageData] {
				s := newTestMessageItemAsProcessing("B-202", date(2023, 12, 1, 0, 0, 0))
				_ = s.markAsProcessing(date(2023, 12, 1, 0, 1, 1), 0)
				s.Version = 2
				s.ReceiveCount = 1
				r := &ReceiveMessageOutput[test.MessageData]{
					Result: &Result{
						ID:                   s.ID,
						Status:               s.Status,
						LastUpdatedTimestamp: s.LastUpdatedTimestamp,
						Version:              s.Version,
					},
					PeekFromQueueTimestamp: s.PeekFromQueueTimestamp,
					PeekedMessageObject:    s,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "should return EmptyQueueError when exists message but visibility timeout is not expired",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("B-202",
							date(2023, 12, 1, 0, 0, 0)).
							marshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: date(2023, 12, 1, 0, 0, 59),
			},
			want:    nil,
			wantErr: &EmptyQueueError{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, tt.sdkClock, false)
			defer clean()
			result, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
			err = checkExpectedError(t, err, tt.wantErr, "ReceiveMessage()")
			if err != nil || tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("ReceiveMessage() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestDynamoMQClientReceiveMessageUseFIFO(t *testing.T) {
	t.Parallel()
	now := date(2023, 12, 1, 0, 0, 10)
	ctx := context.Background()
	client, clean := prepareTestClient(t, ctx, func(t *testing.T) (string, *dynamodb.Client, func()) {
		return setupDynamoDB(t,
			&types.PutRequest{
				Item: newTestMessageItemAsReady("A-101", date(2023, 12, 1, 0, 0, 3)).
					marshalMapUnsafe(),
			},
			&types.PutRequest{
				Item: newTestMessageItemAsReady("A-202", date(2023, 12, 1, 0, 0, 2)).
					marshalMapUnsafe(),
			},
			&types.PutRequest{
				Item: newTestMessageItemAsReady("A-303", date(2023, 12, 1, 0, 0, 1)).
					marshalMapUnsafe(),
			},
		)
	}, mockClock{
		t: now,
	}, true)
	defer clean()

	wants := []*ReceiveMessageOutput[test.MessageData]{
		newMessageFromReadyToProcessing("A-303", date(2023, 12, 1, 0, 0, 1), now),
		newMessageFromReadyToProcessing("A-202", date(2023, 12, 1, 0, 0, 2), now),
		newMessageFromReadyToProcessing("A-101", date(2023, 12, 1, 0, 0, 3), now),
	}

	for i, want := range wants {
		result, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
		if err != nil {
			t.Errorf("ReceiveMessage() [%d] error = %v", i, err)
			return
		}
		if !reflect.DeepEqual(result, want) {
			v1, _ := json.Marshal(result)
			v2, _ := json.Marshal(want)
			t.Errorf("ReceiveMessage() [%d] got = %v, want %v", i, string(v1), string(v2))
		}
		_, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
		if !errors.Is(err, &EmptyQueueError{}) {
			t.Errorf("ReceiveMessage() [%d] error = %v, wantErr %v", i, err, &EmptyQueueError{})
			return
		}
		_, err = client.DeleteMessage(ctx, &DeleteMessageInput{
			ID: result.ID,
		})
		if err != nil {
			t.Errorf("DeleteMessage() [%d] error = %v", i, err)
			return
		}
	}

	_, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("ReceiveMessage() [last] error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
}

func TestDynamoMQClientReceiveMessageNotUseFIFO(t *testing.T) {
	t.Parallel()
	now := date(2023, 12, 1, 0, 0, 10)
	ctx := context.Background()
	client, clean := prepareTestClient(t, ctx, func(t *testing.T) (string, *dynamodb.Client, func()) {
		return setupDynamoDB(t,
			&types.PutRequest{
				Item: newTestMessageItemAsReady("A-101", date(2023, 12, 1, 0, 0, 3)).
					marshalMapUnsafe(),
			},
			&types.PutRequest{
				Item: newTestMessageItemAsReady("A-202", date(2023, 12, 1, 0, 0, 2)).
					marshalMapUnsafe(),
			},
			&types.PutRequest{
				Item: newTestMessageItemAsReady("A-303", date(2023, 12, 1, 0, 0, 1)).
					marshalMapUnsafe(),
			},
		)
	}, mockClock{
		t: now,
	}, false)
	defer clean()
	wants := []*ReceiveMessageOutput[test.MessageData]{
		newMessageFromReadyToProcessing("A-303", date(2023, 12, 1, 0, 0, 1), now),
		newMessageFromReadyToProcessing("A-202", date(2023, 12, 1, 0, 0, 2), now),
		newMessageFromReadyToProcessing("A-101", date(2023, 12, 1, 0, 0, 3), now),
	}
	for i, want := range wants {
		result, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
		if err != nil {
			t.Errorf("ReceiveMessage() [%d] error = %v", i, err)
			return
		}
		if !reflect.DeepEqual(result, want) {
			v1, _ := json.Marshal(result)
			v2, _ := json.Marshal(want)
			t.Errorf("ReceiveMessage() [%d] got = %v, want %v", i, string(v1), string(v2))
		}
	}
	_, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("ReceiveMessage() [last] error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
}

func TestDynamoMQClientUpdateMessageAsVisible(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (string, *dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *UpdateMessageAsVisibleOutput[test.MessageData]
		wantErr  error
	}{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "",
			},
			want:    nil,
			wantErr: &IDNotProvidedError{},
		},
		{
			name: testNameShouldReturnIDNotFoundError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "B-202",
			},
			want:    nil,
			wantErr: &IDNotFoundError{},
		},
		{
			name: "should succeed when id is found",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("A-101",
							date(2023, 12, 1, 0, 0, 10)).marshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: date(2023, 12, 1, 0, 0, 10),
			},
			args: args{
				id: "A-101",
			},
			want: &UpdateMessageAsVisibleOutput[test.MessageData]{
				Result: &Result{
					ID:     "A-101",
					Status: StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339Nano(
						date(2023, 12, 1, 0, 0, 10)),
					Version: 2,
				},
				Message: func() *Message[test.MessageData] {
					now := date(2023, 12, 1, 0, 0, 10)
					message := newTestMessageItemAsProcessing("A-101", now)
					err := message.markAsReady(now)
					if err != nil {
						panic(err)
					}
					message.Version = 2
					return message
				}(),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, tt.sdkClock, false)
			defer clean()
			result, err := client.UpdateMessageAsVisible(ctx, &UpdateMessageAsVisibleInput{
				ID: tt.args.id,
			})
			err = checkExpectedError(t, err, tt.wantErr, "UpdateMessageAsVisible()")
			if err != nil || tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("UpdateMessageAsVisible() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestDynamoMQClientDeleteMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (string, *dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     error
	}{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "",
			},
			want: &IDNotProvidedError{},
		},
		{
			name: "should not return error when not existing id",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "B-101",
			},
			want: nil,
		},
		{
			name: "should succeed when id is found",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, tt.sdkClock, false)
			defer clean()
			_, err := client.DeleteMessage(ctx, &DeleteMessageInput{
				ID: tt.args.id,
			})
			err = checkExpectedError(t, err, tt.want, "DeleteMessage()")
		})
	}
}

func TestDynamoMQClientMoveMessageToDLQ(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    func(*testing.T) (string, *dynamodb.Client, func())
		sdkClock clock.Clock
		args     *MoveMessageToDLQInput
		want     *MoveMessageToDLQOutput
		wantErr  error
	}{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: &MoveMessageToDLQInput{
				ID: "",
			},
			want:    nil,
			wantErr: &IDNotProvidedError{},
		},
		{
			name: testNameShouldReturnIDNotFoundError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: &MoveMessageToDLQInput{
				ID: "B-202",
			},
			want:    nil,
			wantErr: &IDNotFoundError{},
		},
		{
			name: "should succeed when id is found and queue type is standard",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("A-101",
							date(2023, 12, 1, 0, 0, 0)).
							marshalMapUnsafe(),
					},
				)
			},
			args: &MoveMessageToDLQInput{
				ID: "A-101",
			},
			want: func() *MoveMessageToDLQOutput {
				s := newTestMessageItemAsDLQ("A-101",
					date(2023, 12, 1, 0, 0, 0))
				r := &MoveMessageToDLQOutput{
					ID:                   s.ID,
					Status:               s.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.Version,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "should succeed when id is found and queue type is DLQ and status is processing",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("A-101",
							date(2023, 12, 1, 0, 0, 0)).
							marshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: date(2023, 12, 1, 0, 0, 10),
			},
			args: &MoveMessageToDLQInput{
				ID: "A-101",
			},
			want: func() *MoveMessageToDLQOutput {
				s := newTestMessageItemAsReady("A-101",
					date(2023, 12, 1, 0, 0, 0))
				err := s.markAsMovedToDLQ(date(2023, 12, 1, 0, 0, 10))
				if err != nil {
					panic(err)
				}
				s.Version = 2
				r := &MoveMessageToDLQOutput{
					ID:                   s.ID,
					Status:               s.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.Version,
				}
				return r
			}(),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, tt.sdkClock, false)
			defer clean()
			result, err := client.MoveMessageToDLQ(ctx, tt.args)
			err = checkExpectedError(t, err, tt.wantErr, "MoveMessageToDLQ()")
			if err != nil || tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("MoveMessageToDLQ() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestDynamoMQClientRedriveMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (string, *dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *RedriveMessageOutput
		wantErr  error
	}{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "",
			},
			want:    nil,
			wantErr: &IDNotProvidedError{},
		},
		{
			name: testNameShouldReturnIDNotFoundError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "B-202",
			},
			want:    nil,
			wantErr: &IDNotFoundError{},
		},
		{
			name: "should succeed when id is found and status is ready",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("A-101",
							date(2023, 12, 1, 0, 0, 0)).marshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: date(2023, 12, 1, 0, 0, 10),
			},
			args: args{
				id: "A-101",
			},
			want: &RedriveMessageOutput{
				ID:     "A-101",
				Status: StatusReady,
				LastUpdatedTimestamp: clock.FormatRFC3339Nano(
					date(2023, 12, 1, 0, 0, 10)),
				Version: 2,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, tt.sdkClock, false)
			defer clean()
			result, err := client.RedriveMessage(ctx, &RedriveMessageInput{
				ID: tt.args.id,
			})
			err = checkExpectedError(t, err, tt.wantErr, "RedriveMessage()")
			if err != nil || tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("RedriveMessage() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestDynamoMQClientGetQueueStats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func(*testing.T) (string, *dynamodb.Client, func())
		want    *GetQueueStatsOutput
		wantErr error
	}{
		{
			name: "should return empty items stats when no item in standard queue",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t)
			},
			want: &GetQueueStatsOutput{
				First100IDsInQueue:         []string{},
				First100SelectedIDsInQueue: []string{},
				TotalRecordsInQueue:        0,
				TotalRecordsInProcessing:   0,
				TotalRecordsNotStarted:     0,
			},
		},
		{
			name: "should return one item stats when one item in standard queue",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			want: &GetQueueStatsOutput{
				First100IDsInQueue:         []string{"A-101"},
				First100SelectedIDsInQueue: []string{},
				TotalRecordsInQueue:        1,
				TotalRecordsInProcessing:   0,
				TotalRecordsNotStarted:     1,
			},
		},
		{
			name: "should return one processing item stats when one item in standard queue",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			want: &GetQueueStatsOutput{
				First100IDsInQueue:         []string{"A-101"},
				First100SelectedIDsInQueue: []string{"A-101"},
				TotalRecordsInQueue:        1,
				TotalRecordsInProcessing:   1,
				TotalRecordsNotStarted:     0,
			},
		},
		{
			name: "should return two items stats when two items in standard queue",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202", clock.Now().Add(1*time.Second)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("C-303", clock.Now().Add(2*time.Second)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("D-404", clock.Now().Add(3*time.Second)).marshalMapUnsafe(),
					},
				)
			},
			want: &GetQueueStatsOutput{
				First100IDsInQueue:         []string{"A-101", "B-202", "C-303", "D-404"},
				First100SelectedIDsInQueue: []string{"C-303", "D-404"},
				TotalRecordsInQueue:        4,
				TotalRecordsInProcessing:   2,
				TotalRecordsNotStarted:     2,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, clock.RealClock{}, false)
			defer clean()
			got, err := client.GetQueueStats(ctx, &GetQueueStatsInput{})
			err = checkExpectedError(t, err, tt.wantErr, "GetQueueStats()")
			if err != nil || tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetQueueStats() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDynamoMQClientGetDLQStats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func(*testing.T) (string, *dynamodb.Client, func())
		want    *GetDLQStatsOutput
		wantErr error
	}{
		{
			name: "should return empty items when no items in DLQ",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now().Add(time.Second)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202", clock.Now().Add(1*time.Second)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("C-303", clock.Now().Add(2*time.Second)).marshalMapUnsafe(),
					},
				)
			},
			want: &GetDLQStatsOutput{
				First100IDsInQueue: []string{},
				TotalRecordsInDLQ:  0,
			},
		},
		{
			name: "should return three DLQ items when items in DLQ",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202", clock.Now().Add(1*time.Second)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsProcessing("C-303", clock.Now().Add(2*time.Second)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("D-404", clock.Now().Add(3*time.Second)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("E-505", clock.Now().Add(4*time.Second)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("F-606", clock.Now().Add(5*time.Second)).marshalMapUnsafe(),
					},
				)
			},
			want: &GetDLQStatsOutput{
				First100IDsInQueue: []string{"D-404", "E-505", "F-606"},
				TotalRecordsInDLQ:  3,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, clock.RealClock{}, false)
			defer clean()
			got, err := client.GetDLQStats(ctx, &GetDLQStatsInput{})
			err = checkExpectedError(t, err, tt.wantErr, "GetDLQStats()")
			if err != nil || tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDLQStats() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDynamoMQClientGetMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []struct {
		name    string
		setup   func(*testing.T) (string, *dynamodb.Client, func())
		args    args
		want    *Message[test.MessageData]
		wantErr error
	}{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "",
			},
			want:    nil,
			wantErr: &IDNotProvidedError{},
		},
		{
			name: "should not return message when id is not found",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "B-202",
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name: "should return message when id is found",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101",
							date(2023, 12, 1, 0, 0, 0)).marshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want: newTestMessageItemAsReady("A-101",
				date(2023, 12, 1, 0, 0, 0)),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, clock.RealClock{}, false)
			defer clean()
			got, err := client.GetMessage(ctx, &GetMessageInput{
				ID: tt.args.id,
			})
			err = checkExpectedError(t, err, tt.wantErr, "GetMessage()")
			if err != nil || tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(got.Message, tt.want) {
				t.Errorf("GetMessage() got = %v, want %v", got.Message, tt.want)
			}
		})
	}
}

func TestDynamoMQClientReplaceMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		message *Message[test.MessageData]
	}
	tests := []struct {
		name    string
		setup   func(*testing.T) (string, *dynamodb.Client, func())
		args    args
		want    *Message[test.MessageData]
		wantErr error
	}{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				message: &Message[test.MessageData]{
					ID: "",
				},
			},
			want:    nil,
			wantErr: &IDNotProvidedError{},
		},
		{
			name: "should return message when id is duplicated",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				message: newTestMessageItemAsReady("A-101",
					date(2023, 12, 1, 0, 0, 0)),
			},
			want: newTestMessageItemAsReady("A-101",
				date(2023, 12, 1, 0, 0, 0)),
			wantErr: nil,
		},
		{
			name: "should return message when id is unique",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).marshalMapUnsafe(),
					},
				)
			},
			args: args{
				message: newTestMessageItemAsReady("B-202",
					date(2023, 12, 1, 0, 0, 0)),
			},
			want: newTestMessageItemAsReady("B-202",
				date(2023, 12, 1, 0, 0, 0)),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, clock.RealClock{}, false)
			defer clean()
			_, err := client.ReplaceMessage(ctx, &ReplaceMessageInput[test.MessageData]{
				Message: tt.args.message,
			})
			err = checkExpectedError(t, err, tt.wantErr, "ReplaceMessage()")
			if err != nil || tt.wantErr != nil {
				return
			}
			got, err := client.GetMessage(ctx, &GetMessageInput{
				ID: tt.args.message.ID,
			})
			if err != nil {
				t.Errorf("GetMessage() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got.Message, tt.want) {
				t.Errorf("GetMessage() got = %v, want %v", got.Message, tt.want)
			}
		})
	}
}

func TestDynamoMQClientListMessages(t *testing.T) {
	t.Parallel()
	type args struct {
		size int32
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (string, *dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     []*Message[test.MessageData]
		wantErr  error
	}{
		{
			name: "should return empty list when no messages",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t)
			},
			args: args{
				size: 10,
			},
			want:    []*Message[test.MessageData]{},
			wantErr: nil,
		},
		{
			name: "should return list of messages when messages exist",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				messages := generateExpectedMessages("A",
					date(2023, 12, 1, 0, 0, 0), 10)
				puts := generatePutRequests(messages)
				return setupDynamoDB(t, puts...)
			},
			args: args{
				size: 10,
			},
			want: generateExpectedMessages("A",
				date(2023, 12, 1, 0, 0, 0), 10),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, clean := prepareTestClient(t, ctx, tt.setup, tt.sdkClock, false)
			defer clean()
			result, err := client.ListMessages(ctx, &ListMessagesInput{
				Size: tt.args.size,
			})
			err = checkExpectedError(t, err, tt.wantErr, "ListMessages()")
			if err != nil || tt.wantErr != nil {
				return
			}
			sort.Slice(result.Messages, func(i, j int) bool {
				return result.Messages[i].LastUpdatedTimestamp < result.Messages[j].LastUpdatedTimestamp
			})
			if !reflect.DeepEqual(result.Messages, tt.want) {
				t.Errorf("ListMessages() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func generateExpectedMessages(idPrefix string, now time.Time, count int) []*Message[test.MessageData] {
	messages := make([]*Message[test.MessageData], count)
	for i := 0; i < count; i++ {
		now = now.Add(time.Minute)
		messages[i] = newTestMessageItemAsReady(fmt.Sprintf("%s-%d", idPrefix, i), now)
	}
	return messages
}

func generatePutRequests(messages []*Message[test.MessageData]) []*types.PutRequest {
	var puts []*types.PutRequest
	for _, message := range messages {
		puts = append(puts, &types.PutRequest{
			Item: message.marshalMapUnsafe(),
		})
	}
	return puts
}

func checkExpectedError(t *testing.T, err, wantErr error, messagePrefix string) error {
	if wantErr != nil {
		if !errors.Is(err, wantErr) {
			t.Errorf("%s error = %v, wantErr %v", messagePrefix, err, wantErr)
			return err
		}
		return nil
	}
	if err != nil {
		t.Errorf("%s unexpected error = %v", messagePrefix, err)
		return err
	}
	return nil
}

func prepareTestClient(t *testing.T, ctx context.Context,
	setupTable func(*testing.T) (string, *dynamodb.Client, func()),
	sdkClock clock.Clock,
	useFIFO bool,
) (Client[test.MessageData], func()) {
	tableName, raw, clean := setupTable(t)
	optFns := []func(*ClientOptions){
		WithTableName(tableName),
		WithAWSDynamoDBClient(raw),
		withClock(sdkClock),
		WithUseFIFO(useFIFO),
		WithAWSVisibilityTimeout(1),
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatalf("failed to load aws config: %s\n", err)
		return nil, nil
	}
	client, err := NewFromConfig[test.MessageData](cfg, optFns...)
	if err != nil {
		t.Fatalf("failed to create DynamoMQ client: %s\n", err)
		return nil, nil
	}
	return client, clean
}

func date(year int, month time.Month, day, hour, min, sec int) time.Time {
	return time.Date(year, month, day, hour, min, sec, 0, time.UTC)
}

func newTestMessageItemAsReady(id string, now time.Time) *Message[test.MessageData] {
	return NewMessage[test.MessageData](id, test.NewMessageData(id), now)
}

func newTestMessageItemAsProcessing(id string, now time.Time) *Message[test.MessageData] {
	message := NewMessage[test.MessageData](id, test.NewMessageData(id), now)
	err := message.markAsProcessing(now, 0)
	if err != nil {
		panic(err)
	}
	return message
}

func newTestMessageItemAsDLQ(id string, now time.Time) *Message[test.MessageData] {
	message := NewMessage[test.MessageData](id, test.NewMessageData(id), now)
	err := message.markAsMovedToDLQ(now)
	if err != nil {
		panic(err)
	}
	return message
}

func newMessageFromReadyToProcessing(id string,
	readyTime time.Time, processingTime time.Time) *ReceiveMessageOutput[test.MessageData] {
	s := newTestMessageItemAsReady(id, readyTime)
	_ = s.markAsProcessing(processingTime, 0)
	s.Version = 2
	s.ReceiveCount = 1
	r := &ReceiveMessageOutput[test.MessageData]{
		Result: &Result{
			ID:                   s.ID,
			Status:               s.Status,
			LastUpdatedTimestamp: s.LastUpdatedTimestamp,
			Version:              s.Version,
		},
		PeekFromQueueTimestamp: s.PeekFromQueueTimestamp,
		PeekedMessageObject:    s,
	}
	return r
}

type mockClock struct {
	t time.Time
}

func (m mockClock) Now() time.Time {
	return m.t
}

func withClock(clock clock.Clock) func(s *ClientOptions) {
	return func(s *ClientOptions) {
		if clock != nil {
			s.Clock = clock
		}
	}
}

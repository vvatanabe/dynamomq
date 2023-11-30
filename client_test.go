package dynamomq_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/upsidr/dynamotest"
	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/mock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func SetupDynamoDB(t *testing.T, initialData ...*types.PutRequest) (tableName string, client *dynamodb.Client, clean func()) {
	client, clean = dynamotest.NewDynamoDB(t)
	tableName = dynamomq.DefaultTableName + "-" + uuid.NewString()
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

func NewSetupFunc(initialData ...*types.PutRequest) func(t *testing.T) (string, *dynamodb.Client, func()) {
	return func(t *testing.T) (string, *dynamodb.Client, func()) {
		return SetupDynamoDB(t, initialData...)
	}
}

type ClientTestCase[Args any, Want any] struct {
	name     string
	setup    func(*testing.T) (string, *dynamodb.Client, func())
	sdkClock clock.Clock
	args     Args
	want     Want
	wantErr  error
}

func TestDynamoMQClientShouldReturnError(t *testing.T) {
	t.Parallel()
	client, cancel := prepareTestClient(context.Background(), t,
		NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())), mock.Clock{}, false, nil, nil, nil)
	defer cancel()
	type testCase struct {
		name      string
		operation func() error
		wantError error
	}
	tests := []testCase{
		{
			name: "SendMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.SendMessage(context.Background(), nil)
				return err
			},
			wantError: &dynamomq.IDNotProvidedError{},
		},
		{
			name: "SendMessage should return IDDuplicatedError",
			operation: func() error {
				_, err := client.SendMessage(context.Background(), &dynamomq.SendMessageInput[test.MessageData]{
					ID:   "A-101",
					Data: test.NewMessageData("A-101"),
				})
				return err
			},
			wantError: &dynamomq.IDDuplicatedError{},
		},
		{
			name: "UpdateMessageAsVisible should return IDNotProvidedError",
			operation: func() error {
				_, err := client.UpdateMessageAsVisible(context.Background(), nil)
				return err
			},
			wantError: &dynamomq.IDNotProvidedError{},
		},
		{
			name: "UpdateMessageAsVisible should return IDNotFoundError",
			operation: func() error {
				_, err := client.UpdateMessageAsVisible(context.Background(), &dynamomq.UpdateMessageAsVisibleInput{
					ID: "B-101",
				})
				return err
			},
			wantError: &dynamomq.IDNotFoundError{},
		},
		{
			name: "MoveMessageToDLQ should return IDNotProvidedError",
			operation: func() error {
				_, err := client.MoveMessageToDLQ(context.Background(), nil)
				return err
			},
			wantError: &dynamomq.IDNotProvidedError{},
		},
		{
			name: "MoveMessageToDLQ should return IDNotFoundError",
			operation: func() error {
				_, err := client.MoveMessageToDLQ(context.Background(), &dynamomq.MoveMessageToDLQInput{
					ID: "B-101",
				})
				return err
			},
			wantError: &dynamomq.IDNotFoundError{},
		},
		{
			name: "RedriveMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.RedriveMessage(context.Background(), nil)
				return err
			},
			wantError: &dynamomq.IDNotProvidedError{},
		},
		{
			name: "RedriveMessage should return IDNotFoundError",
			operation: func() error {
				_, err := client.RedriveMessage(context.Background(), &dynamomq.RedriveMessageInput{
					ID: "B-101",
				})
				return err
			},
			wantError: &dynamomq.IDNotFoundError{},
		},
		{
			name: "DeleteMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.DeleteMessage(context.Background(), nil)
				return err
			},
			wantError: &dynamomq.IDNotProvidedError{},
		},
		{
			name: "GetMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.GetMessage(context.Background(), nil)
				return err
			},
			wantError: &dynamomq.IDNotProvidedError{},
		},
		{
			name: "ReplaceMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.ReplaceMessage(context.Background(), nil)
				return err
			},
			wantError: &dynamomq.IDNotProvidedError{},
		},
	}
	for _, tt := range tests {
		test.AssertError(t, tt.operation(), tt.wantError, tt.name)
	}
}

func TestDynamoMQClientSendMessage(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[*dynamomq.SendMessageInput[test.MessageData], *dynamomq.SendMessageOutput[test.MessageData]]{
		{
			name:  "should succeed when id is not duplicated",
			setup: NewSetupFunc(),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate,
			},
			args: &dynamomq.SendMessageInput[test.MessageData]{
				ID:   "A-101",
				Data: test.NewMessageData("A-101"),
			},
			want: &dynamomq.SendMessageOutput[test.MessageData]{
				Result: &dynamomq.Result{
					ID:                   "A-101",
					Status:               dynamomq.StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339Nano(test.DefaultTestDate),
					Version:              1,
				},
				Message: func() *dynamomq.Message[test.MessageData] {
					s := NewTestMessageItemAsReady("A-101", test.DefaultTestDate)
					return s
				}(),
			},
		},
	}
	runTestsParallel[*dynamomq.SendMessageInput[test.MessageData], *dynamomq.SendMessageOutput[test.MessageData]](t, "SendMessage()", tests,
		func(client dynamomq.Client[test.MessageData], args *dynamomq.SendMessageInput[test.MessageData]) (*dynamomq.SendMessageOutput[test.MessageData], error) {
			return client.SendMessage(context.Background(), args)
		})
}

func TestDynamoMQClientReceiveMessage(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[any, *dynamomq.ReceiveMessageOutput[test.MessageData]]{
		{
			name: "should return EmptyQueueError when queue is empty",
			setup: NewSetupFunc(
				newPutRequestWithProcessingItem("A-202", clock.Now()),
				newPutRequestWithDLQItem("A-303", clock.Now()),
			),
			want:    nil,
			wantErr: &dynamomq.EmptyQueueError{},
		},
		{
			name:  "should return message when exists ready message",
			setup: NewSetupFunc(newPutRequestWithReadyItem("B-202", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(10 * time.Second),
			},
			want: func() *dynamomq.ReceiveMessageOutput[test.MessageData] {
				return NewMessageFromReadyToProcessing("B-202", test.DefaultTestDate, test.DefaultTestDate.Add(10*time.Second))
			}(),
			wantErr: nil,
		},
		{
			name:  "should return message when exists message expired visibility timeout",
			setup: NewSetupFunc(newPutRequestWithReadyItem("B-202", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(10 * time.Minute).Add(1 * time.Second),
			},
			want: func() *dynamomq.ReceiveMessageOutput[test.MessageData] {
				m := NewTestMessageItemAsProcessing("B-202", test.DefaultTestDate)
				MarkAsProcessing(m, test.DefaultTestDate.Add(10*time.Minute).Add(1*time.Second))
				m.Version = 2
				m.ReceiveCount = 1
				r := &dynamomq.ReceiveMessageOutput[test.MessageData]{
					Result: &dynamomq.Result{
						ID:                   m.ID,
						Status:               m.Status,
						LastUpdatedTimestamp: m.LastUpdatedTimestamp,
						Version:              m.Version,
					},
					PeekFromQueueTimestamp: m.PeekFromQueueTimestamp,
					PeekedMessageObject:    m,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name:  "should return EmptyQueueError when exists message but visibility timeout is not expired",
			setup: NewSetupFunc(newPutRequestWithProcessingItem("B-202", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(59 * time.Second),
			},
			want:    nil,
			wantErr: &dynamomq.EmptyQueueError{},
		},
	}
	runTestsParallel[any, *dynamomq.ReceiveMessageOutput[test.MessageData]](t, "ReceiveMessage()", tests,
		func(client dynamomq.Client[test.MessageData], _ any) (*dynamomq.ReceiveMessageOutput[test.MessageData], error) {
			return client.ReceiveMessage(context.Background(), nil)
		})
}

func testDynamoMQClientReceiveMessageSequence(t *testing.T, useFIFO bool) {
	now := test.DefaultTestDate.Add(10 * time.Second)
	ctx := context.Background()
	client, clean := prepareTestClient(ctx, t, NewSetupFunc(
		newPutRequestWithReadyItem("A-101", test.DefaultTestDate.Add(3*time.Second)),
		newPutRequestWithReadyItem("A-202", test.DefaultTestDate.Add(2*time.Second)),
		newPutRequestWithReadyItem("A-303", test.DefaultTestDate.Add(1*time.Second))),
		mock.Clock{
			T: now,
		}, useFIFO, nil, nil, nil)
	defer clean()

	wants := []*dynamomq.ReceiveMessageOutput[test.MessageData]{
		NewMessageFromReadyToProcessing("A-303", test.DefaultTestDate.Add(1*time.Second), now),
		NewMessageFromReadyToProcessing("A-202", test.DefaultTestDate.Add(2*time.Second), now),
		NewMessageFromReadyToProcessing("A-101", test.DefaultTestDate.Add(3*time.Second), now),
	}

	for i, want := range wants {
		result, err := client.ReceiveMessage(ctx, &dynamomq.ReceiveMessageInput{})
		test.AssertError(t, err, nil, fmt.Sprintf("ReceiveMessage() [%d-1]", i))
		test.AssertDeepEqual(t, result, want, fmt.Sprintf("ReceiveMessage() [%d-2]", i))

		if !useFIFO {
			return
		}

		_, err = client.ReceiveMessage(ctx, &dynamomq.ReceiveMessageInput{})
		test.AssertError(t, err, &dynamomq.EmptyQueueError{}, fmt.Sprintf("ReceiveMessage() [%d-3]", i))

		_, err = client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
			ID: result.ID,
		})
		test.AssertError(t, err, nil, fmt.Sprintf("DeleteMessage() [%d]", i))
	}

	_, err := client.ReceiveMessage(ctx, &dynamomq.ReceiveMessageInput{})
	test.AssertError(t, err, &dynamomq.EmptyQueueError{}, "ReceiveMessage() [last]")
}

func TestDynamoMQClientReceiveMessageUseFIFO(t *testing.T) {
	t.Parallel()
	testDynamoMQClientReceiveMessageSequence(t, true)
}

func TestDynamoMQClientReceiveMessageNotUseFIFO(t *testing.T) {
	t.Parallel()
	testDynamoMQClientReceiveMessageSequence(t, false)
}

func TestDynamoMQClientUpdateMessageAsVisible(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	now := test.DefaultTestDate.Add(10 * time.Second)
	tests := []ClientTestCase[args, *dynamomq.UpdateMessageAsVisibleOutput[test.MessageData]]{
		{
			name:  "should succeed when id is found",
			setup: NewSetupFunc(newPutRequestWithProcessingItem("A-101", now)),
			sdkClock: mock.Clock{
				T: now,
			},
			args: args{
				id: "A-101",
			},
			want: &dynamomq.UpdateMessageAsVisibleOutput[test.MessageData]{
				Result: &dynamomq.Result{
					ID:                   "A-101",
					Status:               dynamomq.StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339Nano(now),
					Version:              2,
				},
				Message: func() *dynamomq.Message[test.MessageData] {
					m := NewTestMessageItemAsProcessing("A-101", now)
					MarkAsReady(m, now)
					m.Version = 2
					return m
				}(),
			},
		},
		{
			name:  "should return InvalidStateTransitionError when status is ready",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", now)),
			sdkClock: mock.Clock{
				T: now,
			},
			args: args{
				id: "A-101",
			},
			want: &dynamomq.UpdateMessageAsVisibleOutput[test.MessageData]{},
			wantErr: dynamomq.InvalidStateTransitionError{
				Msg:       "message is currently ready",
				Operation: "mark as ready",
				Current:   dynamomq.StatusReady,
			},
		},
	}
	runTestsParallel[args, *dynamomq.UpdateMessageAsVisibleOutput[test.MessageData]](t, "UpdateMessageAsVisible()", tests,
		func(client dynamomq.Client[test.MessageData], args args) (*dynamomq.UpdateMessageAsVisibleOutput[test.MessageData], error) {
			return client.UpdateMessageAsVisible(context.Background(), &dynamomq.UpdateMessageAsVisibleInput{
				ID: args.id,
			})
		})
}

func TestDynamoMQClientDeleteMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []ClientTestCase[args, *dynamomq.DeleteMessageOutput]{
		{
			name:  "should not return error when not existing id",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())),
			args: args{
				id: "B-101",
			},
			want: &dynamomq.DeleteMessageOutput{},
		},
		{
			name:  "should succeed when id is found",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())),
			args: args{
				id: "A-101",
			},
			want: &dynamomq.DeleteMessageOutput{},
		},
	}
	runTestsParallel[args, *dynamomq.DeleteMessageOutput](t, "DeleteMessage()", tests,
		func(client dynamomq.Client[test.MessageData], args args) (*dynamomq.DeleteMessageOutput, error) {
			return client.DeleteMessage(context.Background(), &dynamomq.DeleteMessageInput{
				ID: args.id,
			})
		})
}

func TestDynamoMQClientMoveMessageToDLQ(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[*dynamomq.MoveMessageToDLQInput, *dynamomq.MoveMessageToDLQOutput]{
		{
			name:  "should succeed when id is found and queue type is standard",
			setup: NewSetupFunc(newPutRequestWithDLQItem("A-101", test.DefaultTestDate)),
			args: &dynamomq.MoveMessageToDLQInput{
				ID: "A-101",
			},
			want: func() *dynamomq.MoveMessageToDLQOutput {
				s := NewTestMessageItemAsDLQ("A-101", test.DefaultTestDate)
				r := &dynamomq.MoveMessageToDLQOutput{
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
			name:  "should succeed when id is found and queue type is DLQ and status is processing",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(10 * time.Second),
			},
			args: &dynamomq.MoveMessageToDLQInput{
				ID: "A-101",
			},
			want: func() *dynamomq.MoveMessageToDLQOutput {
				m := NewTestMessageItemAsReady("A-101", test.DefaultTestDate)
				MarkAsMovedToDLQ(m, test.DefaultTestDate.Add(10*time.Second))
				m.Version = 2
				r := &dynamomq.MoveMessageToDLQOutput{
					ID:                   m.ID,
					Status:               m.Status,
					LastUpdatedTimestamp: m.LastUpdatedTimestamp,
					Version:              m.Version,
				}
				return r
			}(),
			wantErr: nil,
		},
	}
	runTestsParallel[*dynamomq.MoveMessageToDLQInput, *dynamomq.MoveMessageToDLQOutput](t, "MoveMessageToDLQ()", tests,
		func(client dynamomq.Client[test.MessageData], args *dynamomq.MoveMessageToDLQInput) (*dynamomq.MoveMessageToDLQOutput, error) {
			return client.MoveMessageToDLQ(context.Background(), args)
		})
}

func TestDynamoMQClientRedriveMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []ClientTestCase[args, *dynamomq.RedriveMessageOutput]{
		{
			name:  "should succeed when id is found and status is ready",
			setup: NewSetupFunc(newPutRequestWithDLQItem("A-101", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(10 * time.Second),
			},
			args: args{
				id: "A-101",
			},
			want: &dynamomq.RedriveMessageOutput{
				ID:                   "A-101",
				Status:               dynamomq.StatusReady,
				LastUpdatedTimestamp: clock.FormatRFC3339Nano(test.DefaultTestDate.Add(10 * time.Second)),
				Version:              2,
			},
		},
		{
			name:  "should return InvalidStateTransitionError when message is not DLQ",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(10 * time.Second),
			},
			args: args{
				id: "A-101",
			},
			want: &dynamomq.RedriveMessageOutput{},
			wantErr: dynamomq.InvalidStateTransitionError{
				Msg:       "can only redrive messages from DLQ",
				Operation: "mark as restored from DLQ",
				Current:   dynamomq.StatusReady,
			},
		},
		{
			name: "should return InvalidStateTransitionError when message is selected in DLQ",
			setup: NewSetupFunc(&types.PutRequest{
				Item: func() map[string]types.AttributeValue {
					msg := NewTestMessageItemAsDLQ("A-101", test.DefaultTestDate)
					MarkAsProcessing(msg, test.DefaultTestDate)
					return marshalMapUnsafe(msg)
				}(),
			}),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate,
			},
			args: args{
				id: "A-101",
			},
			want: &dynamomq.RedriveMessageOutput{},
			wantErr: dynamomq.InvalidStateTransitionError{
				Msg:       "can only redrive messages from READY",
				Operation: "mark as restored from DLQ",
				Current:   dynamomq.StatusProcessing,
			},
		},
	}
	runTestsParallel[args, *dynamomq.RedriveMessageOutput](t, "RedriveMessage()", tests,
		func(client dynamomq.Client[test.MessageData], args args) (*dynamomq.RedriveMessageOutput, error) {
			return client.RedriveMessage(context.Background(), &dynamomq.RedriveMessageInput{
				ID: args.id,
			})
		})
}

func TestDynamoMQClientGetQueueStats(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[any, *dynamomq.GetQueueStatsOutput]{
		{
			name:  "should return empty items stats when no item in standard queue",
			setup: NewSetupFunc(),
			want: &dynamomq.GetQueueStatsOutput{
				First100IDsInQueue:         []string{},
				First100SelectedIDsInQueue: []string{},
				TotalRecordsInQueue:        0,
				TotalRecordsInProcessing:   0,
				TotalRecordsNotStarted:     0,
			},
		},
		{
			name:  "should return one item stats when one item in standard queue",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())),
			want: &dynamomq.GetQueueStatsOutput{
				First100IDsInQueue:         []string{"A-101"},
				First100SelectedIDsInQueue: []string{},
				TotalRecordsInQueue:        1,
				TotalRecordsInProcessing:   0,
				TotalRecordsNotStarted:     1,
			},
		},
		{
			name:  "should return one processing item stats when one item in standard queue",
			setup: NewSetupFunc(newPutRequestWithProcessingItem("A-101", clock.Now())),
			want: &dynamomq.GetQueueStatsOutput{
				First100IDsInQueue:         []string{"A-101"},
				First100SelectedIDsInQueue: []string{"A-101"},
				TotalRecordsInQueue:        1,
				TotalRecordsInProcessing:   1,
				TotalRecordsNotStarted:     0,
			},
		},
		{
			name: "should return two items stats when two items in standard queue",
			setup: NewSetupFunc(
				newPutRequestWithReadyItem("A-101", clock.Now()),
				newPutRequestWithReadyItem("B-202", clock.Now().Add(1*time.Second)),
				newPutRequestWithProcessingItem("C-303", clock.Now().Add(2*time.Second)),
				newPutRequestWithProcessingItem("D-404", clock.Now().Add(3*time.Second)),
			),
			want: &dynamomq.GetQueueStatsOutput{
				First100IDsInQueue:         []string{"A-101", "B-202", "C-303", "D-404"},
				First100SelectedIDsInQueue: []string{"C-303", "D-404"},
				TotalRecordsInQueue:        4,
				TotalRecordsInProcessing:   2,
				TotalRecordsNotStarted:     2,
			},
		},
	}
	runTestsParallel[any, *dynamomq.GetQueueStatsOutput](t, "GetQueueStats()", tests,
		func(client dynamomq.Client[test.MessageData], _ any) (*dynamomq.GetQueueStatsOutput, error) {
			return client.GetQueueStats(context.Background(), nil)
		})
}

func TestDynamoMQClientGetDLQStats(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[any, *dynamomq.GetDLQStatsOutput]{
		{
			name: "should return empty items when no items in DLQ",
			setup: NewSetupFunc(
				newPutRequestWithReadyItem("A-101", clock.Now().Add(time.Second)),
				newPutRequestWithReadyItem("B-202", clock.Now().Add(time.Second)),
				newPutRequestWithProcessingItem("C-303", clock.Now().Add(2*time.Second)),
			),
			want: &dynamomq.GetDLQStatsOutput{
				First100IDsInQueue: []string{},
				TotalRecordsInDLQ:  0,
			},
		},
		{
			name: "should return three DLQ items when items in DLQ",
			setup: NewSetupFunc(
				newPutRequestWithReadyItem("A-101", clock.Now().Add(time.Second)),
				newPutRequestWithReadyItem("B-202", clock.Now().Add(time.Second)),
				newPutRequestWithProcessingItem("C-303", clock.Now().Add(2*time.Second)),
				newPutRequestWithDLQItem("D-404", clock.Now().Add(3*time.Second)),
				newPutRequestWithDLQItem("E-505", clock.Now().Add(4*time.Second)),
				newPutRequestWithDLQItem("F-606", clock.Now().Add(5*time.Second)),
			),
			want: &dynamomq.GetDLQStatsOutput{
				First100IDsInQueue: []string{"D-404", "E-505", "F-606"},
				TotalRecordsInDLQ:  3,
			},
		},
	}
	runTestsParallel[any, *dynamomq.GetDLQStatsOutput](t, "GetDLQStats()", tests,
		func(client dynamomq.Client[test.MessageData], _ any) (*dynamomq.GetDLQStatsOutput, error) {
			return client.GetDLQStats(context.Background(), nil)
		})
}

func TestDynamoMQClientGetMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []ClientTestCase[args, *dynamomq.Message[test.MessageData]]{
		{
			name:  "should not return message when id is not found",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())),
			args: args{
				id: "B-202",
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name: "should return message when id is found",
			setup: NewSetupFunc(
				newPutRequestWithReadyItem("A-101", test.DefaultTestDate),
				newPutRequestWithReadyItem("B-202", clock.Now()),
			),
			args: args{
				id: "A-101",
			},
			want:    NewTestMessageItemAsReady("A-101", test.DefaultTestDate),
			wantErr: nil,
		},
	}
	runTestsParallel[args, *dynamomq.Message[test.MessageData]](t, "GetMessage()", tests,
		func(client dynamomq.Client[test.MessageData], args args) (*dynamomq.Message[test.MessageData], error) {
			got, err := client.GetMessage(context.Background(), &dynamomq.GetMessageInput{
				ID: args.id,
			})
			return got.Message, err
		})
}

func TestDynamoMQClientReplaceMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		message *dynamomq.Message[test.MessageData]
	}
	tests := []ClientTestCase[args, *dynamomq.Message[test.MessageData]]{
		{
			name:  "should return message when id is duplicated",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())),
			args: args{
				message: NewTestMessageItemAsReady("A-101", test.DefaultTestDate),
			},
			want:    NewTestMessageItemAsReady("A-101", test.DefaultTestDate),
			wantErr: nil,
		},
		{
			name:  "should return message when id is unique",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())),
			args: args{
				message: NewTestMessageItemAsReady("B-202", test.DefaultTestDate),
			},
			want:    NewTestMessageItemAsReady("B-202", test.DefaultTestDate),
			wantErr: nil,
		},
	}
	runTestsParallel[args, *dynamomq.Message[test.MessageData]](t, "ReplaceMessage()", tests,
		func(client dynamomq.Client[test.MessageData], args args) (*dynamomq.Message[test.MessageData], error) {
			ctx := context.Background()
			_, err := client.ReplaceMessage(ctx, &dynamomq.ReplaceMessageInput[test.MessageData]{
				Message: args.message,
			})
			if err != nil {
				return nil, err
			}
			got, err := client.GetMessage(ctx, &dynamomq.GetMessageInput{
				ID: args.message.ID,
			})
			if err != nil {
				return nil, err
			}
			return got.Message, err
		})
}

func TestDynamoMQClientListMessages(t *testing.T) {
	t.Parallel()
	type args struct {
		size int32
	}
	tests := []ClientTestCase[*args, []*dynamomq.Message[test.MessageData]]{
		{
			name:  "should return empty list when no messages",
			setup: NewSetupFunc(),
			args: &args{
				size: 10,
			},
			want:    []*dynamomq.Message[test.MessageData]{},
			wantErr: nil,
		},
		{
			name: "should return list of messages when messages exist",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				messages := generateExpectedMessages(test.DefaultTestDate)
				puts := generatePutRequests(messages)
				return SetupDynamoDB(t, puts...)
			},
			args: &args{
				size: 10,
			},
			want:    generateExpectedMessages(test.DefaultTestDate),
			wantErr: nil,
		},
		{
			name: "should return list of messages when messages exist and args is nil",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				messages := generateExpectedMessages(test.DefaultTestDate)
				puts := generatePutRequests(messages)
				return SetupDynamoDB(t, puts...)
			},
			args:    nil,
			want:    generateExpectedMessages(test.DefaultTestDate),
			wantErr: nil,
		},
	}
	runTestsParallel[*args, []*dynamomq.Message[test.MessageData]](t, "ListMessages()", tests,
		func(client dynamomq.Client[test.MessageData], args *args) ([]*dynamomq.Message[test.MessageData], error) {
			var in *dynamomq.ListMessagesInput
			if args != nil {
				in = &dynamomq.ListMessagesInput{
					Size: args.size,
				}
			}
			out, err := client.ListMessages(context.Background(), in)
			return out.Messages, err
		})
}

func runTestsParallel[Args any, Want any](t *testing.T, prefix string,
	tests []ClientTestCase[Args, Want], operation func(dynamomq.Client[test.MessageData], Args) (Want, error)) {
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client, clean := prepareTestClient(context.Background(), t, tt.setup, tt.sdkClock, false, nil, nil, nil)
			defer clean()
			result, err := operation(client, tt.args)
			if tt.wantErr != nil {
				test.AssertError(t, err, tt.wantErr, prefix)
				return
			}
			test.AssertDeepEqual(t, result, tt.want, prefix)
		})
	}
}

func prepareTestClient(ctx context.Context, t *testing.T,
	setupTable func(*testing.T) (string, *dynamodb.Client, func()),
	sdkClock clock.Clock,
	useFIFO bool,
	unmarshalMap func(m map[string]types.AttributeValue, out interface{}) error,
	marshalMap func(in interface{}) (map[string]types.AttributeValue, error),
	unmarshalListOfMaps func(l []map[string]types.AttributeValue, out interface{}) error,
) (dynamomq.Client[test.MessageData], func()) {
	t.Helper()
	tableName, raw, clean := setupTable(t)
	optFns := []func(*dynamomq.ClientOptions){
		dynamomq.WithTableName(tableName),
		dynamomq.WithQueueingIndexName(dynamomq.DefaultQueueingIndexName),
		dynamomq.WithAWSBaseEndpoint(""),
		dynamomq.WithAWSDynamoDBClient(raw),
		mock.WithClock(sdkClock),
		dynamomq.WithUseFIFO(useFIFO),
		dynamomq.WithAWSVisibilityTimeout(1),
		dynamomq.WithAWSRetryMaxAttempts(dynamomq.DefaultRetryMaxAttempts),
		WithUnmarshalMap(unmarshalMap),
		WithMarshalMap(marshalMap),
		WithUnmarshalListOfMaps(unmarshalListOfMaps),
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatalf("failed to load aws config: %s\n", err)
		return nil, nil
	}
	client, err := dynamomq.NewFromConfig[test.MessageData](cfg, optFns...)
	if err != nil {
		t.Fatalf("failed to create DynamoMQ client: %s\n", err)
		return nil, nil
	}
	return client, clean
}

func newPutRequestWithReadyItem(id string, now time.Time) *types.PutRequest {
	return &types.PutRequest{
		Item: marshalMapUnsafe(NewTestMessageItemAsReady(id, now)),
	}
}

func newPutRequestWithProcessingItem(id string, now time.Time) *types.PutRequest {
	return &types.PutRequest{
		Item: marshalMapUnsafe(NewTestMessageItemAsProcessing(id, now)),
	}
}

func newPutRequestWithDLQItem(id string, now time.Time) *types.PutRequest {
	return &types.PutRequest{
		Item: marshalMapUnsafe(NewTestMessageItemAsDLQ(id, now)),
	}
}

func generateExpectedMessages(now time.Time) []*dynamomq.Message[test.MessageData] {
	messages := make([]*dynamomq.Message[test.MessageData], 10)
	for i := 0; i < 10; i++ {
		now = now.Add(time.Minute)
		messages[i] = NewTestMessageItemAsReady(fmt.Sprintf("A-%d", i), now)
	}
	return messages
}

func generatePutRequests(messages []*dynamomq.Message[test.MessageData]) []*types.PutRequest {
	var puts []*types.PutRequest
	for _, message := range messages {
		puts = append(puts, &types.PutRequest{
			Item: marshalMapUnsafe(message),
		})
	}
	return puts
}

func marshalMapUnsafe[T any](m *dynamomq.Message[T]) map[string]types.AttributeValue {
	item, _ := marshalMap(m)
	return item
}

func marshalMap[T any](m *dynamomq.Message[T]) (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(m)
	if err != nil {
		return nil, dynamomq.MarshalingAttributeError{Cause: err}
	}
	return item, nil
}

func TestNewFromConfig(t *testing.T) {
	_, err := dynamomq.NewFromConfig[any](aws.Config{}, dynamomq.WithAWSBaseEndpoint("https://localhost:8000"))
	if err != nil {
		t.Errorf("failed to new client from config: %s\n", err)
	}
}

func TestTestDynamoMQClientReturnUnmarshalingAttributeError(t *testing.T) {
	t.Parallel()
	setupFunc := NewSetupFunc(
		newPutRequestWithReadyItem("A-101", clock.Now()),
		newPutRequestWithDLQItem("B-101", clock.Now()),
	)
	client, cancel := prepareTestClient(context.Background(), t, setupFunc, mock.Clock{}, false,
		func(m map[string]types.AttributeValue, out interface{}) error {
			return test.ErrTest
		},
		nil,
		func(l []map[string]types.AttributeValue, out interface{}) error {
			return test.ErrTest
		})
	defer cancel()
	type testCase struct {
		name      string
		operation func() (any, error)
	}
	tests := []testCase{
		{
			name: "ReceiveMessage should return UnmarshalingAttributeError",
			operation: func() (any, error) {
				return client.ReceiveMessage(context.Background(), &dynamomq.ReceiveMessageInput{})
			},
		},
		{
			name: "GetQueueStats should return UnmarshalingAttributeError",
			operation: func() (any, error) {
				return client.GetQueueStats(context.Background(), &dynamomq.GetQueueStatsInput{})
			},
		},
		{
			name: "GetDLQStats should return UnmarshalingAttributeError",
			operation: func() (any, error) {
				return client.GetDLQStats(context.Background(), &dynamomq.GetDLQStatsInput{})
			},
		},
		{
			name: "GetMessage should return UnmarshalingAttributeError",
			operation: func() (any, error) {
				return client.GetMessage(context.Background(), &dynamomq.GetMessageInput{
					ID: "A-101",
				})
			},
		},
		{
			name: "UpdateMessageAsVisible should return UnmarshalingAttributeError",
			operation: func() (any, error) {
				return client.UpdateMessageAsVisible(context.Background(), &dynamomq.UpdateMessageAsVisibleInput{
					ID: "A-101",
				})
			},
		},
		{
			name: "MoveMessageToDLQ should return UnmarshalingAttributeError",
			operation: func() (any, error) {
				return client.MoveMessageToDLQ(context.Background(), &dynamomq.MoveMessageToDLQInput{
					ID: "A-101",
				})
			},
		},
		{
			name: "RedriveMessage should return UnmarshalingAttributeError",
			operation: func() (any, error) {
				return client.RedriveMessage(context.Background(), &dynamomq.RedriveMessageInput{
					ID: "B-101",
				})
			},
		},
		{
			name: "ListMessages should return UnmarshalingAttributeError",
			operation: func() (any, error) {
				return client.ListMessages(context.Background(), &dynamomq.ListMessagesInput{
					Size: dynamomq.DefaultMaxListMessages,
				})
			},
		},
	}
	for _, tt := range tests {
		_, err := tt.operation()
		test.AssertError(t, err, dynamomq.UnmarshalingAttributeError{
			Cause: test.ErrTest,
		}, tt.name)
	}
}

func TestTestDynamoMQClientReturnMarshalingAttributeError(t *testing.T) {
	t.Parallel()
	setupFunc := NewSetupFunc(
		newPutRequestWithReadyItem("A-101", clock.Now()),
	)
	client, cancel := prepareTestClient(context.Background(), t, setupFunc, mock.Clock{}, false, nil,
		func(in interface{}) (map[string]types.AttributeValue, error) {
			return nil, test.ErrTest
		}, nil)
	defer cancel()
	type testCase struct {
		name      string
		operation func() (any, error)
	}
	tests := []testCase{
		{
			name: "ReceiveMessage should return MarshalingAttributeError",
			operation: func() (any, error) {
				return client.SendMessage(context.Background(), &dynamomq.SendMessageInput[test.MessageData]{
					ID:   "B-101",
					Data: test.NewMessageData("B-101"),
				})
			},
		},
		{
			name: "ReceiveMessage should return MarshalingAttributeError",
			operation: func() (any, error) {
				return client.ReplaceMessage(context.Background(), &dynamomq.ReplaceMessageInput[test.MessageData]{
					Message: NewTestMessageItemAsReady("B-101", clock.Now()),
				})
			},
		},
	}
	for _, tt := range tests {
		_, err := tt.operation()
		test.AssertError(t, err, dynamomq.MarshalingAttributeError{
			Cause: test.ErrTest,
		}, tt.name)
	}
}

func TestTestDynamoMQClientReturnDynamoDBAPIError(t *testing.T) {
	t.Parallel()
	client, err := dynamomq.NewFromConfig[test.MessageData](aws.Config{})
	if err != nil {
		t.Fatalf("failed to create DynamoMQ client: %s\n", err)
	}
	type testCase struct {
		name      string
		operation func() (any, error)
	}
	tests := []testCase{
		{
			name: "ReceiveMessage should return DynamoDBAPIError",
			operation: func() (any, error) {
				return client.ReceiveMessage(context.Background(), &dynamomq.ReceiveMessageInput{})
			},
		},
		{
			name: "DeleteMessage should return DynamoDBAPIError",
			operation: func() (any, error) {
				return client.DeleteMessage(context.Background(), &dynamomq.DeleteMessageInput{
					ID: "A-101",
				})
			},
		},
		{
			name: "GetQueueStats should return DynamoDBAPIError",
			operation: func() (any, error) {
				return client.GetQueueStats(context.Background(), &dynamomq.GetQueueStatsInput{})
			},
		},
		{
			name: "GetDLQStats should return DynamoDBAPIError",
			operation: func() (any, error) {
				return client.GetDLQStats(context.Background(), &dynamomq.GetDLQStatsInput{})
			},
		},
		{
			name: "GetMessage should return DynamoDBAPIError",
			operation: func() (any, error) {
				return client.GetMessage(context.Background(), &dynamomq.GetMessageInput{
					ID: "A-101",
				})
			},
		},
		{
			name: "ListMessages should return DynamoDBAPIError",
			operation: func() (any, error) {
				return client.ListMessages(context.Background(), &dynamomq.ListMessagesInput{
					Size: dynamomq.DefaultMaxListMessages,
				})
			},
		},
	}
	for _, tt := range tests {
		_, opeErr := tt.operation()
		if _, ok := assertErrorType[dynamomq.DynamoDBAPIError](opeErr); !ok {
			t.Errorf("error = %v, want %v", "DynamoDBAPIError", reflect.TypeOf(opeErr))
		}
	}
}

func TestTestDynamoMQClientReturnBuildingExpressionError(t *testing.T) {
	t.Parallel()
	buildExpression := func(b expression.Builder) (expression.Expression, error) {
		return expression.Expression{}, dynamomq.BuildingExpressionError{Cause: test.ErrTest}
	}
	client, err := dynamomq.NewFromConfig[test.MessageData](aws.Config{}, WithBuildExpression(buildExpression))
	if err != nil {
		t.Fatalf("failed to create DynamoMQ client: %s\n", err)
	}
	type testCase struct {
		name      string
		operation func() (any, error)
	}
	tests := []testCase{
		{
			name: "ReceiveMessage should return BuildingExpressionError",
			operation: func() (any, error) {
				return client.ReceiveMessage(context.Background(), &dynamomq.ReceiveMessageInput{})
			},
		},
		{
			name: "GetQueueStats should return BuildingExpressionError",
			operation: func() (any, error) {
				return client.GetQueueStats(context.Background(), &dynamomq.GetQueueStatsInput{})
			},
		},
		{
			name: "GetDLQStats should return BuildingExpressionError",
			operation: func() (any, error) {
				return client.GetDLQStats(context.Background(), &dynamomq.GetDLQStatsInput{})
			},
		},
	}
	for _, tt := range tests {
		_, opeErr := tt.operation()
		if _, ok := assertErrorType[dynamomq.BuildingExpressionError](opeErr); !ok {
			t.Errorf("error = %v, want %v", "BuildingExpressionError", reflect.TypeOf(opeErr))
		}
	}
}

func assertErrorType[T error](err error) (T, bool) {
	var wantErr T
	if errors.As(err, &wantErr) {
		return wantErr, true
	}
	return wantErr, false
}

func WithUnmarshalMap(f func(m map[string]types.AttributeValue, out interface{}) error) func(s *dynamomq.ClientOptions) {
	return func(s *dynamomq.ClientOptions) {
		if f != nil {
			s.UnmarshalMap = f
		}
	}
}

func WithUnmarshalListOfMaps(f func(l []map[string]types.AttributeValue, out interface{}) error) func(s *dynamomq.ClientOptions) {
	return func(s *dynamomq.ClientOptions) {
		if f != nil {
			s.UnmarshalListOfMaps = f
		}
	}
}

func WithMarshalMap(f func(in interface{}) (map[string]types.AttributeValue, error)) func(s *dynamomq.ClientOptions) {
	return func(s *dynamomq.ClientOptions) {
		if f != nil {
			s.MarshalMap = f
		}
	}
}

func WithBuildExpression(f func(b expression.Builder) (expression.Expression, error)) func(s *dynamomq.ClientOptions) {
	return func(s *dynamomq.ClientOptions) {
		if f != nil {
			s.BuildExpression = f
		}
	}
}

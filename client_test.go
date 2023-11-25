package dynamomq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	uuid "github.com/satori/go.uuid"
	"github.com/upsidr/dynamotest"
	. "github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/mock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func SetupDynamoDB(t *testing.T, initialData ...*types.PutRequest) (tableName string, client *dynamodb.Client, clean func()) {
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
	client, cancel := prepareTestClient(t, context.Background(),
		NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())), mock.Clock{}, false)
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
			wantError: &IDNotProvidedError{},
		},
		{
			name: "SendMessage should return IDDuplicatedError",
			operation: func() error {
				_, err := client.SendMessage(context.Background(), &SendMessageInput[test.MessageData]{
					ID:   "A-101",
					Data: test.NewMessageData("A-101"),
				})
				return err
			},
			wantError: &IDDuplicatedError{},
		},
		{
			name: "UpdateMessageAsVisible should return IDNotProvidedError",
			operation: func() error {
				_, err := client.UpdateMessageAsVisible(context.Background(), nil)
				return err
			},
			wantError: &IDNotProvidedError{},
		},
		{
			name: "UpdateMessageAsVisible should return IDNotFoundError",
			operation: func() error {
				_, err := client.UpdateMessageAsVisible(context.Background(), &UpdateMessageAsVisibleInput{
					ID: "B-101",
				})
				return err
			},
			wantError: &IDNotFoundError{},
		},
		{
			name: "MoveMessageToDLQ should return IDNotProvidedError",
			operation: func() error {
				_, err := client.MoveMessageToDLQ(context.Background(), nil)
				return err
			},
			wantError: &IDNotProvidedError{},
		},
		{
			name: "MoveMessageToDLQ should return IDNotFoundError",
			operation: func() error {
				_, err := client.MoveMessageToDLQ(context.Background(), &MoveMessageToDLQInput{
					ID: "B-101",
				})
				return err
			},
			wantError: &IDNotFoundError{},
		},
		{
			name: "RedriveMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.RedriveMessage(context.Background(), nil)
				return err
			},
			wantError: &IDNotProvidedError{},
		},
		{
			name: "RedriveMessage should return IDNotFoundError",
			operation: func() error {
				_, err := client.RedriveMessage(context.Background(), &RedriveMessageInput{
					ID: "B-101",
				})
				return err
			},
			wantError: &IDNotFoundError{},
		},
		{
			name: "DeleteMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.DeleteMessage(context.Background(), nil)
				return err
			},
			wantError: &IDNotProvidedError{},
		},
		{
			name: "GetMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.GetMessage(context.Background(), nil)
				return err
			},
			wantError: &IDNotProvidedError{},
		},
		{
			name: "ReplaceMessage should return IDNotProvidedError",
			operation: func() error {
				_, err := client.ReplaceMessage(context.Background(), nil)
				return err
			},
			wantError: &IDNotProvidedError{},
		},
	}
	for _, tt := range tests {
		test.AssertError(t, tt.operation(), tt.wantError, tt.name)
	}
}

func TestDynamoMQClientSendMessage(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[*SendMessageInput[test.MessageData], *SendMessageOutput[test.MessageData]]{
		{
			name:  "should succeed when id is not duplicated",
			setup: NewSetupFunc(),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate,
			},
			args: &SendMessageInput[test.MessageData]{
				ID:   "A-101",
				Data: test.NewMessageData("A-101"),
			},
			want: &SendMessageOutput[test.MessageData]{
				Result: &Result{
					ID:                   "A-101",
					Status:               StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339Nano(test.DefaultTestDate),
					Version:              1,
				},
				Message: func() *Message[test.MessageData] {
					s := NewTestMessageItemAsReady("A-101", test.DefaultTestDate)
					return s
				}(),
			},
		},
	}
	runTestsParallel[*SendMessageInput[test.MessageData], *SendMessageOutput[test.MessageData]](t, "SendMessage()", tests,
		func(client Client[test.MessageData], args *SendMessageInput[test.MessageData]) (*SendMessageOutput[test.MessageData], error) {
			return client.SendMessage(context.Background(), args)
		})
}

func TestDynamoMQClientReceiveMessage(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[any, *ReceiveMessageOutput[test.MessageData]]{
		{
			name: "should return EmptyQueueError when queue is empty",
			setup: NewSetupFunc(
				newPutRequestWithProcessingItem("A-202", clock.Now()),
				newPutRequestWithDLQItem("A-303", clock.Now()),
			),
			want:    nil,
			wantErr: &EmptyQueueError{},
		},
		{
			name:  "should return message when exists ready message",
			setup: NewSetupFunc(newPutRequestWithReadyItem("B-202", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(10 * time.Second),
			},
			want: func() *ReceiveMessageOutput[test.MessageData] {
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
			want: func() *ReceiveMessageOutput[test.MessageData] {
				m := NewTestMessageItemAsProcessing("B-202", test.DefaultTestDate)
				MarkAsProcessing(m, test.DefaultTestDate.Add(10*time.Minute).Add(1*time.Second))
				m.Version = 2
				m.ReceiveCount = 1
				r := &ReceiveMessageOutput[test.MessageData]{
					Result: &Result{
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
			wantErr: &EmptyQueueError{},
		},
	}
	runTestsParallel[any, *ReceiveMessageOutput[test.MessageData]](t, "ReceiveMessage()", tests,
		func(client Client[test.MessageData], _ any) (*ReceiveMessageOutput[test.MessageData], error) {
			return client.ReceiveMessage(context.Background(), &ReceiveMessageInput{})
		})
}

func testDynamoMQClientReceiveMessageSequence(t *testing.T, useFIFO bool) {
	now := test.DefaultTestDate.Add(10 * time.Second)
	ctx := context.Background()
	client, clean := prepareTestClient(t, ctx, NewSetupFunc(
		newPutRequestWithReadyItem("A-101", test.DefaultTestDate.Add(3*time.Second)),
		newPutRequestWithReadyItem("A-202", test.DefaultTestDate.Add(2*time.Second)),
		newPutRequestWithReadyItem("A-303", test.DefaultTestDate.Add(1*time.Second))),
		mock.Clock{
			T: now,
		}, useFIFO)
	defer clean()

	wants := []*ReceiveMessageOutput[test.MessageData]{
		NewMessageFromReadyToProcessing("A-303", test.DefaultTestDate.Add(1*time.Second), now),
		NewMessageFromReadyToProcessing("A-202", test.DefaultTestDate.Add(2*time.Second), now),
		NewMessageFromReadyToProcessing("A-101", test.DefaultTestDate.Add(3*time.Second), now),
	}

	for i, want := range wants {
		result, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
		test.AssertError(t, err, nil, fmt.Sprintf("ReceiveMessage() [%d-1]", i))
		test.AssertDeepEqual(t, result, want, fmt.Sprintf("ReceiveMessage() [%d-2]", i))

		if !useFIFO {
			return
		}

		_, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
		test.AssertError(t, err, &EmptyQueueError{}, fmt.Sprintf("ReceiveMessage() [%d-3]", i))

		_, err = client.DeleteMessage(ctx, &DeleteMessageInput{
			ID: result.ID,
		})
		test.AssertError(t, err, nil, fmt.Sprintf("DeleteMessage() [%d]", i))
	}

	_, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	test.AssertError(t, err, &EmptyQueueError{}, "ReceiveMessage() [last]")
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
	tests := []ClientTestCase[args, *UpdateMessageAsVisibleOutput[test.MessageData]]{
		{
			name:  "should succeed when id is found",
			setup: NewSetupFunc(newPutRequestWithProcessingItem("A-101", now)),
			sdkClock: mock.Clock{
				T: now,
			},
			args: args{
				id: "A-101",
			},
			want: &UpdateMessageAsVisibleOutput[test.MessageData]{
				Result: &Result{
					ID:                   "A-101",
					Status:               StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339Nano(now),
					Version:              2,
				},
				Message: func() *Message[test.MessageData] {
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
			want: &UpdateMessageAsVisibleOutput[test.MessageData]{},
			wantErr: InvalidStateTransitionError{
				Msg:       "message is currently ready",
				Operation: "mark as ready",
				Current:   StatusReady,
			},
		},
	}
	runTestsParallel[args, *UpdateMessageAsVisibleOutput[test.MessageData]](t, "UpdateMessageAsVisible()", tests,
		func(client Client[test.MessageData], args args) (*UpdateMessageAsVisibleOutput[test.MessageData], error) {
			return client.UpdateMessageAsVisible(context.Background(), &UpdateMessageAsVisibleInput{
				ID: args.id,
			})
		})
}

func TestDynamoMQClientDeleteMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []ClientTestCase[args, *DeleteMessageOutput]{
		{
			name:  "should not return error when not existing id",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())),
			args: args{
				id: "B-101",
			},
			want: &DeleteMessageOutput{},
		},
		{
			name:  "should succeed when id is found",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", clock.Now())),
			args: args{
				id: "A-101",
			},
			want: &DeleteMessageOutput{},
		},
	}
	runTestsParallel[args, *DeleteMessageOutput](t, "DeleteMessage()", tests,
		func(client Client[test.MessageData], args args) (*DeleteMessageOutput, error) {
			return client.DeleteMessage(context.Background(), &DeleteMessageInput{
				ID: args.id,
			})
		})
}

func TestDynamoMQClientMoveMessageToDLQ(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[*MoveMessageToDLQInput, *MoveMessageToDLQOutput]{
		{
			name:  "should succeed when id is found and queue type is standard",
			setup: NewSetupFunc(newPutRequestWithDLQItem("A-101", test.DefaultTestDate)),
			args: &MoveMessageToDLQInput{
				ID: "A-101",
			},
			want: func() *MoveMessageToDLQOutput {
				s := NewTestMessageItemAsDLQ("A-101", test.DefaultTestDate)
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
			name:  "should succeed when id is found and queue type is DLQ and status is processing",
			setup: NewSetupFunc(newPutRequestWithReadyItem("A-101", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(10 * time.Second),
			},
			args: &MoveMessageToDLQInput{
				ID: "A-101",
			},
			want: func() *MoveMessageToDLQOutput {
				m := NewTestMessageItemAsReady("A-101", test.DefaultTestDate)
				MarkAsMovedToDLQ(m, test.DefaultTestDate.Add(10*time.Second))
				m.Version = 2
				r := &MoveMessageToDLQOutput{
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
	runTestsParallel[*MoveMessageToDLQInput, *MoveMessageToDLQOutput](t, "MoveMessageToDLQ()", tests,
		func(client Client[test.MessageData], args *MoveMessageToDLQInput) (*MoveMessageToDLQOutput, error) {
			return client.MoveMessageToDLQ(context.Background(), args)
		})
}

func TestDynamoMQClientRedriveMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []ClientTestCase[args, *RedriveMessageOutput]{
		{
			name:  "should succeed when id is found and status is ready",
			setup: NewSetupFunc(newPutRequestWithDLQItem("A-101", test.DefaultTestDate)),
			sdkClock: mock.Clock{
				T: test.DefaultTestDate.Add(10 * time.Second),
			},
			args: args{
				id: "A-101",
			},
			want: &RedriveMessageOutput{
				ID:                   "A-101",
				Status:               StatusReady,
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
			want: &RedriveMessageOutput{},
			wantErr: InvalidStateTransitionError{
				Msg:       "can only redrive messages from DLQ",
				Operation: "mark as restored from DLQ",
				Current:   StatusReady,
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
			want: &RedriveMessageOutput{},
			wantErr: InvalidStateTransitionError{
				Msg:       "can only redrive messages from READY",
				Operation: "mark as restored from DLQ",
				Current:   StatusProcessing,
			},
		},
	}
	runTestsParallel[args, *RedriveMessageOutput](t, "RedriveMessage()", tests,
		func(client Client[test.MessageData], args args) (*RedriveMessageOutput, error) {
			return client.RedriveMessage(context.Background(), &RedriveMessageInput{
				ID: args.id,
			})
		})
}

func TestDynamoMQClientGetQueueStats(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[any, *GetQueueStatsOutput]{
		{
			name:  "should return empty items stats when no item in standard queue",
			setup: NewSetupFunc(),
			want: &GetQueueStatsOutput{
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
			want: &GetQueueStatsOutput{
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
			setup: NewSetupFunc(
				newPutRequestWithReadyItem("A-101", clock.Now()),
				newPutRequestWithReadyItem("B-202", clock.Now().Add(1*time.Second)),
				newPutRequestWithProcessingItem("C-303", clock.Now().Add(2*time.Second)),
				newPutRequestWithProcessingItem("D-404", clock.Now().Add(3*time.Second)),
			),
			want: &GetQueueStatsOutput{
				First100IDsInQueue:         []string{"A-101", "B-202", "C-303", "D-404"},
				First100SelectedIDsInQueue: []string{"C-303", "D-404"},
				TotalRecordsInQueue:        4,
				TotalRecordsInProcessing:   2,
				TotalRecordsNotStarted:     2,
			},
		},
	}
	runTestsParallel[any, *GetQueueStatsOutput](t, "GetQueueStats()", tests,
		func(client Client[test.MessageData], _ any) (*GetQueueStatsOutput, error) {
			return client.GetQueueStats(context.Background(), &GetQueueStatsInput{})
		})
}

func TestDynamoMQClientGetDLQStats(t *testing.T) {
	t.Parallel()
	tests := []ClientTestCase[any, *GetDLQStatsOutput]{
		{
			name: "should return empty items when no items in DLQ",
			setup: NewSetupFunc(
				newPutRequestWithReadyItem("A-101", clock.Now().Add(time.Second)),
				newPutRequestWithReadyItem("B-202", clock.Now().Add(time.Second)),
				newPutRequestWithProcessingItem("C-303", clock.Now().Add(2*time.Second)),
			),
			want: &GetDLQStatsOutput{
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
			want: &GetDLQStatsOutput{
				First100IDsInQueue: []string{"D-404", "E-505", "F-606"},
				TotalRecordsInDLQ:  3,
			},
		},
	}
	runTestsParallel[any, *GetDLQStatsOutput](t, "GetDLQStats()", tests,
		func(client Client[test.MessageData], _ any) (*GetDLQStatsOutput, error) {
			return client.GetDLQStats(context.Background(), &GetDLQStatsInput{})
		})
}

func TestDynamoMQClientGetMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		id string
	}
	tests := []ClientTestCase[args, *Message[test.MessageData]]{
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
	runTestsParallel[args, *Message[test.MessageData]](t, "GetMessage()", tests,
		func(client Client[test.MessageData], args args) (*Message[test.MessageData], error) {
			got, err := client.GetMessage(context.Background(), &GetMessageInput{
				ID: args.id,
			})
			return got.Message, err
		})
}

func TestDynamoMQClientReplaceMessage(t *testing.T) {
	t.Parallel()
	type args struct {
		message *Message[test.MessageData]
	}
	tests := []ClientTestCase[args, *Message[test.MessageData]]{
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
	runTestsParallel[args, *Message[test.MessageData]](t, "ReplaceMessage()", tests,
		func(client Client[test.MessageData], args args) (*Message[test.MessageData], error) {
			ctx := context.Background()
			_, err := client.ReplaceMessage(ctx, &ReplaceMessageInput[test.MessageData]{
				Message: args.message,
			})
			if err != nil {
				return nil, err
			}
			got, err := client.GetMessage(ctx, &GetMessageInput{
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
	tests := []ClientTestCase[args, []*Message[test.MessageData]]{
		{
			name:  "should return empty list when no messages",
			setup: NewSetupFunc(),
			args: args{
				size: 10,
			},
			want:    []*Message[test.MessageData]{},
			wantErr: nil,
		},
		{
			name: "should return list of messages when messages exist",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				messages := generateExpectedMessages("A", test.DefaultTestDate, 10)
				puts := generatePutRequests(messages)
				return SetupDynamoDB(t, puts...)
			},
			args: args{
				size: 10,
			},
			want: generateExpectedMessages("A",
				test.DefaultTestDate, 10),
			wantErr: nil,
		},
	}
	runTestsParallel[args, []*Message[test.MessageData]](t, "ListMessages()", tests,
		func(client Client[test.MessageData], args args) ([]*Message[test.MessageData], error) {
			out, err := client.ListMessages(context.Background(), &ListMessagesInput{Size: args.size})
			return out.Messages, err
		})
}

func runTestsParallel[Args any, Want any](t *testing.T, prefix string,
	tests []ClientTestCase[Args, Want], operation func(Client[test.MessageData], Args) (Want, error)) {
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client, clean := prepareTestClient(t, context.Background(), tt.setup, tt.sdkClock, false)
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

func prepareTestClient(t *testing.T, ctx context.Context,
	setupTable func(*testing.T) (string, *dynamodb.Client, func()),
	sdkClock clock.Clock,
	useFIFO bool,
) (Client[test.MessageData], func()) {
	t.Helper()
	tableName, raw, clean := setupTable(t)
	optFns := []func(*ClientOptions){
		WithTableName(tableName),
		WithQueueingIndexName(DefaultQueueingIndexName),
		WithAWSBaseEndpoint(""),
		WithAWSDynamoDBClient(raw),
		mock.WithClock(sdkClock),
		WithUseFIFO(useFIFO),
		WithAWSVisibilityTimeout(1),
		WithAWSRetryMaxAttempts(DefaultRetryMaxAttempts),
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

func generateExpectedMessages(idPrefix string, now time.Time, count int) []*Message[test.MessageData] {
	messages := make([]*Message[test.MessageData], count)
	for i := 0; i < count; i++ {
		now = now.Add(time.Minute)
		messages[i] = NewTestMessageItemAsReady(fmt.Sprintf("%s-%d", idPrefix, i), now)
	}
	return messages
}

func generatePutRequests(messages []*Message[test.MessageData]) []*types.PutRequest {
	var puts []*types.PutRequest
	for _, message := range messages {
		puts = append(puts, &types.PutRequest{
			Item: marshalMapUnsafe(message),
		})
	}
	return puts
}

func marshalMapUnsafe[T any](m *Message[T]) map[string]types.AttributeValue {
	item, _ := marshalMap(m)
	return item
}

func marshalMap[T any](m *Message[T]) (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(m)
	if err != nil {
		return nil, MarshalingAttributeError{Cause: err}
	}
	return item, nil
}

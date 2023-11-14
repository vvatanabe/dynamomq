package dynamomq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

type TestCase[Args any, Want any] struct {
	name     string
	setup    func(*testing.T) (string, *dynamodb.Client, func())
	sdkClock clock.Clock
	args     Args
	want     Want
	wantErr  error
}

func TestDynamoMQClientSendMessage(t *testing.T) {
	t.Parallel()
	tests := []TestCase[*SendMessageInput[test.MessageData], *SendMessageOutput[test.MessageData]]{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
	runTestsParallel[*SendMessageInput[test.MessageData], *SendMessageOutput[test.MessageData]](t, "SendMessage()", tests,
		func(client Client[test.MessageData], args *SendMessageInput[test.MessageData]) (*SendMessageOutput[test.MessageData], error) {
			return client.SendMessage(context.Background(), args)
		})
}

func TestDynamoMQClientReceiveMessage(t *testing.T) {
	t.Parallel()
	tests := []TestCase[any, *ReceiveMessageOutput[test.MessageData]]{
		{
			name: "should return EmptyQueueError when queue is empty",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					newPutRequestWithProcessingItem("A-202", clock.Now()),
					newPutRequestWithDLQItem("A-303", clock.Now()),
				)
			},
			want:    nil,
			wantErr: &EmptyQueueError{},
		},
		{
			name: "should return message when exists ready message",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					newPutRequestWithReadyItem("B-202", date(2023, 12, 1, 0, 0, 0)))
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
					newPutRequestWithProcessingItem("B-202",
						date(2023, 12, 1, 0, 0, 0)),
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
					newPutRequestWithProcessingItem("B-202",
						date(2023, 12, 1, 0, 0, 0)),
				)
			},
			sdkClock: mockClock{
				t: date(2023, 12, 1, 0, 0, 59),
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
	now := date(2023, 12, 1, 0, 0, 10)
	ctx := context.Background()
	client, clean := prepareTestClient(t, ctx, func(t *testing.T) (string, *dynamodb.Client, func()) {
		return setupDynamoDB(t,
			newPutRequestWithReadyItem("A-101", date(2023, 12, 1, 0, 0, 3)),
			newPutRequestWithReadyItem("A-202", date(2023, 12, 1, 0, 0, 2)),
			newPutRequestWithReadyItem("A-303", date(2023, 12, 1, 0, 0, 1)),
		)
	}, mockClock{
		t: now,
	}, useFIFO)
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
		assertDeepEqual(t, result, want, fmt.Sprintf("ReceiveMessage() [%d]", i))

		if !useFIFO {
			return
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
	tests := []TestCase[args, *UpdateMessageAsVisibleOutput[test.MessageData]]{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
					newPutRequestWithProcessingItem("A-101",
						date(2023, 12, 1, 0, 0, 10)),
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
	tests := []TestCase[args, *DeleteMessageOutput]{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
			},
			args: args{
				id: "",
			},
			wantErr: &IDNotProvidedError{},
		},
		{
			name: "should not return error when not existing id",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
			},
			args: args{
				id: "B-101",
			},
			want: &DeleteMessageOutput{},
		},
		{
			name: "should succeed when id is found",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
			},
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
	tests := []TestCase[*MoveMessageToDLQInput, *MoveMessageToDLQOutput]{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
					newPutRequestWithDLQItem("A-101", date(2023, 12, 1, 0, 0, 0)))
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
					newPutRequestWithProcessingItem("A-101",
						date(2023, 12, 1, 0, 0, 0)),
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
	tests := []TestCase[args, *RedriveMessageOutput]{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
					newPutRequestWithDLQItem("A-101", date(2023, 12, 1, 0, 0, 0)))
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
	runTestsParallel[args, *RedriveMessageOutput](t, "RedriveMessage()", tests,
		func(client Client[test.MessageData], args args) (*RedriveMessageOutput, error) {
			return client.RedriveMessage(context.Background(), &RedriveMessageInput{
				ID: args.id,
			})
		})
}

func TestDynamoMQClientGetQueueStats(t *testing.T) {
	t.Parallel()
	tests := []TestCase[any, *GetQueueStatsOutput]{
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
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
				return setupDynamoDB(t, newPutRequestWithProcessingItem("A-101", clock.Now()))
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
					newPutRequestWithReadyItem("A-101", clock.Now()),
					newPutRequestWithReadyItem("B-202", clock.Now().Add(1*time.Second)),
					newPutRequestWithProcessingItem("C-303", clock.Now().Add(2*time.Second)),
					newPutRequestWithProcessingItem("D-404", clock.Now().Add(3*time.Second)),
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
	runTestsParallel[any, *GetQueueStatsOutput](t, "GetQueueStats()", tests,
		func(client Client[test.MessageData], _ any) (*GetQueueStatsOutput, error) {
			return client.GetQueueStats(context.Background(), &GetQueueStatsInput{})
		})
}

func TestDynamoMQClientGetDLQStats(t *testing.T) {
	t.Parallel()
	tests := []TestCase[any, *GetDLQStatsOutput]{
		{
			name: "should return empty items when no items in DLQ",
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t,
					newPutRequestWithReadyItem("A-101", clock.Now().Add(time.Second)),
					newPutRequestWithReadyItem("B-202", clock.Now().Add(time.Second)),
					newPutRequestWithProcessingItem("C-303", clock.Now().Add(2*time.Second)),
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
					newPutRequestWithReadyItem("A-101", clock.Now().Add(time.Second)),
					newPutRequestWithReadyItem("B-202", clock.Now().Add(time.Second)),
					newPutRequestWithProcessingItem("C-303", clock.Now().Add(2*time.Second)),
					newPutRequestWithDLQItem("D-404", clock.Now().Add(3*time.Second)),
					newPutRequestWithDLQItem("E-505", clock.Now().Add(4*time.Second)),
					newPutRequestWithDLQItem("F-606", clock.Now().Add(5*time.Second)),
				)
			},
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
	tests := []TestCase[args, *Message[test.MessageData]]{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
					newPutRequestWithReadyItem("A-101", date(2023, 12, 1, 0, 0, 0)),
					newPutRequestWithReadyItem("B-202", clock.Now()),
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
	tests := []TestCase[args, *Message[test.MessageData]]{
		{
			name: testNameShouldReturnIDNotProvidedError,
			setup: func(t *testing.T) (string, *dynamodb.Client, func()) {
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
				return setupDynamoDB(t, newPutRequestWithReadyItem("A-101", clock.Now()))
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
	tests := []TestCase[args, []*Message[test.MessageData]]{
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
	runTestsParallel[args, []*Message[test.MessageData]](t, "ListMessages()", tests,
		func(client Client[test.MessageData], args args) ([]*Message[test.MessageData], error) {
			out, err := client.ListMessages(context.Background(), &ListMessagesInput{Size: args.size})
			return out.Messages, err
		})
}

func runTestsParallel[Args any, Want any](t *testing.T, prefix string,
	tests []TestCase[Args, Want], operation func(Client[test.MessageData], Args) (Want, error)) {
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client, clean := prepareTestClient(t, context.Background(), tt.setup, tt.sdkClock, false)
			defer clean()
			result, err := operation(client, tt.args)
			err = assertError(t, err, tt.wantErr, prefix)
			if err != nil || tt.wantErr != nil {
				return
			}
			assertDeepEqual(t, result, tt.want, prefix)
		})
	}
}

func assertError(t *testing.T, got, want error, prefix string) error {
	t.Helper()
	if want != nil {
		if !errors.Is(got, want) {
			t.Errorf("%s error = %v, want %v", prefix, got, want)
			return got
		}
		return nil
	}
	if got != nil {
		t.Errorf("%s unexpected error = %v", prefix, got)
		return got
	}
	return nil
}

func assertDeepEqual(t *testing.T, got, want any, prefix string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		v1, _ := json.Marshal(got)
		v2, _ := json.Marshal(want)
		t.Errorf("%s got = %v, want %v", prefix, string(v1), string(v2))
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

func newPutRequestWithReadyItem(id string, now time.Time) *types.PutRequest {
	return &types.PutRequest{
		Item: newTestMessageItemAsReady(id, now).marshalMapUnsafe(),
	}
}

func newPutRequestWithProcessingItem(id string, now time.Time) *types.PutRequest {
	return &types.PutRequest{
		Item: newTestMessageItemAsProcessing(id, now).marshalMapUnsafe(),
	}
}

func newPutRequestWithDLQItem(id string, now time.Time) *types.PutRequest {
	return &types.PutRequest{
		Item: newTestMessageItemAsDLQ(id, now).marshalMapUnsafe(),
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

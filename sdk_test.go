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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/upsidr/dynamotest"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

type mockClock struct {
	t time.Time
}

func (m mockClock) Now() time.Time {
	return m.t
}

func withClock(clock clock.Clock) func(s *ClientOptions) {
	return func(s *ClientOptions) {
		if clock != nil {
			s.clock = clock
		}
	}
}

func setupDynamoDB(t *testing.T, initialData ...*types.PutRequest) (client *dynamodb.Client, clean func()) {
	client, clean = dynamotest.NewDynamoDB(t)
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
			TableName: aws.String(DefaultTableName),
		},
		InitialData: initialData,
	})
	return
}

func newTestMessageItemAsReady(id string, now time.Time) *Message[test.MessageData] {
	return NewDefaultMessage[test.MessageData](id, test.NewMessageData(id), now)
}

func newTestMessageItemAsPeeked(id string, now time.Time) *Message[test.MessageData] {
	message := NewDefaultMessage[test.MessageData](id, test.NewMessageData(id), now)
	err := message.StartProcessing(now, 0)
	if err != nil {
		panic(err)
	}
	return message
}

func newTestMessageItemAsDLQ(id string, now time.Time) *Message[test.MessageData] {
	message := NewDefaultMessage[test.MessageData](id, test.NewMessageData(id), now)
	err := message.MoveToDLQ(now)
	if err != nil {
		panic(err)
	}
	return message
}

func TestQueueSDKClientEnqueue(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     *SendMessageInput[test.MessageData]
		want     *SendMessageOutput[test.MessageData]
		wantErr  error
	}{
		{
			name: "should return IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "should return IDDuplicatedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "should enqueue succeeds",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: &SendMessageInput[test.MessageData]{
				ID:   "A-101",
				Data: test.NewMessageData("A-101"),
			},
			want: &SendMessageOutput[test.MessageData]{
				Result: &Result{
					ID:                   "A-101",
					Status:               StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)),
					Version:              1,
				},
				Message: func() *Message[test.MessageData] {
					s := newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
					return s
				}(),
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			result, err := client.SendMessage(ctx, tt.args)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("SendMessage() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("SendMessage() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("SendMessage() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientPeek(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		want     *ReceiveMessageOutput[test.MessageData]
		wantErr  error
	}{
		{
			name: "should return EmptyQueueError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("A-202", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("A-303", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			want:    nil,
			wantErr: &EmptyQueueError{},
		},
		{
			name: "should peek when not selected",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			want: func() *ReceiveMessageOutput[test.MessageData] {
				s := newTestMessageItemAsReady("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				err := s.StartProcessing(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC), 0)
				if err != nil {
					panic(err)
				}
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
			name: "should peek when visibility timeout has expired",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("B-202",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 1, 1, 0, time.UTC),
			},
			want: func() *ReceiveMessageOutput[test.MessageData] {
				s := newTestMessageItemAsPeeked("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				err := s.StartProcessing(time.Date(2023, 12, 1, 0, 1, 1, 0, time.UTC), 0)
				if err != nil {
					panic(err)
				}
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
			name: "can not peek when visibility timeout",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("B-202",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 59, 0, time.UTC),
			},
			want:    nil,
			wantErr: &EmptyQueueError{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			result, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("ReceiveMessage() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("ReceiveMessage() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("ReceiveMessage() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientPeekUseFIFO(t *testing.T) {
	raw, clean := setupDynamoDB(t,
		&types.PutRequest{
			Item: newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 3, 0, time.UTC)).MarshalMapUnsafe(),
		},
		&types.PutRequest{
			Item: newTestMessageItemAsReady("A-202", time.Date(2023, 12, 1, 0, 0, 2, 0, time.UTC)).MarshalMapUnsafe(),
		},
		&types.PutRequest{
			Item: newTestMessageItemAsReady("A-303", time.Date(2023, 12, 1, 0, 0, 1, 0, time.UTC)).MarshalMapUnsafe(),
		},
	)
	defer clean()

	now := time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)

	ctx := context.Background()
	client, err := NewFromConfig[test.MessageData](ctx,
		WithAWSDynamoDBClient(raw),
		withClock(mockClock{
			t: now,
		}),
		WithAWSVisibilityTimeout(1),
		WithUseFIFO(true))
	if err != nil {
		t.Fatalf("NewFromConfig() error = %v", err)
		return
	}

	want1 := func() *ReceiveMessageOutput[test.MessageData] {
		s := newTestMessageItemAsReady("A-303", time.Date(2023, 12, 1, 0, 0, 1, 0, time.UTC))
		err := s.StartProcessing(now, 0)
		if err != nil {
			panic(err)
		}
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
	}()

	result, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if err != nil {
		t.Errorf("ReceiveMessage() 1 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want1) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want1)
		t.Errorf("ReceiveMessage() 1 got = %v, want %v", string(v1), string(v2))
	}
	_, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("ReceiveMessage() 2 error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
	_, err = client.DeleteMessage(ctx, &DeleteMessageInput{
		ID: result.ID,
	})
	if err != nil {
		t.Errorf("Done() 1 error = %v", err)
		return
	}

	want2 := func() *ReceiveMessageOutput[test.MessageData] {
		s := newTestMessageItemAsReady("A-202", time.Date(2023, 12, 1, 0, 0, 2, 0, time.UTC))
		err := s.StartProcessing(now, 0)
		if err != nil {
			panic(err)
		}
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
	}()

	result, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if err != nil {
		t.Errorf("ReceiveMessage() 3 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want2) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want2)
		t.Errorf("ReceiveMessage() 3 got = %v, want %v", string(v1), string(v2))
	}

	_, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("ReceiveMessage() 4 error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
	_, err = client.DeleteMessage(ctx, &DeleteMessageInput{
		ID: result.ID,
	})
	if err != nil {
		t.Errorf("Done() 2 error = %v", err)
		return
	}

	want3 := func() *ReceiveMessageOutput[test.MessageData] {
		s := newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 3, 0, time.UTC))
		err := s.StartProcessing(now, 0)
		if err != nil {
			panic(err)
		}
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
	}()

	result, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if err != nil {
		t.Errorf("ReceiveMessage() 5 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want3) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want3)
		t.Errorf("ReceiveMessage() 5 got = %v, want %v", string(v1), string(v2))
	}

	_, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("ReceiveMessage() 6 error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
	_, err = client.DeleteMessage(ctx, &DeleteMessageInput{
		ID: result.ID,
	})
	if err != nil {
		t.Errorf("Done() 3 error = %v", err)
		return
	}
	_, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("ReceiveMessage() 7 error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
}

func TestQueueSDKClientPeekNotUseFIFO(t *testing.T) {
	raw, clean := setupDynamoDB(t,
		&types.PutRequest{
			Item: newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 3, 0, time.UTC)).MarshalMapUnsafe(),
		},
		&types.PutRequest{
			Item: newTestMessageItemAsReady("A-202", time.Date(2023, 12, 1, 0, 0, 2, 0, time.UTC)).MarshalMapUnsafe(),
		},
		&types.PutRequest{
			Item: newTestMessageItemAsReady("A-303", time.Date(2023, 12, 1, 0, 0, 1, 0, time.UTC)).MarshalMapUnsafe(),
		},
	)
	defer clean()

	now := time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)

	ctx := context.Background()
	client, err := NewFromConfig[test.MessageData](ctx,
		WithAWSDynamoDBClient(raw),
		withClock(mockClock{
			t: now,
		}),
		WithAWSVisibilityTimeout(1))
	if err != nil {
		t.Fatalf("NewFromConfig() error = %v", err)
		return
	}

	want1 := func() *ReceiveMessageOutput[test.MessageData] {
		s := newTestMessageItemAsReady("A-303", time.Date(2023, 12, 1, 0, 0, 1, 0, time.UTC))
		err := s.StartProcessing(now, 0)
		if err != nil {
			panic(err)
		}
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
	}()

	result, err := client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if err != nil {
		t.Errorf("ReceiveMessage() 1 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want1) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want1)
		t.Errorf("ReceiveMessage() 1 got = %v, want %v", string(v1), string(v2))
	}

	want2 := func() *ReceiveMessageOutput[test.MessageData] {
		s := newTestMessageItemAsReady("A-202", time.Date(2023, 12, 1, 0, 0, 2, 0, time.UTC))
		err := s.StartProcessing(now, 0)
		if err != nil {
			panic(err)
		}
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
	}()

	result, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if err != nil {
		t.Errorf("ReceiveMessage() 2 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want2) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want2)
		t.Errorf("ReceiveMessage() 2 got = %v, want %v", string(v1), string(v2))
	}

	want3 := func() *ReceiveMessageOutput[test.MessageData] {
		s := newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 3, 0, time.UTC))
		err := s.StartProcessing(now, 0)
		if err != nil {
			panic(err)
		}
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
	}()

	result, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if err != nil {
		t.Errorf("ReceiveMessage() 3 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want3) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want3)
		t.Errorf("ReceiveMessage() 3 got = %v, want %v", string(v1), string(v2))
	}

	_, err = client.ReceiveMessage(ctx, &ReceiveMessageInput{})
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("ReceiveMessage() 4 error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
}

func TestQueueSDKClientRetry(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *UpdateMessageAsVisibleOutput[test.MessageData]
		wantErr  error
	}{
		{
			name: "should return IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "should return IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "existing id do not return error",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("A-101", time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)).MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				id: "A-101",
			},
			want: &UpdateMessageAsVisibleOutput[test.MessageData]{
				Result: &Result{
					ID:                   "A-101",
					Status:               StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)),
					Version:              2,
				},
				Message: func() *Message[test.MessageData] {
					message := newTestMessageItemAsPeeked("A-101", time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
					err := message.Ready(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
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
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			result, err := client.UpdateMessageAsVisible(ctx, &UpdateMessageAsVisibleInput{
				ID: tt.args.id,
			})
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("UpdateMessageAsVisible() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("UpdateMessageAsVisible() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("UpdateMessageAsVisible() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientDelete(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     error
	}{
		{
			name: "should return IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "",
			},
			want: &IDNotProvidedError{},
		},
		{
			name: "not exist id does not return error",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "B-101",
			},
			want: nil,
		},
		{
			name: "existing id do not return error",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			_, err = client.DeleteMessage(ctx, &DeleteMessageInput{
				ID: tt.args.id,
			})
			if err != tt.want {
				t.Errorf("DeleteMessage() error = %v, wantErr %v", err, tt.want)
				return
			}
		})
	}
}

func TestQueueSDKClientSendToDLQ(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     *MoveMessageToDLQInput
		want     *MoveMessageToDLQOutput
		wantErr  error
	}{
		{
			name: "should return IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "should return IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "should send to DLQ succeeds when already exists DLQ",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("A-101",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			args: &MoveMessageToDLQInput{
				ID: "A-101",
			},
			want: func() *MoveMessageToDLQOutput {
				s := newTestMessageItemAsDLQ("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
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
			name: "should send to DLQ succeeds",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("A-101",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: &MoveMessageToDLQInput{
				ID: "A-101",
			},
			want: func() *MoveMessageToDLQOutput {
				s := newTestMessageItemAsReady("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				err := s.MoveToDLQ(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
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
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			result, err := client.MoveMessageToDLQ(ctx, tt.args)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("MoveMessageToDLQ() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("MoveMessageToDLQ() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("MoveMessageToDLQ() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientRedrive(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *RedriveMessageOutput
		wantErr  error
	}{
		{
			name: "should return IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "should return IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "should redrive succeeds",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				id: "A-101",
			},
			want: &RedriveMessageOutput{
				ID:                   "A-101",
				Status:               StatusReady,
				LastUpdatedTimestamp: clock.FormatRFC3339(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)),
				Version:              2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			result, err := client.RedriveMessage(ctx, &RedriveMessageInput{
				ID: tt.args.id,
			})
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("RedriveMessage() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("RedriveMessage() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("RedriveMessage() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientGetQueueStats(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) (*dynamodb.Client, func())
		want  *GetQueueStatsOutput
	}{
		{
			name: "empty items",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
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
			name: "has one item in enqueued",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "has one item in peeked",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "has two item in enqueued or peeked",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202", clock.Now().Add(1*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("C-303", clock.Now().Add(2*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("D-404", clock.Now().Add(3*time.Second)).MarshalMapUnsafe(),
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
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			got, err := client.GetQueueStats(ctx, &GetQueueStatsInput{})
			if err != nil {
				t.Errorf("GetQueueStats() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetQueueStats() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueueSDKClientGetDLQStats(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) (*dynamodb.Client, func())
		want  *GetDLQStatsOutput
	}{
		{
			name: "empty items",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now().Add(time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202", clock.Now().Add(1*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("C-303", clock.Now().Add(2*time.Second)).MarshalMapUnsafe(),
					},
				)
			},
			want: &GetDLQStatsOutput{
				First100IDsInQueue: []string{},
				TotalRecordsInDLQ:  0,
			},
		},
		{
			name: "has three items in DLQ",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202", clock.Now().Add(1*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsPeeked("C-303", clock.Now().Add(2*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("D-404", clock.Now().Add(3*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("E-505", clock.Now().Add(4*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsDLQ("F-606", clock.Now().Add(5*time.Second)).MarshalMapUnsafe(),
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
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			got, err := client.GetDLQStats(ctx, &GetDLQStatsInput{})
			if err != nil {
				t.Errorf("GetDLQStats() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDLQStats() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueueSDKClientGet(t *testing.T) {

	type args struct {
		id string
	}
	tests := []struct {
		name    string
		setup   func(*testing.T) (*dynamodb.Client, func())
		args    args
		want    *Message[test.MessageData]
		wantErr error
	}{
		{
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "nil",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "get a message",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestMessageItemAsReady("B-202", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want:    newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			got, err := client.GetMessage(ctx, &GetMessageInput{
				ID: tt.args.id,
			})
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("GetMessage() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if !reflect.DeepEqual(got.Message, tt.want) {
				t.Errorf("GetMessage() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueueSDKClientPut(t *testing.T) {

	type args struct {
		message *Message[test.MessageData]
	}
	tests := []struct {
		name    string
		setup   func(*testing.T) (*dynamodb.Client, func())
		args    args
		want    *Message[test.MessageData]
		wantErr error
	}{
		{
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "duplicated id",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				message: newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			},
			want:    newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			wantErr: nil,
		},
		{
			name: "unique id",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				message: newTestMessageItemAsReady("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			},
			want:    newTestMessageItemAsReady("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			err = client.Put(ctx, tt.args.message)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Put() error = %v", err)
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

func TestQueueSDKClientTouch(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *Result
		wantErr  error
	}{
		{
			name: "should return IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "should return IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "touch",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestMessageItemAsReady("A-101",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				id: "A-101",
			},
			want: func() *Result {
				s := newTestMessageItemAsReady("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.Touch(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.Version = 2
				r := &Result{
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
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			result, err := client.Touch(ctx, tt.args.id)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Touch() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Touch() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("Touch() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientList(t *testing.T) {
	type args struct {
		size int32
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     []*Message[test.MessageData]
		wantErr  error
	}{
		{
			name: "empty",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t)
			},
			args: args{
				size: 10,
			},
			want:    []*Message[test.MessageData]{},
			wantErr: nil,
		},
		{
			name: "list",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				var puts []*types.PutRequest
				for i := 0; i < 10; i++ {
					puts = append(puts, &types.PutRequest{
						Item: newTestMessageItemAsReady(fmt.Sprintf("A-%d", i),
							time.Date(2023, 12, 1, 0, 0, i, 0, time.UTC)).
							MarshalMapUnsafe(),
					})
				}
				return setupDynamoDB(t, puts...)
			},
			args: args{
				size: 10,
			},
			want: func() []*Message[test.MessageData] {
				var messages []*Message[test.MessageData]
				for i := 0; i < 10; i++ {
					messages = append(messages, newTestMessageItemAsReady(fmt.Sprintf("A-%d", i),
						time.Date(2023, 12, 1, 0, 0, i, 0, time.UTC)))
				}
				return messages
			}(),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewFromConfig[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewFromConfig() error = %v", err)
				return
			}
			result, err := client.ListMessages(ctx, &ListMessagesInput{
				Size: tt.args.size,
			})
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("ListMessages() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("ListMessages() error = %v", err)
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

package sdk

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

func withClock(clock clock.Clock) Option {
	return func(s *options) {
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
	message.MarkAsPeeked(now)
	return message
}

func newTestMessageItemAsDLQ(id string, now time.Time) *Message[test.MessageData] {
	message := NewDefaultMessage[test.MessageData](id, test.NewMessageData(id), now)
	message.MarkAsDLQ(now)
	return message
}

func TestQueueSDKClientEnqueue(t *testing.T) {
	type args struct {
		id   string
		data *test.MessageData
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *EnqueueResult[test.MessageData]
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
				id:   "",
				data: nil,
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
			args: args{
				id:   "A-101",
				data: test.NewMessageData("A-101"),
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
			args: args{
				id:   "A-101",
				data: test.NewMessageData("A-101"),
			},
			want: &EnqueueResult[test.MessageData]{
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.Enqueue(ctx, tt.args.id, tt.args.data)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Enqueue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Enqueue() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("Enqueue() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientPeek(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		want     *PeekResult[test.MessageData]
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
			want: func() *PeekResult[test.MessageData] {
				s := newTestMessageItemAsReady("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsPeeked(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.Version = 2
				s.ReceiveCount = 1
				r := &PeekResult[test.MessageData]{
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
			want: func() *PeekResult[test.MessageData] {
				s := newTestMessageItemAsPeeked("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsPeeked(time.Date(2023, 12, 1, 0, 1, 1, 0, time.UTC))
				s.Version = 2
				s.ReceiveCount = 1
				r := &PeekResult[test.MessageData]{
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.Peek(ctx)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Peek() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Peek() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("Peek() got = %v, want %v", result, tt.want)
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
	client, err := NewQueueSDKClient[test.MessageData](ctx,
		WithAWSDynamoDBClient(raw),
		withClock(mockClock{
			t: now,
		}),
		WithAWSVisibilityTimeout(1),
		WithUseFIFO(true))
	if err != nil {
		t.Fatalf("NewQueueSDKClient() error = %v", err)
		return
	}

	want1 := func() *PeekResult[test.MessageData] {
		s := newTestMessageItemAsReady("A-303", time.Date(2023, 12, 1, 0, 0, 1, 0, time.UTC))
		s.MarkAsPeeked(now)
		s.Version = 2
		s.ReceiveCount = 1
		r := &PeekResult[test.MessageData]{
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

	result, err := client.Peek(ctx)
	if err != nil {
		t.Errorf("Peek() 1 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want1) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want1)
		t.Errorf("Peek() 1 got = %v, want %v", string(v1), string(v2))
	}
	_, err = client.Peek(ctx)
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("Peek() 2 error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
	err = client.Delete(ctx, result.ID)
	if err != nil {
		t.Errorf("Done() 1 error = %v", err)
		return
	}

	want2 := func() *PeekResult[test.MessageData] {
		s := newTestMessageItemAsReady("A-202", time.Date(2023, 12, 1, 0, 0, 2, 0, time.UTC))
		s.MarkAsPeeked(now)
		s.Version = 2
		s.ReceiveCount = 1
		r := &PeekResult[test.MessageData]{
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

	result, err = client.Peek(ctx)
	if err != nil {
		t.Errorf("Peek() 3 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want2) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want2)
		t.Errorf("Peek() 3 got = %v, want %v", string(v1), string(v2))
	}

	_, err = client.Peek(ctx)
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("Peek() 4 error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
	err = client.Delete(ctx, result.ID)
	if err != nil {
		t.Errorf("Done() 2 error = %v", err)
		return
	}

	want3 := func() *PeekResult[test.MessageData] {
		s := newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 3, 0, time.UTC))
		s.MarkAsPeeked(now)
		s.Version = 2
		s.ReceiveCount = 1
		r := &PeekResult[test.MessageData]{
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

	result, err = client.Peek(ctx)
	if err != nil {
		t.Errorf("Peek() 5 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want3) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want3)
		t.Errorf("Peek() 5 got = %v, want %v", string(v1), string(v2))
	}

	_, err = client.Peek(ctx)
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("Peek() 6 error = %v, wantErr %v", err, &EmptyQueueError{})
		return
	}
	err = client.Delete(ctx, result.ID)
	if err != nil {
		t.Errorf("Done() 3 error = %v", err)
		return
	}
	_, err = client.Peek(ctx)
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("Peek() 7 error = %v, wantErr %v", err, &EmptyQueueError{})
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
	client, err := NewQueueSDKClient[test.MessageData](ctx,
		WithAWSDynamoDBClient(raw),
		withClock(mockClock{
			t: now,
		}),
		WithAWSVisibilityTimeout(1))
	if err != nil {
		t.Fatalf("NewQueueSDKClient() error = %v", err)
		return
	}

	want1 := func() *PeekResult[test.MessageData] {
		s := newTestMessageItemAsReady("A-303", time.Date(2023, 12, 1, 0, 0, 1, 0, time.UTC))
		s.MarkAsPeeked(now)
		s.Version = 2
		s.ReceiveCount = 1
		r := &PeekResult[test.MessageData]{
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

	result, err := client.Peek(ctx)
	if err != nil {
		t.Errorf("Peek() 1 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want1) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want1)
		t.Errorf("Peek() 1 got = %v, want %v", string(v1), string(v2))
	}

	want2 := func() *PeekResult[test.MessageData] {
		s := newTestMessageItemAsReady("A-202", time.Date(2023, 12, 1, 0, 0, 2, 0, time.UTC))
		s.MarkAsPeeked(now)
		s.Version = 2
		s.ReceiveCount = 1
		r := &PeekResult[test.MessageData]{
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

	result, err = client.Peek(ctx)
	if err != nil {
		t.Errorf("Peek() 2 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want2) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want2)
		t.Errorf("Peek() 2 got = %v, want %v", string(v1), string(v2))
	}

	want3 := func() *PeekResult[test.MessageData] {
		s := newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 3, 0, time.UTC))
		s.MarkAsPeeked(now)
		s.Version = 2
		s.ReceiveCount = 1
		r := &PeekResult[test.MessageData]{
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

	result, err = client.Peek(ctx)
	if err != nil {
		t.Errorf("Peek() 3 error = %v", err)
		return
	}
	if !reflect.DeepEqual(result, want3) {
		v1, _ := json.Marshal(result)
		v2, _ := json.Marshal(want3)
		t.Errorf("Peek() 3 got = %v, want %v", string(v1), string(v2))
	}

	_, err = client.Peek(ctx)
	if !errors.Is(err, &EmptyQueueError{}) {
		t.Errorf("Peek() 4 error = %v, wantErr %v", err, &EmptyQueueError{})
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
		want     *RetryResult[test.MessageData]
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
						Item: newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)).MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				id: "A-101",
			},
			want: &RetryResult[test.MessageData]{
				Result: &Result{
					ID:                   "A-101",
					Status:               StatusReady,
					LastUpdatedTimestamp: clock.FormatRFC3339(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)),
					Version:              2,
				},
				Message: func() *Message[test.MessageData] {
					message := newTestMessageItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
					message.MarkAsRetry(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.Retry(ctx, tt.args.id)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Retry() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Retry() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("Retry() got = %v, want %v", result, tt.want)
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			err = client.Delete(ctx, tt.args.id)
			if err != tt.want {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.want)
				return
			}
		})
	}
}

func TestQueueSDKClientSendToDLQ(t *testing.T) {
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
			args: args{
				id: "A-101",
			},
			want: func() *Result {
				s := newTestMessageItemAsDLQ("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
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
			args: args{
				id: "A-101",
			},
			want: func() *Result {
				s := newTestMessageItemAsDLQ("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsDLQ(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.SendToDLQ(ctx, tt.args.id)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("SendToDLQ() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("SendToDLQ() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("SendToDLQ() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientRedrive(t *testing.T) {
	// FIXME
}

func TestQueueSDKClientGetQueueStats(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) (*dynamodb.Client, func())
		want  *QueueStats
	}{
		{
			name: "empty items",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t)
			},
			want: &QueueStats{
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
			want: &QueueStats{
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
			want: &QueueStats{
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
			want: &QueueStats{
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			got, err := client.GetQueueStats(ctx)
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
		want  *DLQStats
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
			want: &DLQStats{
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
			want: &DLQStats{
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			got, err := client.GetDLQStats(ctx)
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			got, err := client.Get(ctx, tt.args.id)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
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
			message, err := client.Get(ctx, tt.args.message.ID)
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			if !reflect.DeepEqual(message, tt.want) {
				t.Errorf("Get() got = %v, want %v", message, tt.want)
			}
		})
	}
}

func TestQueueSDKClientUpsert(t *testing.T) {
	type args struct {
		message *Message[test.MessageData]
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *Message[test.MessageData]
		wantErr  error
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
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				message: newTestMessageItemAsPeeked("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			},
			want: func() *Message[test.MessageData] {
				s := newTestMessageItemAsPeeked("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.Update(s, time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				return s
			}(),
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			err = client.Upsert(ctx, tt.args.message)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Upsert() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Upsert() error = %v", err)
				return
			}
			message, err := client.Get(ctx, tt.args.message.ID)
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			if !reflect.DeepEqual(message, tt.want) {
				t.Errorf("Get() got = %v, want %v", message, tt.want)
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
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
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.List(ctx, tt.args.size)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("List() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("List() error = %v", err)
				return
			}
			sort.Slice(result, func(i, j int) bool {
				return result[i].LastUpdatedTimestamp < result[j].LastUpdatedTimestamp
			})
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("List() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientListIDs(t *testing.T) {
	type args struct {
		size int32
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     []string
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
			want:    []string{},
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
			want: func() []string {
				var ids []string
				for i := 0; i < 10; i++ {
					ids = append(ids, fmt.Sprintf("A-%d", i))
				}
				return ids
			}(),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.ListIDs(ctx, tt.args.size)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("ListIDs() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("ListIDs() error = %v", err)
				return
			}
			sort.Slice(result, func(i, j int) bool {
				return result[i] < result[j]
			})
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("ListIDs() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientListExtendedIDs(t *testing.T) {
	type args struct {
		size int32
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     []string
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
			want:    []string{},
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
			want: func() []string {
				var ids []string
				for i := 0; i < 10; i++ {
					item := newTestMessageItemAsReady(fmt.Sprintf("A-%d", i),
						time.Date(2023, 12, 1, 0, 0, i, 0, time.UTC))
					ids = append(ids, fmt.Sprintf("ID: %s, status: %s", item.ID, item.Status))
				}
				return ids
			}(),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient[test.MessageData](ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.ListExtendedIDs(ctx, tt.args.size)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("ListExtendedIDs() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("ListExtendedIDs() error = %v", err)
				return
			}
			sort.Slice(result, func(i, j int) bool {
				return result[i] < result[j]
			})
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("ListExtendedIDs() got = %v, want %v", result, tt.want)
			}
		})
	}
}

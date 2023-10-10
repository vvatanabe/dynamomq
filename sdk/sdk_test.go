package sdk

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/upsidr/dynamotest"
	"github.com/vvatanabe/go82f46979/internal/clock"
)

type mockClock struct {
	t time.Time
}

func (m mockClock) Now() time.Time {
	return m.t
}

func withClock(clock clock.Clock) Option {
	return func(s *queueSDKClient) {
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
					AttributeName: aws.String("DLQ"),
					AttributeType: types.ScalarAttributeTypeN,
				},
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("last_updated_timestamp"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("queued"),
					AttributeType: types.ScalarAttributeTypeN,
				},
			},
			BillingMode:               types.BillingModePayPerRequest,
			DeletionProtectionEnabled: aws.Bool(false),
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String("dlq-last_updated_timestamp-index"),
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("DLQ"),
							KeyType:       types.KeyTypeHash,
						},
						{
							AttributeName: aws.String("last_updated_timestamp"),
							KeyType:       types.KeyTypeRange,
						},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
				},
				{
					IndexName: aws.String("queueud-last_updated_timestamp-index"),
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("queued"),
							KeyType:       types.KeyTypeHash,
						},
						{
							AttributeName: aws.String("last_updated_timestamp"),
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
			TableName: aws.String("Shipment"),
		},
		InitialData: initialData,
	})
	return
}

func newTestShipmentItem(id string, now time.Time) *Shipment {
	return NewDefaultShipment(id, newTestShipmentData(id), now)
}

func newTestShipmentItemAsReady(id string, now time.Time) *Shipment {
	shipment := NewDefaultShipment(id, newTestShipmentData(id), now)
	shipment.MarkAsReadyForShipment(now)
	return shipment
}

func newTestShipmentItemAsEnqueued(id string, now time.Time) *Shipment {
	shipment := NewDefaultShipment(id, newTestShipmentData(id), now)
	shipment.MarkAsEnqueued(now)
	return shipment
}

func newTestShipmentItemAsPeeked(id string, now time.Time) *Shipment {
	shipment := NewDefaultShipment(id, newTestShipmentData(id), now)
	shipment.MarkAsPeeked(now)
	return shipment
}

func newTestShipmentItemAsRemoved(id string, now time.Time) *Shipment {
	shipment := NewDefaultShipment(id, newTestShipmentData(id), now)
	shipment.MarkAsRemoved(now)
	return shipment
}

func newTestShipmentItemAsDLQ(id string, now time.Time) *Shipment {
	shipment := NewDefaultShipment(id, newTestShipmentData(id), now)
	shipment.MarkAsDLQ(now)
	return shipment
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
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
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
						Item: newTestShipmentItemAsEnqueued("A-101", clock.Now()).MarshalMapUnsafe(),
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
						Item: newTestShipmentItemAsPeeked("A-101", clock.Now()).MarshalMapUnsafe(),
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
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsEnqueued("B-202", clock.Now().Add(1*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("C-303", clock.Now().Add(2*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("D-404", clock.Now().Add(3*time.Second)).MarshalMapUnsafe(),
					},
				)
			},
			want: &QueueStats{
				First100IDsInQueue:         []string{"B-202", "C-303", "D-404"},
				First100SelectedIDsInQueue: []string{"C-303", "D-404"},
				TotalRecordsInQueue:        3,
				TotalRecordsInProcessing:   2,
				TotalRecordsNotStarted:     1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw))
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
						Item: newTestShipmentItem("A-101", clock.Now().Add(time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsEnqueued("B-202", clock.Now().Add(1*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("C-303", clock.Now().Add(2*time.Second)).MarshalMapUnsafe(),
					},
				)
			},
			want: &DLQStats{
				First100IDsInQueue: []string{},
				TotalRecordsInDLQ:  0,
			},
		},
		{
			name: "has two items in DLQ",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsEnqueued("B-202", clock.Now().Add(1*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("C-303", clock.Now().Add(2*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsDLQ("D-404", clock.Now().Add(3*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsDLQ("E-505", clock.Now().Add(4*time.Second)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsDLQ("F-606", clock.Now().Add(5*time.Second)).MarshalMapUnsafe(),
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw))
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
		want    *Shipment
		wantErr error
	}{
		{
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "get a shipment",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItem("B-202", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want:    newTestShipmentItem("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw))
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
		shipment *Shipment
	}
	tests := []struct {
		name    string
		setup   func(*testing.T) (*dynamodb.Client, func())
		args    args
		want    *Shipment
		wantErr error
	}{
		{
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				shipment: &Shipment{
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
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				shipment: newTestShipmentItem("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			},
			want:    newTestShipmentItem("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			wantErr: nil,
		},
		{
			name: "unique id",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				shipment: newTestShipmentItem("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			},
			want:    newTestShipmentItem("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			err = client.Put(ctx, tt.args.shipment)
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
			shipment, err := client.Get(ctx, tt.args.shipment.ID)
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			if !reflect.DeepEqual(shipment, tt.want) {
				t.Errorf("Get() got = %v, want %v", shipment, tt.want)
			}
		})
	}
}

func TestQueueSDKClientUpsert(t *testing.T) {
	type args struct {
		shipment *Shipment
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *Shipment
		wantErr  error
	}{
		{
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				shipment: &Shipment{
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
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				shipment: newTestShipmentItemAsPeeked("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			},
			want: func() *Shipment {
				s := newTestShipmentItemAsPeeked("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
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
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				shipment: newTestShipmentItem("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			},
			want:    newTestShipmentItem("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			err = client.Upsert(ctx, tt.args.shipment)
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
			shipment, err := client.Get(ctx, tt.args.shipment.ID)
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			if !reflect.DeepEqual(shipment, tt.want) {
				t.Errorf("Get() got = %v, want %v", shipment, tt.want)
			}
		})
	}
}

func TestQueueSDKClientUpdateStatus(t *testing.T) {
	type args struct {
		id        string
		newStatus Status
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
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id:        "",
				newStatus: "",
			},
			want:    nil,
			wantErr: &IDNotProvidedError{},
		},
		{
			name: "IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id:        "B-202",
				newStatus: StatusReadyToShip,
			},
			want:    nil,
			wantErr: &IDNotFoundError{},
		},
		{
			name: "same status",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id:        "A-101",
				newStatus: StatusUnderConstruction,
			},
			want: &Result{
				ID:                   "A-101",
				Status:               StatusUnderConstruction,
				LastUpdatedTimestamp: clock.FormatRFC3339(time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
				Version:              1,
			},
			wantErr: nil,
		},
		{
			name: "under construction to ready to ship",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				id:        "A-101",
				newStatus: StatusReadyToShip,
			},
			want: &Result{
				ID:                   "A-101",
				Status:               StatusReadyToShip,
				LastUpdatedTimestamp: clock.FormatRFC3339(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)),
				Version:              2,
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.UpdateStatus(ctx, tt.args.id, tt.args.newStatus)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("UpdateStatus() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("UpdateStatus() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("UpdateStatus() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientEnqueue(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *EnqueueResult
		wantErr  error
	}{
		{
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "RecordNotConstructedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want:    nil,
			wantErr: &RecordNotConstructedError{},
		},
		{
			name: "IllegalStateError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want:    nil,
			wantErr: &IllegalStateError{},
		},
		{
			name: "enqueue",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				id: "A-101",
			},
			want: &EnqueueResult{
				Result: &Result{
					ID:                   "A-101",
					Status:               StatusReadyToShip,
					LastUpdatedTimestamp: clock.FormatRFC3339(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)),
					Version:              2,
				},
				Shipment: func() *Shipment {
					s := newTestShipmentItemAsReady("A-101", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
					s.MarkAsEnqueued(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
					s.SystemInfo.Version = 2
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.Enqueue(ctx, tt.args.id)
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
		want     *PeekResult
		wantErr  error
	}{
		{
			name: "EmptyQueueError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("A-202", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsDLQ("A-303", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			want:    nil,
			wantErr: &EmptyQueueError{},
		},
		{
			name: "can peek when not selected",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsEnqueued("B-202",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			want: func() *PeekResult {
				s := newTestShipmentItemAsEnqueued("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsPeeked(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.SystemInfo.Version = 2
				r := &PeekResult{
					Result: &Result{
						ID:                   s.ID,
						Status:               s.SystemInfo.Status,
						LastUpdatedTimestamp: s.LastUpdatedTimestamp,
						Version:              s.SystemInfo.Version,
					},
					TimestampMillisUTC:   s.SystemInfo.PeekUTCTimestamp,
					PeekedShipmentObject: s,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "can peek when visibility timeout has expired",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("B-202",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 1, 1, 0, time.UTC),
			},
			want: func() *PeekResult {
				s := newTestShipmentItemAsPeeked("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsPeeked(time.Date(2023, 12, 1, 0, 1, 1, 0, time.UTC))
				s.SystemInfo.Version = 2
				r := &PeekResult{
					Result: &Result{
						ID:                   s.ID,
						Status:               s.SystemInfo.Status,
						LastUpdatedTimestamp: s.LastUpdatedTimestamp,
						Version:              s.SystemInfo.Version,
					},
					TimestampMillisUTC:   s.SystemInfo.PeekUTCTimestamp,
					PeekedShipmentObject: s,
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
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("B-202",
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
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

func TestQueueSDKClientDequeue(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		want     *DequeueResult
		wantErr  error
	}{
		{
			name: "EmptyQueueError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("A-202", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsDLQ("A-303", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			want:    nil,
			wantErr: &EmptyQueueError{},
		},
		{
			name: "can dequeue when not selected",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsEnqueued("B-202",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			want: func() *DequeueResult {
				s := newTestShipmentItemAsEnqueued("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsPeeked(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.MarkAsRemoved(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.SystemInfo.Version = 3
				r := &DequeueResult{
					Result: &Result{
						ID:                   s.ID,
						Status:               s.SystemInfo.Status,
						LastUpdatedTimestamp: s.LastUpdatedTimestamp,
						Version:              s.SystemInfo.Version,
					},
					DequeuedShipmentObject: s,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "can dequeue when visibility timeout has expired",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("B-202",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 1, 1, 0, time.UTC),
			},
			want: func() *DequeueResult {
				s := newTestShipmentItemAsPeeked("B-202", time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsPeeked(time.Date(2023, 12, 1, 0, 1, 1, 0, time.UTC))
				s.MarkAsRemoved(time.Date(2023, 12, 1, 0, 1, 1, 0, time.UTC))
				s.SystemInfo.Version = 3
				r := &DequeueResult{
					Result: &Result{
						ID:                   s.ID,
						Status:               s.SystemInfo.Status,
						LastUpdatedTimestamp: s.LastUpdatedTimestamp,
						Version:              s.SystemInfo.Version,
					},
					DequeuedShipmentObject: s,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "can not dequeue when visibility timeout",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("B-202",
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.Dequeue(ctx)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Dequeue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Dequeue() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("Dequeue() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientRemove(t *testing.T) {
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
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "already removed",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsRemoved("A-101",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want: func() *Result {
				s := newTestShipmentItemAsRemoved("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				r := &Result{
					ID:                   s.ID,
					Status:               s.SystemInfo.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.SystemInfo.Version,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "can remove",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101",
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
				s := newTestShipmentItem("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsRemoved(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.SystemInfo.Version = 2
				r := &Result{
					ID:                   s.ID,
					Status:               s.SystemInfo.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.SystemInfo.Version,
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.Remove(ctx, tt.args.id)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Remove() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Remove() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("Remove() got = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestQueueSDKClientRestore(t *testing.T) {
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
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "already restored",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsEnqueued("A-101",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want: func() *Result {
				s := newTestShipmentItemAsEnqueued("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				r := &Result{
					ID:                   s.ID,
					Status:               s.SystemInfo.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.SystemInfo.Version,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "can restore",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("A-101",
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
				s := newTestShipmentItemAsPeeked("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsEnqueued(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.SystemInfo.Version = 2
				r := &Result{
					ID:                   s.ID,
					Status:               s.SystemInfo.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.SystemInfo.Version,
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			result, err := client.Restore(ctx, tt.args.id)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Restore() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Restore() error = %v", err)
				return
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("Restore() got = %v, want %v", result, tt.want)
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
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "already DLQ",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsDLQ("A-101",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).
							MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			want: func() *Result {
				s := newTestShipmentItemAsDLQ("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				r := &Result{
					ID:                   s.ID,
					Status:               s.SystemInfo.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.SystemInfo.Version,
				}
				return r
			}(),
			wantErr: nil,
		},
		{
			name: "can DLQ",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("A-101",
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
				s := newTestShipmentItemAsDLQ("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.MarkAsDLQ(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.SystemInfo.Version = 2
				r := &Result{
					ID:                   s.ID,
					Status:               s.SystemInfo.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.SystemInfo.Version,
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
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
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "IDNotFoundError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
						Item: newTestShipmentItem("A-101",
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
				s := newTestShipmentItem("A-101",
					time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
				s.Touch(time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC))
				s.SystemInfo.Version = 2
				r := &Result{
					ID:                   s.ID,
					Status:               s.SystemInfo.Status,
					LastUpdatedTimestamp: s.LastUpdatedTimestamp,
					Version:              s.SystemInfo.Version,
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
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
		want     []*Shipment
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
			want:    []*Shipment{},
			wantErr: nil,
		},
		{
			name: "list",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				var puts []*types.PutRequest
				for i := 0; i < 10; i++ {
					puts = append(puts, &types.PutRequest{
						Item: newTestShipmentItem(fmt.Sprintf("A-%d", i),
							time.Date(2023, 12, 1, 0, 0, i, 0, time.UTC)).
							MarshalMapUnsafe(),
					})
				}
				return setupDynamoDB(t, puts...)
			},
			args: args{
				size: 10,
			},
			want: func() []*Shipment {
				var shipments []*Shipment
				for i := 0; i < 10; i++ {
					shipments = append(shipments, newTestShipmentItem(fmt.Sprintf("A-%d", i),
						time.Date(2023, 12, 1, 0, 0, i, 0, time.UTC)))
				}
				return shipments
			}(),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
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
				return result[i].SystemInfo.LastUpdatedTimestamp < result[j].SystemInfo.LastUpdatedTimestamp
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
						Item: newTestShipmentItem(fmt.Sprintf("A-%d", i),
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
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
						Item: newTestShipmentItem(fmt.Sprintf("A-%d", i),
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
					item := newTestShipmentItem(fmt.Sprintf("A-%d", i),
						time.Date(2023, 12, 1, 0, 0, i, 0, time.UTC))
					ids = append(ids, fmt.Sprintf("ID: %s, status: %s", item.ID, item.SystemInfo.Status))
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
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
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

func TestQueueSDKClientDelete(t *testing.T) {
	type args struct {
		id string
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
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
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
			name: "not exist id does not return error",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "B-101",
			},
			wantErr: nil,
		},
		{
			name: "existing id do not return error",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101", clock.Now()).MarshalMapUnsafe(),
					},
				)
			},
			args: args{
				id: "A-101",
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			err = client.Delete(ctx, tt.args.id)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Delete() error = %v", err)
				return
			}
		})
	}
}

func TestQueueSDKClientCreateTestData(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		setup    func(*testing.T) (*dynamodb.Client, func())
		sdkClock clock.Clock
		args     args
		want     *Shipment
		wantErr  error
	}{
		{
			name: "IDNotProvidedError",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t)
			},
			args: args{
				id: "",
			},
			want:    nil,
			wantErr: &IDNotProvidedError{},
		},
		{
			name: "can create test data by duplicate id",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101",
							time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)).MarshalMapUnsafe(),
					},
				)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				id: "A-101",
			},
			want:    newTestShipmentItem("A-101", time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)),
			wantErr: nil,
		},
		{
			name: "can create test data",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t)
			},
			sdkClock: mockClock{
				t: time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC),
			},
			args: args{
				id: "A-101",
			},
			want:    newTestShipmentItem("A-101", time.Date(2023, 12, 1, 0, 0, 10, 0, time.UTC)),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, clean := tt.setup(t)
			defer clean()
			ctx := context.Background()
			client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw), withClock(tt.sdkClock), WithAWSVisibilityTimeout(1))
			if err != nil {
				t.Fatalf("NewQueueSDKClient() error = %v", err)
				return
			}
			shipment, err := client.CreateTestData(ctx, tt.args.id)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("CreateTestData() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				return
			}
			if err != nil {
				t.Errorf("CreateTestData() error = %v", err)
				return
			}
			if !reflect.DeepEqual(shipment, tt.want) {
				t.Errorf("CreateTestData() got = %v, want %v", shipment, tt.want)
			}
		})
	}
}

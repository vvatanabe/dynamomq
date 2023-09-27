package sdk

import (
	"context"
	"reflect"
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

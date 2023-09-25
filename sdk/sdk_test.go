package sdk

import (
	"context"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/upsidr/dynamotest"
)

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

func newTestShipmentItem(id string) *Shipment {
	return NewShipmentWithIDAndData(id, newTestShipmentData(id))
}

func newTestShipmentItemAsEnqueued(id string) *Shipment {
	shipment := NewShipmentWithIDAndData(id, newTestShipmentData(id))
	shipment.MarkAsEnqueued()
	return shipment
}

func newTestShipmentItemAsPeeked(id string) *Shipment {
	shipment := NewShipmentWithIDAndData(id, newTestShipmentData(id))
	shipment.MarkAsPeeked()
	return shipment
}

func TestQueueSDKClientGetQueueStats(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T) (*dynamodb.Client, func())
		want  *QueueStats
	}{
		{
			name: "under construction",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101").MarshalMapUnsafe(),
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
			name: "enqueued",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsEnqueued("A-101").MarshalMapUnsafe(),
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
			name: "peeked",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("A-101").MarshalMapUnsafe(),
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
			name: "under construction and enqueued and peeked",
			setup: func(t *testing.T) (*dynamodb.Client, func()) {
				return setupDynamoDB(t,
					&types.PutRequest{
						Item: newTestShipmentItem("A-101").MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsEnqueued("B-202").MarshalMapUnsafe(),
					},
					&types.PutRequest{
						Item: newTestShipmentItemAsPeeked("C-303").MarshalMapUnsafe(),
					},
				)
			},
			want: &QueueStats{
				First100IDsInQueue:         []string{"B-202", "C-303"},
				First100SelectedIDsInQueue: []string{"C-303"},
				TotalRecordsInQueue:        2,
				TotalRecordsInProcessing:   1,
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
				t.Fatalf("failed to new QueueSDKClient: %v", err)
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

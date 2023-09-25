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

func TestQueueSDKClientGetQueueStats(t *testing.T) {
	raw, clean := setupDynamoDB(t, &types.PutRequest{
		Item: NewShipmentWithIDAndData("A-101", newTestData("A-101")).MarshalMapUnsafe(),
	}, &types.PutRequest{
		Item: NewShipmentWithIDAndData("B-202", newTestData("B-202")).MarshalMapUnsafe(),
	}, &types.PutRequest{
		Item: NewShipmentWithIDAndData("C-303", newTestData("C-303")).MarshalMapUnsafe(),
	}, &types.PutRequest{
		Item: NewShipmentWithIDAndData("D-404", newTestData("D-404")).MarshalMapUnsafe(),
	})
	defer clean()
	ctx := context.Background()
	client, err := NewQueueSDKClient(ctx, WithAWSDynamoDBClient(raw))
	if err != nil {
		t.Errorf("failed to new QueueSDKClient: %v", err)
		return
	}
	want := &QueueStats{
		First100IDsInQueue:         []string{},
		First100SelectedIDsInQueue: []string{},
		TotalRecordsInQueue:        0,
		TotalRecordsInProcessing:   0,
		TotalRecordsNotStarted:     0,
	}
	got, err := client.GetQueueStats(ctx)
	if err != nil {
		t.Errorf("GetQueueStats() error = %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetQueueStats() got = %v, want %v", got, want)
	}
}

package sdk

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	go82f46979 "github.com/vvatanabe/go82f46979"
	"github.com/vvatanabe/go82f46979/appdata"
	"github.com/vvatanabe/go82f46979/model"
)

type QueueSDKClient struct {
	dynamoDB *dynamodb.Client
	key      interface{} // Placeholder for ConfigField type
	config   interface{} // Placeholder for Configuration type

	actualTableName           string
	logicalTableName          string
	awsRegion                 string
	awsCredentialsProfileName string

	retryPolicyRetryCount int
}

// GetQueueStats retrieves statistics about the current state of the queue.
//
// The function queries the DynamoDB table to fetch records that are currently queued.
// It calculates the total number of records in the queue, the number of records that are
// currently being processed, and the number of records that have not yet started processing.
// Additionally, it provides the IDs of the first 100 records in the queue and the IDs of the
// first 100 records that are currently being processed.
//
// Parameters:
//
//	ctx: The context for the request, used for timeout and cancellation.
//
// Returns:
//   - A pointer to a QueueStats object containing the calculated statistics.
//   - An error if there's any issue querying the database or processing the results.
//
// Note: The function uses pagination to query the DynamoDB table and will continue querying
// until all records have been fetched or an error occurs.
//
// Example DynamoDB Query (expressed in a JSON-like representation):
//
//	{
//	  "ProjectionExpression": "id, system_info",
//	  "IndexName": "QueueingIndexName",
//	  "TableName": "ActualTableName",
//	  "ExpressionAttributeNames": {
//	    "#queued": "queued"
//	  },
//	  "KeyConditionExpression": "#queued = :value",
//	  "ScanIndexForward": true,
//	  "Limit": 250,
//	  "ExpressionAttributeValues": {
//	    ":value": {"S": "1"}
//	  }
//	}
func (c *QueueSDKClient) GetQueueStats(ctx context.Context) (*model.QueueStats, error) {
	var totalQueueSize int
	var exclusiveStartKey map[string]types.AttributeValue

	keyCond := expression.KeyEqual(expression.Key("queued"), expression.Value("1"))
	proj := expression.NamesList(expression.Name("id"), expression.Name("system_info"))

	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).WithProjection(proj).Build()
	if err != nil {
		return nil, fmt.Errorf("error building expression: %w", err)
	}

	var peekedRecords int
	var allQueueIDs []string
	var processingIDs []string

	for {
		queryInput := &dynamodb.QueryInput{
			ProjectionExpression:      expr.Projection(),
			IndexName:                 aws.String(go82f46979.QueueingIndexName),
			TableName:                 aws.String(c.actualTableName),
			ExpressionAttributeNames:  expr.Names(),
			KeyConditionExpression:    expr.KeyCondition(),
			ScanIndexForward:          aws.Bool(true),
			Limit:                     aws.Int32(250),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         exclusiveStartKey,
		}

		queryOutput, err := c.dynamoDB.Query(ctx, queryInput)
		if err != nil {
			return nil, fmt.Errorf("error querying dynamodb: %w", err)
		}

		exclusiveStartKey = queryOutput.LastEvaluatedKey

		for _, itemMap := range queryOutput.Items {
			totalQueueSize++

			item := appdata.Shipment{}
			err := attributevalue.UnmarshalMap(itemMap, &item)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal map: %s", err)
			}

			if item.SystemInfo.SelectedFromQueue {
				peekedRecords++
				if len(processingIDs) < 100 {
					processingIDs = append(processingIDs, item.ID)
				}
			}

			if len(allQueueIDs) < 100 {
				allQueueIDs = append(allQueueIDs, item.ID)
			}
		}

		if exclusiveStartKey == nil {
			break
		}
	}

	return &model.QueueStats{
		TotalRecordsInProcessing:   peekedRecords,
		TotalRecordsInQueue:        totalQueueSize,
		TotalRecordsNotStarted:     totalQueueSize - peekedRecords,
		First100IDsInQueue:         allQueueIDs,
		First100SelectedIDsInQueue: processingIDs,
	}, nil
}

// GetDLQStats retrieves statistics about the current state of the Dead Letter Queue (DLQ).
//
// The function queries the DynamoDB table to fetch records that are currently in the DLQ.
// It calculates the total number of records in the DLQ and provides the IDs of the first 100 records.
//
// Parameters:
//
//	ctx: The context for the request, used for timeout and cancellation.
//
// Returns:
//   - A pointer to a DLQResult object containing the calculated statistics.
//   - An error if there's any issue querying the database or processing the results.
//
// Note: The function uses pagination to query the DynamoDB table and will continue querying
// until all records have been fetched or an error occurs.
//
// Example DynamoDB Query (expressed in a JSON-like representation):
//
//	{
//	  "ProjectionExpression": "id, DLQ, system_info",
//	  "IndexName": "DLQQueueingIndexName",
//	  "TableName": "ActualTableName",
//	  "ExpressionAttributeNames": {
//	    "#DLQ": "DLQ"
//	  },
//	  "KeyConditionExpression": "#DLQ = :value",
//	  "Limit": 250,
//	  "ExpressionAttributeValues": {
//	    ":value": {"S": "1"}
//	  }
//	}
func (c *QueueSDKClient) GetDLQStats(ctx context.Context) (*model.DLQResult, error) {
	var totalDLQSize int
	var lastEvaluatedKey map[string]types.AttributeValue

	var listBANs []string

	keyCondition := expression.KeyEqual(expression.Key("DLQ"), expression.Value("1"))
	proj := expression.NamesList(expression.Name("id"), expression.Name("DLQ"), expression.Name("system_info"))

	expr, err := expression.NewBuilder().WithKeyCondition(keyCondition).WithProjection(proj).Build()
	if err != nil {
		return nil, fmt.Errorf("error building expression: %w", err)
	}

	for {
		input := &dynamodb.QueryInput{
			ProjectionExpression:      expr.Projection(),
			IndexName:                 aws.String(go82f46979.DlqQueueingIndexName),
			TableName:                 aws.String(c.actualTableName),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			KeyConditionExpression:    expr.KeyCondition(),
			Limit:                     aws.Int32(250),
			ExclusiveStartKey:         lastEvaluatedKey,
		}

		resp, err := c.dynamoDB.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("error querying dynamodb: %w", err)
		}

		lastEvaluatedKey = resp.LastEvaluatedKey

		for _, itemMap := range resp.Items {
			totalDLQSize++

			if len(listBANs) < 100 {
				item := appdata.Shipment{}
				err := attributevalue.UnmarshalMap(itemMap, &item)
				if err != nil {
					return nil, fmt.Errorf("failed to unmarshal map: %s", err)
				}
				listBANs = append(listBANs, item.ID)
			}
		}

		if lastEvaluatedKey == nil {
			break
		}
	}

	return &model.DLQResult{
		First100IDsInQueue: listBANs,
		TotalRecordsInDLQ:  totalDLQSize,
	}, nil
}

// Get retrieves a shipment record from the database by its ID.
//
// If the provided 'id' is empty, it returns nil and an error indicating that
// the ID is not provided.
//
// Parameters:
//   - ctx (context.Context): The context for the request.
//   - id (string): The unique identifier of the shipment record to retrieve.
//
// Returns:
//   - (*appdata.Shipment): A pointer to the retrieved shipment record.
//   - (error): An error if any occurred during the retrieval process, including
//     if the 'id' is empty, the database query fails, or unmarshaling the response
//     fails.
//
// Example JSON DynamoDB API Request:
//
//	{
//	  "TableName": "ActualTableName",
//	  "Key": {
//	    "ID": {
//	      "S": "your-id-value"
//	    }
//	  }
//	}
func (c *QueueSDKClient) Get(ctx context.Context, id string) (*appdata.Shipment, error) {
	if id == "" {
		return nil, errors.New("id is not provided ... cannot retrieve the shipment record")
	}

	input := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{Value: id},
		},
		TableName:      aws.String(c.actualTableName),
		ConsistentRead: aws.Bool(true),
	}

	resp, err := c.dynamoDB.GetItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to dynamodb get item: %s", err)
	}

	item := appdata.Shipment{}
	err = attributevalue.UnmarshalMap(resp.Item, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	return &item, nil
}

// PutImpl is a method provided by QueueSDKClient that adds or updates a Shipment object
// in a DynamoDB table. The Shipment object is stored in the table with the specified ID,
// creating a new entry if it doesn't exist. If the useUpsert parameter is true, it attempts
// to update an existing Shipment if one is present, incrementing the version.
// If false, it deletes the Shipment.
//
// Parameters:
//   - ctx: The context object is used for timeout, cancellation, and value sharing for the operation.
//   - shipment: The Shipment object to add or update.
//   - useUpsert: A boolean indicating whether to attempt an update if an existing Shipment is present.
//
// Returns:
//   - (error): An error if one occurs, otherwise, it returns nil on success.
func (c *QueueSDKClient) PutImpl(ctx context.Context, shipment *appdata.Shipment, useUpsert bool) error {
	if shipment == nil {
		return errors.New("shipment object cannot be nil")
	}

	// Check if already present
	retrievedShipment, err := c.Get(ctx, shipment.ID)
	if err != nil {
		return fmt.Errorf("failed to get a shipment: %s", err)
	}

	var version int

	// Upsert or delete
	if retrievedShipment != nil {
		if useUpsert {
			version = retrievedShipment.SystemInfo.Version
		} else {
			_, err := c.dynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(c.actualTableName),
				Key: map[string]types.AttributeValue{
					"ID": &types.AttributeValueMemberS{Value: shipment.ID},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to delete item: %w", err)
			}
		}
	}

	now := time.Now().UTC()

	system := &model.SystemInfo{
		Version:              version + 1,
		InQueue:              false,
		SelectedFromQueue:    false,
		Status:               shipment.SystemInfo.Status,
		CreationTimestamp:    now.Format(time.RFC3339),
		LastUpdatedTimestamp: now.Format(time.RFC3339),
	}

	shipment.SystemInfo = system

	item, err := attributevalue.MarshalMap(shipment)
	if err != nil {
		return fmt.Errorf("failed to marshal map: %w", err)
	}
	_, err = c.dynamoDB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.actualTableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	return nil
}

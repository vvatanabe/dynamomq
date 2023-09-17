package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/go82f46979/model"
)

const (
	AwsRegionDefault     = "us-east-1"
	AwsProfileDefault    = "default"
	DefaultTableName     = "Shipment"
	QueueingIndexName    = "queueud-last_updated_timestamp-index"
	DlqQueueingIndexName = "dlq-last_updated_timestamp-index"

	visibilityTimeoutInMinutes = 1
)

type QueueSDKClient struct {
	dynamoDB *dynamodb.Client

	tableName                 string
	awsRegion                 string
	awsCredentialsProfileName string
	credentialsProvider       aws.CredentialsProvider
}

func initialize(ctx context.Context, builder *Builder) (*QueueSDKClient, error) {

	c := &QueueSDKClient{
		tableName:                 builder.tableName,
		awsRegion:                 builder.awsRegion,
		credentialsProvider:       builder.credentialsProvider,
		awsCredentialsProfileName: builder.awsCredentialsProfileName,
	}

	if c.credentialsProvider == nil {
		accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		sessionToken := os.Getenv("AWS_SESSION_TOKEN")

		creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)
		c.credentialsProvider = &creds
	}

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(c.awsRegion),
		config.WithCredentialsProvider(c.credentialsProvider),
		config.WithSharedConfigProfile(c.awsCredentialsProfileName),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	c.dynamoDB = dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.RetryMaxAttempts = 10
	})

	return c, nil
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

	keyCond := expression.KeyEqual(expression.Key("queued"), expression.Value(1))
	proj := expression.NamesList(expression.Name("id"), expression.Name("system_info"))

	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).WithProjection(proj).Build()
	if err != nil {
		return nil, fmt.Errorf("error building expression: %w", err)
	}

	var peekedRecords int
	allQueueIDs := make([]string, 0)
	processingIDs := make([]string, 0)

	for {
		queryInput := &dynamodb.QueryInput{
			ProjectionExpression:      expr.Projection(),
			IndexName:                 aws.String(QueueingIndexName),
			TableName:                 aws.String(c.tableName),
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

			item := model.Shipment{}
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
//   - A pointer to a DLQStats object containing the calculated statistics.
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
func (c *QueueSDKClient) GetDLQStats(ctx context.Context) (*model.DLQStats, error) {
	var totalDLQSize int
	var lastEvaluatedKey map[string]types.AttributeValue

	listBANs := make([]string, 0)

	keyCondition := expression.KeyEqual(expression.Key("DLQ"), expression.Value(1))
	proj := expression.NamesList(expression.Name("id"), expression.Name("DLQ"), expression.Name("system_info"))

	expr, err := expression.NewBuilder().WithKeyCondition(keyCondition).WithProjection(proj).Build()
	if err != nil {
		return nil, fmt.Errorf("error building expression: %w", err)
	}

	for {
		input := &dynamodb.QueryInput{
			ProjectionExpression:      expr.Projection(),
			IndexName:                 aws.String(DlqQueueingIndexName),
			TableName:                 aws.String(c.tableName),
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
				item := model.Shipment{}
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

	return &model.DLQStats{
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
//   - (*model.Shipment): A pointer to the retrieved shipment record.
//   - (error): An error if any occurred during the retrieval process, including
//     if the 'id' is empty, the database query fails, or unmarshaling the response
//     fails.
//
// Example JSON DynamoDB API Request:
//
//	{
//	  "TableName": "ActualTableName",
//	  "Key": {
//	    "id": {
//	      "S": "your-id-value"
//	    }
//	  }
//	}
func (c *QueueSDKClient) Get(ctx context.Context, id string) (*model.Shipment, error) {
	if id == "" {
		return nil, errors.New("id is not provided ... cannot retrieve the shipment record")
	}

	input := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		TableName:      aws.String(c.tableName),
		ConsistentRead: aws.Bool(true),
	}

	resp, err := c.dynamoDB.GetItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to dynamodb get item: %s", err)
	}

	if resp.Item == nil {
		return nil, nil
	}

	item := model.Shipment{}
	err = attributevalue.UnmarshalMap(resp.Item, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	return &item, nil
}

// Put stores a given Shipment object in a DynamoDB table using the PutImpl method.
// The object is stored in the table with its specified ID, creating a new entry if it
// doesn't exist. If an entry with the same ID exists, the method will delete it.
//
// Parameters:
//   - ctx: Context used for timeout, cancellation, and value sharing for the operation.
//   - shipment: The Shipment object to be stored.
//
// Returns:
//   - error: Returns an error if one occurs, otherwise, it returns nil on successful storage.
func (c *QueueSDKClient) Put(ctx context.Context, shipment *model.Shipment) error {
	return c.PutImpl(ctx, shipment, false)
}

// Upsert attempts to update an existing Shipment object in a DynamoDB table or inserts it
// if it doesn't exist. It uses the PutImpl method to perform this upsert operation.
// If an entry with the same ID exists, the method will update it.
//
// Parameters:
//   - ctx: Context used for timeout, cancellation, and value sharing for the operation.
//   - shipment: The Shipment object to be upserted.
//
// Returns:
//   - error: Returns an error if one occurs, otherwise, it returns nil on successful upsert.
func (c *QueueSDKClient) Upsert(ctx context.Context, shipment *model.Shipment) error {
	return c.PutImpl(ctx, shipment, true)
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
func (c *QueueSDKClient) PutImpl(ctx context.Context, shipment *model.Shipment, useUpsert bool) error {
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
				TableName: aws.String(c.tableName),
				Key: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: shipment.ID},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to delete item: %w", err)
			}
		}
	}

	nowStr := time.Now().UTC().Format(time.RFC3339)
	system := model.NewSystemInfoWithID(shipment.ID)
	system.InQueue = 0
	system.SelectedFromQueue = false
	system.Status = shipment.SystemInfo.Status
	system.CreationTimestamp = nowStr
	system.LastUpdatedTimestamp = nowStr
	system.Version = version + 1

	shipment.SystemInfo = system

	item, err := attributevalue.MarshalMap(shipment)
	if err != nil {
		return fmt.Errorf("failed to marshal map: %w", err)
	}
	_, err = c.dynamoDB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	return nil
}

// UpdateStatus changes the status of a record with a given ID to a new status.
// This method is primarily intended for situations where there are operational
// issues or live incidents that need addressing. It is advised not to use this
// call unless explicitly necessary for such circumstances.
//
// The method tries to retrieve the record using the provided ID. If the ID is
// not provided or if the record cannot be found, an appropriate error status is
// returned. If the current status of the record matches the new status, the
// method returns without making any changes. Otherwise, it updates the status
// and the associated metadata.
//
// Parameters:
//   - ctx: The context object, used for timeout, cancellation, and value sharing for the operation.
//   - id: The ID of the record to update.
//   - newStatus: The new status to set for the record.
//
// Returns:
//   - A pointer to a Result object containing the result of the update operation.
//   - An error if one occurs during the process. A nil error indicates successful completion.
//
// Example JSON DynamoDB API Request:
//
//	{
//	 "TableName": "ActualTableName",
//	 "Key": {
//	   "id": {
//	     "S": "YourIdValue"
//	   }
//	 },
//	 "UpdateExpression": "ADD #sys.#v :inc SET #sys.#st = :st, #sys.last_updated_timestamp = :lut, last_updated_timestamp = :lut",
//	 "ExpressionAttributeNames": {
//	   "#v": "version",
//	   "#st": "status",
//	   "#sys": "system_info"
//	 },
//	 "ExpressionAttributeValues": {
//	   ":inc": {
//	     "N": "1"
//	   },
//	   ":v": {
//	     "N": "YourVersionValue"
//	   },
//	   ":lut": {
//	     "S": "LastUpdatedTimestamp"
//	   },
//	   ":st": {
//	     "S": "LastUpdatedTimestamp"
//	   }
//	 },
//	 "ConditionExpression": "#sys.#v = :v",
//	 "ReturnValues": "ALL_NEW"
//	}
func (c *QueueSDKClient) UpdateStatus(ctx context.Context, id string, newStatus model.Status) (*model.Result, error) {
	result := &model.Result{
		ID: id,
	}

	if id == "" {
		fmt.Println("ERROR: ID is not provided ... cannot retrieve the record!")
		result.ReturnValue = model.ReturnStatusFailedIDNotFound
		return result, nil
	}

	shipment, err := c.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if shipment == nil {
		fmt.Printf("ERROR: Customer with ID [%s] cannot be found!\n", id)
		result.ReturnValue = model.ReturnStatusFailedIDNotFound
		return result, nil
	}

	prevStatus := shipment.SystemInfo.Status
	version := shipment.SystemInfo.Version

	result.Status = newStatus

	if prevStatus == newStatus {
		result.Version = version
		result.LastUpdatedTimestamp = shipment.SystemInfo.LastUpdatedTimestamp
		result.ReturnValue = model.ReturnStatusSUCCESS
		return result, nil
	}

	now := time.Now().UTC()

	builder := expression.NewBuilder().
		WithUpdate(
			expression.Add(expression.Name("system_info.version"), expression.Value(1)).
				Set(expression.Name("system_info.status"), expression.Value(newStatus)).
				Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
				Set(expression.Name("last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))),
		).
		WithCondition(
			expression.Name("system_info.version").Equal(expression.Value(version)),
		)

	expr, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("error building expression: %v", err)
	}

	input := &dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: id,
			},
		},
		TableName:                 aws.String(c.tableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("updateFullyConstructedFlag() - failed to update multiple attributes in %s\n", c.tableName)
		fmt.Println(err)

		result.ReturnValue = model.ReturnStatusFailedDynamoError
		return result, nil
	}

	item := model.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Status = item.SystemInfo.Status
	result.Version = item.SystemInfo.Version
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp

	result.ReturnValue = model.ReturnStatusSUCCESS
	return result, nil
}

// Enqueue inserts the provided shipment ID into the queue. If the ID is not provided,
// it returns an error indicating the ID was not provided.
// If the shipment with the given ID cannot be found, it returns an error indicating the ID was not found.
//
// The function performs several checks on the status of the shipment:
// - If the status is UNDER_CONSTRUCTION, it indicates the record is not yet constructed.
// - If the status is not READY_TO_SHIP, it indicates an illegal state.
//
// If all checks pass, the function attempts to update several attributes of the shipment
// in the DynamoDB table. If the update is successful, it retrieves the shipment from
// DynamoDB and assigns it to the result.
//
// Parameters:
//
//	ctx: A context.Context for request. It can be used to cancel the request or to carry deadlines.
//	id: The ID of the shipment to enqueue.
//
// Returns:
//
//	*model.EnqueueResult: A pointer to the EnqueueResult structure which contains information about the enqueued shipment.
//	error: An error that can occur during the execution, or nil if no errors occurred.
//
// Example JSON DynamoDB API Request:
//
//	{
//	 "TableName": "ActualTableName",
//	 "Key": {
//	   "id": {
//	     "S": "YourIdValue"
//	   }
//	 },
//	 "ConditionExpression": "#sys.#v = :v",
//	 "UpdateExpression": "ADD #sys.#v :one SET queued = :one, #sys.queued = :one, #sys.queue_selected = :false, last_updated_timestamp = :lut, #sys.last_updated_timestamp = :lut, #sys.queue_added_timestamp = :lut, #sys.#st = :st",
//	 "ExpressionAttributeNames": {
//	   "#v": "version",
//	   "#st": "status",
//	   "#sys": "system_info"
//	 },
//	 "ExpressionAttributeValues": {
//	   ":one": {
//	     "N": "1"
//	   },
//	   ":false": {
//	     "BOOL": false
//	   },
//	   ":v": {
//	     "N": "YourVersionValue"
//	   },
//	   ":st": {
//	     "S": "READY_TO_SHIP"
//	   },
//	   ":lut": {
//	     "S": ""LastUpdatedTimestamp"
//	   }
//	 },
//	 "ReturnValues": "ALL_NEW"
//	}
func (c *QueueSDKClient) Enqueue(ctx context.Context, id string) (*model.EnqueueResult, error) {
	result := model.NewEnqueueResultWithID(id)

	if id == "" {
		fmt.Println("ID is not provided ... cannot proceed with the Enqueue() operation!")
		result.ReturnValue = model.ReturnStatusFailedIDNotProvided
		return result, nil
	}

	retrievedShipment, err := c.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if retrievedShipment == nil {
		fmt.Printf("Shipment with ID [%s] cannot be found!\n", id)
		result.ReturnValue = model.ReturnStatusFailedIDNotProvided
		return result, nil
	}

	version := retrievedShipment.SystemInfo.Version

	result.Status = retrievedShipment.SystemInfo.Status
	result.Version = version
	result.LastUpdatedTimestamp = retrievedShipment.SystemInfo.LastUpdatedTimestamp

	if result.Status == model.StatusUnderConstruction {
		result.ReturnValue = model.ReturnStatusFailedRecordNotConstructed
		return result, nil
	}

	if result.Status != model.StatusReadyToShip {
		result.ReturnValue = model.ReturnStatusFailedIllegalState
		return result, nil
	}

	now := time.Now().UTC()

	expr, err := expression.NewBuilder().
		WithUpdate(expression.Add(
			expression.Name("system_info.version"),
			expression.Value(1),
		).Set(
			expression.Name("queued"),
			expression.Value(1),
		).Set(
			expression.Name("system_info.queued"),
			expression.Value(1),
		).Set(
			expression.Name("system_info.queue_selected"),
			expression.Value(false),
		).Set(
			expression.Name("last_updated_timestamp"),
			expression.Value(now.Format(time.RFC3339)),
		).Set(
			expression.Name("system_info.last_updated_timestamp"),
			expression.Value(now.Format(time.RFC3339)),
		).Set(
			expression.Name("system_info.queue_added_timestamp"),
			expression.Value(now.Format(time.RFC3339)),
		).Set(
			expression.Name("system_info.status"),
			expression.Value(model.StatusReadyToShip),
		)).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(version))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build expression: %v", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &c.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: id,
			},
		},
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("enqueue() - failed to update multiple attributes in %s", c.tableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusFailedDynamoError
		return result, nil
	}

	item := model.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Version = item.SystemInfo.Version
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp
	result.Status = item.SystemInfo.Status

	shipment, err := c.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get shipment: %s", err)
	}

	result.Shipment = shipment
	result.ReturnValue = model.ReturnStatusSUCCESS

	return result, nil
}

// Peek peeks at the top of the queue to get the next item without actually removing it.
// It ensures items in the queue that are orphaned or stuck in a processing state for more than
// the allowed visibility timeout are considered for retrieval. It returns the peeked item's details
// encapsulated in a PeekResult structure or an error if something goes wrong.
//
// Parameters:
// - ctx: The context for managing request lifetime and cancelation.
//
// Returns:
//   - *model.PeekResult: The result of the peek operation, containing details like ID, Version,
//     LastUpdatedTimestamp, Status, and TimestampMillisUTC of the peeked item.
//     It also contains the ReturnValue which denotes the outcome of the operation.
//   - error: An error encountered during the peek operation, if any. Otherwise, nil.
//
// Note:
// The function does not update the top-level attribute `last_updated_timestamp` to
// avoid re-indexing the order.
func (c *QueueSDKClient) Peek(ctx context.Context) (*model.PeekResult, error) {
	var exclusiveStartKey map[string]types.AttributeValue
	var selectedID string
	var selectedVersion int
	recordForPeekIsFound := false

	names := expression.NamesList(
		expression.Name("id"),
		expression.Name("queued"),
		expression.Name("system_info"))
	expr, err := expression.NewBuilder().
		WithProjection(names).
		WithKeyCondition(expression.Key("queued").Equal(expression.Value(1))).
		Build()

	if err != nil {
		return nil, fmt.Errorf("error building expression: %w", err)
	}

	for {
		queryRequest := &dynamodb.QueryInput{
			ProjectionExpression:      expr.Projection(),
			IndexName:                 aws.String(QueueingIndexName),
			TableName:                 aws.String(c.tableName),
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Limit:                     aws.Int32(250),
			ScanIndexForward:          aws.Bool(true),
			ExclusiveStartKey:         exclusiveStartKey,
		}

		queryResult, err := c.dynamoDB.Query(ctx, queryRequest)
		if err != nil {
			return nil, fmt.Errorf("error querying dynamodb: %w", err)
		}

		exclusiveStartKey = queryResult.LastEvaluatedKey

		for _, itemMap := range queryResult.Items {

			item := model.Shipment{}
			err = attributevalue.UnmarshalMap(itemMap, &item)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal map: %s", err)
			}

			isQueueSelected := item.SystemInfo.SelectedFromQueue

			// check if there are no stragglers (marked to be in processing but actually orphan)
			if lastPeekTimeUTC := item.SystemInfo.PeekUTCTimestamp; lastPeekTimeUTC > 0 && isQueueSelected {
				currentTS := time.Now().UnixMilli()

				// if more than VISIBILITY_TIMEOUT_IN_MINUTES
				if (currentTS - lastPeekTimeUTC) > (visibilityTimeoutInMinutes * 60 * 1000) {
					selectedID = item.ID
					selectedVersion = item.SystemInfo.Version
					recordForPeekIsFound = true

					fmt.Printf(" >> Converted struggler, Shipment ID: [%s], age: %d\n", item.ID, currentTS-lastPeekTimeUTC)
				}
			} else { // otherwise, peek first record that satisfy basic condition (queued = :one)

				selectedID = item.ID
				selectedVersion = item.SystemInfo.Version
				recordForPeekIsFound = true
			}

			// no need to go further
			if recordForPeekIsFound {
				break
			}
		}

		if recordForPeekIsFound || exclusiveStartKey == nil {
			break
		}
	}

	result := model.NewPeekResult()

	if selectedID == "" {
		result.ReturnValue = model.ReturnStatusFailedEmptyQueue
		return result, nil
	}

	result.ID = selectedID

	shipment, err := c.Get(ctx, selectedID)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	tsUTC := now.UnixMilli()

	// IMPORTANT
	// please note, we are not updating top-level attribute `last_updated_timestamp` in order to avoid re-indexing the order

	expr, err = expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("system_info.version"), expression.Value(1)).
			Set(expression.Name("system_info.queue_selected"), expression.Value(true)).
			Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.queue_peek_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.peek_utc_timestamp"), expression.Value(tsUTC)).
			Set(expression.Name("system_info.status"), expression.Value(model.StatusProcessingShipment))).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(selectedVersion))).
		Build()
	if err != nil {
		return nil, err
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(c.tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: shipment.ID,
			},
		},
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("peek() - failed to update multiple attributes in %s\n", c.tableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusFailedDynamoError
		return result, nil
	}

	item := model.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.ID = item.ID

	peekedShipment, err := c.Get(ctx, selectedID)
	if err != nil {
		return nil, err
	}

	result.PeekedShipmentObject = peekedShipment

	result.Version = item.SystemInfo.Version
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp
	result.Status = item.SystemInfo.Status
	result.TimestampMillisUTC = item.SystemInfo.PeekUTCTimestamp

	result.ReturnValue = model.ReturnStatusSUCCESS
	return result, nil
}

// Dequeue attempts to dequeue an item from the Queue. It first peeks at the queue to get an item
// and then attempts to remove that item from the Queue if the peek was successful.
//
// Parameters:
//   - ctx: context.Context is the context for method invocation which can be used for timeout and cancellation.
//
// Returns:
//   - *model.DequeueResult: the result of the dequeue operation, containing information about the dequeued item.
//   - error: any error encountered during the operation. If successful, this is nil.
func (c *QueueSDKClient) Dequeue(ctx context.Context) (*model.DequeueResult, error) {
	peekResult, err := c.Peek(ctx)
	if err != nil {
		return nil, err
	}

	var dequeueResult *model.DequeueResult

	if peekResult.ReturnValue == model.ReturnStatusSUCCESS {
		ID := peekResult.ID
		removeResult, err := c.Remove(ctx, ID)
		if err != nil {
			return nil, err
		}

		dequeueResult = model.NewDequeueResultFromReturnResult(removeResult)

		if removeResult.IsSuccessful() {

			dequeueResult.DequeuedShipmentObject = peekResult.PeekedShipmentObject
		}
	} else {
		dequeueResult = model.NewDequeueResultFromReturnResult(peekResult.Result)
	}

	return dequeueResult, nil
}

// Remove tries to remove an item with a specified ID from the underlying datastore.
// The removal is done by updating attributes of the item in the datastore.
//
// Parameters:
//   - ctx: context.Context is the context for method invocation which can be used for timeout and cancellation.
//   - id: string is the unique identifier of the item to be removed.
//
// Returns:
//   - *model.Result: the result of the remove operation, containing information about the removed item's status.
//   - error: any error encountered during the operation, especially related to data marshaling and database interactions.
//     If successful and the item is just not found, the error is nil but the Result reflects the status.
func (c *QueueSDKClient) Remove(ctx context.Context, id string) (*model.Result, error) {
	result := &model.Result{ID: id}

	shipment, err := c.Get(ctx, id)
	if shipment == nil {
		result.ReturnValue = model.ReturnStatusFailedIDNotFound
		return result, nil
	}

	now := time.Now().UTC()

	expr, err := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("system_info.version"), expression.Value(1)).
			Remove(expression.Name("system_info.peek_utc_timestamp")).
			Remove(expression.Name("queued")).
			Remove(expression.Name("DLQ")).
			Set(expression.Name("system_info.queued"), expression.Value(0)).
			Set(expression.Name("system_info.queue_selected"), expression.Value(false)).
			Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.queue_remove_timestamp"), expression.Value(now.Format(time.RFC3339)))).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(shipment.SystemInfo.Version))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("building expression: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &c.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: id,
			},
		},
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("remove() - failed to update multiple attributes in %s\n", c.tableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusFailedDynamoError
		return result, nil
	}

	item := model.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Version = item.SystemInfo.Version
	result.Status = item.SystemInfo.Status
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp

	result.ReturnValue = model.ReturnStatusSUCCESS
	return result, nil
}

// Restore adds back a record to the queue by its ID. The function updates the
// record in the queue to reflect its restored status.
//
// Parameters:
//
//	ctx: The context to be used for the operation.
//	id: The ID of the record to restore.
//
// Returns:
//
//	*model.Result: A pointer to a Result object that contains
//	information about the result of the restore operation, such as the version,
//	status, and last updated timestamp.
//
//	error: An error that describes any issues that occurred during the
//	restore operation. If the operation is successful, this will be nil.
func (c *QueueSDKClient) Restore(ctx context.Context, id string) (*model.Result, error) {
	result := model.NewReturnResultWithID(id)

	shipment, err := c.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if shipment == nil || err != nil {
		result.ReturnValue = model.ReturnStatusFailedIDNotFound
		return result, nil
	}

	now := time.Now().UTC()

	expr, err := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("system_info.version"), expression.Value(1)).
			Remove(expression.Name("DLQ")).
			Set(expression.Name("system_info.queued"), expression.Value(1)).
			Set(expression.Name("queued"), expression.Value(1)).
			Set(expression.Name("system_info.queue_selected"), expression.Value(false)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.queue_add_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.status"), expression.Value(model.StatusReadyToShip))).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(shipment.SystemInfo.Version))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("building expression: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &c.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: id,
			},
		},
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("restore() - failed to update multiple attributes in %s\n", c.tableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusFailedDynamoError
		return result, nil
	}

	item := model.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Version = item.SystemInfo.Version
	result.Status = item.SystemInfo.Status
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp

	result.ReturnValue = model.ReturnStatusSUCCESS
	return result, nil
}

// SendToDLQ sends a specified record with the given ID to the Dead Letter Queue (DLQ).
// The method performs various operations:
// 1. Fetches the shipment details associated with the provided ID.
// 2. Constructs a DynamoDB update expression to mark the record for DLQ and updates timestamps.
// 3. Updates the specified record in the DynamoDB table.
//
// Parameters:
//
//	ctx: The context for the operation. It can be used to control cancelation.
//	id: The ID of the record that needs to be sent to the DLQ.
//
// Returns:
//   - *model.Result: A pointer to the result structure which contains details like Version, Status, LastUpdatedTimestamp,
//     and ReturnValue indicating the result of the operation (e.g., success, failed due to ID not found, etc.).
//   - error: Non-nil if there was an error during the operation.
func (c *QueueSDKClient) SendToDLQ(ctx context.Context, id string) (*model.Result, error) {
	result := model.NewReturnResultWithID(id)

	shipment, err := c.Get(ctx, id)
	if err != nil || shipment == nil {
		result.ReturnValue = model.ReturnStatusFailedIDNotFound
		return result, nil
	}

	now := time.Now().UTC()

	expr, err := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("system_info.version"), expression.Value(1)).
			Remove(expression.Name("queued")).
			Set(expression.Name("DLQ"), expression.Value(1)).
			Set(expression.Name("system_info.queued"), expression.Value(0)).
			Set(expression.Name("system_info.queue_selected"), expression.Value(false)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.dlq_add_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.status"), expression.Value(model.StatusInDLQ))).
		WithCondition(expression.And(
			expression.Name("system_info.version").Equal(expression.Value(shipment.SystemInfo.Version)),
			expression.Name("system_info.queued").Equal(expression.Value(1)),
		)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("building expression: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &c.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: id,
			},
		},
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("SendToDLQ() - failed to update multiple attributes in %s\n", c.tableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusFailedDynamoError
		return result, nil
	}

	item := model.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Version = item.SystemInfo.Version
	result.Status = item.SystemInfo.Status
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp

	result.ReturnValue = model.ReturnStatusSUCCESS
	return result, nil
}

// Touch updates the 'last_updated_timestamp' of a given item identified by its 'id'
// and increments its 'version' by 1 in the DynamoDB table.
// It uses optimistic locking based on the item's 'version' to ensure that updates
// are not conflicting with other concurrent updates.
//
// Parameters:
// ctx: The context for this operation. It can be used to time out or cancel the operation.
// id: The identifier of the item to update.
//
// Returns:
// *model.Result: A result object that contains updated values and status of the operation.
// - If the given 'id' does not exist, the 'ReturnValue' of the result will be set to 'ReturnStatusFailedIDNotFound'.
// - If the operation succeeds in updating the item, the 'ReturnValue' will be set to 'ReturnStatusSUCCESS'.
// - If there is an error while updating in DynamoDB, the 'ReturnValue' will be set to 'ReturnStatusFailedDynamoError'.
// error: An error object indicating any error that occurred during the operation.
// - If there's an error while building the DynamoDB expression, this error is returned.
// - If there's an error unmarshalling the DynamoDB response, this error is returned.
// Otherwise, if the operation succeeds, the error will be 'nil'.
func (c *QueueSDKClient) Touch(ctx context.Context, id string) (*model.Result, error) {
	result := model.NewReturnResultWithID(id)

	shipment, err := c.Get(ctx, id)
	if err != nil || shipment == nil {
		result.ReturnValue = model.ReturnStatusFailedIDNotFound
		return result, nil
	}

	now := time.Now().UTC()

	expr, err := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("system_info.version"), expression.Value(1)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(now.Format(time.RFC3339)))).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(shipment.SystemInfo.Version))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("building expression: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &c.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: id,
			},
		},
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("Touch() - failed to update multiple attributes in %s\n", c.tableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusFailedDynamoError
		return result, nil
	}

	item := model.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Version = item.SystemInfo.Version
	result.Status = item.SystemInfo.Status
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp

	result.ReturnValue = model.ReturnStatusSUCCESS
	return result, nil
}

// List retrieves a list of Shipments from the DynamoDB table up to the given size.
// The function constructs a DynamoDB scan with specific projection expressions and
// returns the list of found shipments.
//
// Parameters:
//   - ctx: The context to use for the request.
//   - size: The maximum number of items to retrieve.
//
// Returns:
//   - A slice of pointers to model.Shipment if found.
//   - error if there's any issue in the operation.
func (c *QueueSDKClient) List(ctx context.Context, size int32) ([]*model.Shipment, error) {
	expr, err := expression.NewBuilder().
		WithProjection(expression.NamesList(expression.Name("id"), expression.Name("system_info"))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("building expression: %w", err)
	}

	input := &dynamodb.ScanInput{
		TableName:                &c.tableName,
		ProjectionExpression:     expr.Projection(),
		ExpressionAttributeNames: expr.Names(),
		Limit:                    aws.Int32(size),
	}

	output, err := c.dynamoDB.Scan(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("scan dynamodb: %w", err)
	}

	var shipments []*model.Shipment
	err = attributevalue.UnmarshalListOfMaps(output.Items, &shipments)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal list of map: %s", err)
	}

	return shipments, nil
}

// ListIDs retrieves a list of IDs from the Shipment items in the DynamoDB table
// up to the given size. It uses the List function to retrieve the shipments and
// then extracts the IDs from them.
//
// Parameters:
//   - ctx: The context to use for the request.
//   - size: The maximum number of IDs to retrieve.
//
// Returns:
//   - A slice of string IDs if found.
//   - error if there's any issue in the operation.
func (c *QueueSDKClient) ListIDs(ctx context.Context, size int32) ([]string, error) {
	shipments, err := c.List(ctx, size)
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(shipments))
	for i, s := range shipments {
		ids[i] = s.ID
	}

	return ids, nil
}

// ListExtendedIDs retrieves a list of extended IDs (formatted as "ID - status: STATUS")
// from the Shipment items in the DynamoDB table up to the given size.
// It uses the List function to retrieve the shipments and then constructs
// the extended ID strings from them.
//
// Parameters:
//   - ctx: The context to use for the request.
//   - size: The maximum number of extended IDs to retrieve.
//
// Returns:
//   - A slice of extended ID strings if found.
//   - error if there's any issue in the operation.
func (c *QueueSDKClient) ListExtendedIDs(ctx context.Context, size int32) ([]string, error) {
	shipments, err := c.List(ctx, size)
	if err != nil {
		return nil, err
	}

	extendedIDs := make([]string, len(shipments))
	for i, s := range shipments {
		extendedIDs[i] = fmt.Sprintf("ID: %s, status: %s", s.ID, s.SystemInfo.Status)
	}

	return extendedIDs, nil
}

// Delete removes the shipment record associated with the provided ID from the database.
// It will return an error if the ID is empty or if there's any issue deleting the record.
//
// Parameters:
//   - ctx: The context to be used for the deletion request. It allows for timeout and cancellation.
//   - id: The unique identifier of the shipment record to be deleted.
//
// Returns:
//   - error: Non-nil if there was an error during the delete operation.
func (c *QueueSDKClient) Delete(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("shipment id cannot be empty")
	}

	input := &dynamodb.DeleteItemInput{
		TableName: &c.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: id,
			},
		},
	}

	_, err := c.dynamoDB.DeleteItem(ctx, input)
	return err
}

// CreateTestData creates a test data shipment record associated with the provided ID.
// It first ensures that no existing data with the given ID exists by deleting it,
// then creates a shipment record with test data.
// If the ID is empty or there's an issue creating the test data, it will return an error.
//
// Parameters:
//   - ctx: The context to be used for the operation. It allows for timeout and cancellation.
//   - id: The unique identifier for the shipment record to be created.
//
// Returns:
//   - *model.Shipment: The created shipment record.
//   - error: Non-nil if there was an error during the creation process.
func (c *QueueSDKClient) CreateTestData(ctx context.Context, id string) (*model.Shipment, error) {
	if id == "" {
		return nil, errors.New("shipment id cannot be empty")
	}

	err := c.Delete(ctx, id)
	if err != nil {
		return nil, err
	}

	data := &model.ShipmentData{
		ID:    id,
		Data1: "Data 1",
		Data2: "Data 2",
		Data3: "Data 3",
		Items: []model.ShipmentItem{
			{SKU: "Item-1", Packed: true},
			{SKU: "Item-2", Packed: true},
			{SKU: "Item-3", Packed: true},
		},
	}

	shipment := model.NewShipmentWithID(id)
	shipment.Data = data

	err = c.Put(ctx, shipment)
	if err != nil {
		return nil, err
	}

	return shipment, nil
}

type Builder struct {
	awsRegion                 string
	awsCredentialsProfileName string
	tableName                 string
	credentialsProvider       aws.CredentialsProvider
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Build(ctx context.Context) (*QueueSDKClient, error) {
	if b.awsRegion == "" {
		b.awsRegion = AwsRegionDefault
	}
	if b.awsCredentialsProfileName == "" {
		b.awsCredentialsProfileName = AwsProfileDefault
	}
	if b.tableName == "" {
		b.tableName = DefaultTableName
	}
	return initialize(ctx, b)
}

func (b *Builder) WithRegion(region string) *Builder {
	b.awsRegion = region
	return b
}

func (b *Builder) WithCredentialsProfileName(profile string) *Builder {
	b.awsCredentialsProfileName = profile
	return b
}

func (b *Builder) WithTableName(tableName string) *Builder {
	b.tableName = tableName
	return b
}

func (b *Builder) WithCredentialsProvider(creds aws.CredentialsProvider) *Builder {
	b.credentialsProvider = creds
	return b
}

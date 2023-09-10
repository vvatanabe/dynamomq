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
	"github.com/vvatanabe/go82f46979"
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
func (c *QueueSDKClient) Put(ctx context.Context, shipment *appdata.Shipment) error {
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
func (c *QueueSDKClient) Upsert(ctx context.Context, shipment *appdata.Shipment) error {
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
//   - A pointer to a ReturnResult object containing the result of the update operation.
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
func (c *QueueSDKClient) UpdateStatus(ctx context.Context, id string, newStatus model.StatusEnum) (*model.ReturnResult, error) {
	result := &model.ReturnResult{
		ID: id,
	}

	if id == "" {
		fmt.Println("ERROR: ID is not provided ... cannot retrieve the record!")
		result.ReturnValue = model.ReturnStatusEnumFailedIDNotFound
		return result, nil
	}

	shipment, err := c.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if shipment == nil {
		fmt.Printf("ERROR: Customer with ID [%s] cannot be found!\n", id)
		result.ReturnValue = model.ReturnStatusEnumFailedIDNotFound
		return result, nil
	}

	prevStatus := shipment.SystemInfo.Status
	version := shipment.SystemInfo.Version

	result.Status = newStatus

	if prevStatus == newStatus {
		result.Version = version
		result.LastUpdatedTimestamp = shipment.SystemInfo.LastUpdatedTimestamp
		result.ReturnValue = model.ReturnStatusEnumSUCCESS
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
		TableName:                 aws.String(c.actualTableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("updateFullyConstructedFlag() - failed to update multiple attributes in %s\n", c.actualTableName)
		fmt.Println(err)

		result.ReturnValue = model.ReturnStatusEnumFailedDynamoError
		return result, nil
	}

	item := appdata.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Status = item.SystemInfo.Status
	result.Version = item.SystemInfo.Version
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp

	result.ReturnValue = model.ReturnStatusEnumSUCCESS
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
		result.ReturnValue = model.ReturnStatusEnumFailedIDNotProvided
		return result, nil
	}

	retrievedShipment, err := c.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if retrievedShipment == nil {
		fmt.Printf("Shipment with ID [%s] cannot be found!\n", id)
		result.ReturnValue = model.ReturnStatusEnumFailedIDNotProvided
		return result, nil
	}

	version := retrievedShipment.SystemInfo.Version

	result.Status = retrievedShipment.SystemInfo.Status
	result.Version = version
	result.LastUpdatedTimestamp = retrievedShipment.SystemInfo.LastUpdatedTimestamp

	if result.Status == model.StatusEnumUnderConstruction {
		result.ReturnValue = model.ReturnStatusEnumFailedRecordNotConstructed
		return result, nil
	}

	if result.Status != model.StatusEnumReadyToShip {
		result.ReturnValue = model.ReturnStatusEnumFailedIllegalState
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
			expression.Value(model.StatusEnumReadyToShip),
		)).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(version))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build expression: %v", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &c.actualTableName,
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
		fmt.Printf("enqueue() - failed to update multiple attributes in %s", c.actualTableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusEnumFailedDynamoError
		return result, nil
	}

	item := appdata.Shipment{}
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
	result.ReturnValue = model.ReturnStatusEnumSUCCESS

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
			IndexName:                 aws.String(go82f46979.QueueingIndexName),
			TableName:                 aws.String(c.actualTableName),
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

			item := appdata.Shipment{}
			err = attributevalue.UnmarshalMap(itemMap, &item)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal map: %s", err)
			}

			isQueueSelected := item.SystemInfo.SelectedFromQueue

			// check if there are no stragglers (marked to be in processing but actually orphan)
			if lastPeekTimeUTC := item.SystemInfo.PeekUTCTimestamp; lastPeekTimeUTC > 0 && isQueueSelected {
				currentTS := time.Now().UnixMilli()

				// if more than VISIBILITY_TIMEOUT_IN_MINUTES
				if (currentTS - lastPeekTimeUTC) > (go82f46979.VisibilityTimeoutInMinutes * 60 * 1000) {
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

	result := &model.PeekResult{}

	if selectedID == "" {
		result.ReturnValue = model.ReturnStatusEnumFailedEmptyQueue
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
			Set(expression.Name("system_info.status"), expression.Value(model.StatusEnumProcessingShipment))).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(selectedVersion))).
		Build()
	if err != nil {
		return nil, err
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(c.actualTableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: shipment.ID,
			},
		},
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	outcome, err := c.dynamoDB.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("peek() - failed to update multiple attributes in %s\n", c.actualTableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusEnumFailedDynamoError
		return result, nil
	}

	item := appdata.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Version = item.SystemInfo.Version
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp
	result.Status = item.SystemInfo.Status
	result.TimestampMillisUTC = item.SystemInfo.PeekUTCTimestamp

	result.ReturnValue = model.ReturnStatusEnumSUCCESS
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

	if peekResult.ReturnValue == model.ReturnStatusEnumSUCCESS {
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
		dequeueResult = model.NewDequeueResultFromReturnResult(peekResult.ReturnResult)
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
//   - *model.ReturnResult: the result of the remove operation, containing information about the removed item's status.
//   - error: any error encountered during the operation, especially related to data marshaling and database interactions.
//     If successful and the item is just not found, the error is nil but the ReturnResult reflects the status.
func (c *QueueSDKClient) Remove(ctx context.Context, id string) (*model.ReturnResult, error) {
	result := &model.ReturnResult{ID: id}

	shipment, err := c.Get(ctx, id)
	if shipment == nil {
		result.ReturnValue = model.ReturnStatusEnumFailedIDNotFound
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
			Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("last_updated_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.queue_remove_timestamp"), expression.Value(now.Format(time.RFC3339))).
			Set(expression.Name("system_info.status"), expression.Value(model.StatusEnumProcessingShipment))).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(shipment.SystemInfo.Version))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("building expression: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &c.actualTableName,
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
		fmt.Printf("remove() - failed to update multiple attributes in %s\n", c.actualTableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusEnumFailedDynamoError
		return result, nil
	}

	item := appdata.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Version = item.SystemInfo.Version
	result.Status = item.SystemInfo.Status
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp

	result.ReturnValue = model.ReturnStatusEnumSUCCESS
	return result, nil
}

// Restore adds back a record to the queue by its ID. The function updates the
// record in the queue to reflect its restored status.
//
// Parameters:
//   ctx: The context to be used for the operation.
//   id: The ID of the record to restore.
//
// Returns:
//   *model.ReturnResult: A pointer to a ReturnResult object that contains
//   information about the result of the restore operation, such as the version,
//   status, and last updated timestamp.
//
//   error: An error that describes any issues that occurred during the
//   restore operation. If the operation is successful, this will be nil.
func (c *QueueSDKClient) Restore(ctx context.Context, id string) (*model.ReturnResult, error) {
	result := model.NewReturnResultWithID(id)

	shipment, err := c.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if shipment == nil || err != nil {
		result.ReturnValue = model.ReturnStatusEnumFailedIDNotFound
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
			Set(expression.Name("system_info.status"), expression.Value(model.StatusEnumReadyToShip))).
		WithCondition(expression.Name("system_info.version").Equal(expression.Value(shipment.SystemInfo.Version))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("building expression: %w", err)
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &c.actualTableName,
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
		fmt.Printf("restore() - failed to update multiple attributes in %s\n", c.actualTableName)
		fmt.Println(err)
		result.ReturnValue = model.ReturnStatusEnumFailedDynamoError
		return result, nil
	}

	item := appdata.Shipment{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %s", err)
	}

	result.Version = item.SystemInfo.Version
	result.Status = item.SystemInfo.Status
	result.LastUpdatedTimestamp = item.SystemInfo.LastUpdatedTimestamp

	result.ReturnValue = model.ReturnStatusEnumSUCCESS
	return result, nil
}

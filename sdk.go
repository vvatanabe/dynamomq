package dynamomq

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
	"github.com/vvatanabe/dynamomq/internal/clock"
)

const (
	AwsRegionDefault                  = "us-east-1"
	AwsProfileDefault                 = "default"
	DefaultTableName                  = "dynamo-mq-table"
	QueueingIndexName                 = "dynamo-mq-index-queue_type-queue_add_timestamp"
	DefaultRetryMaxAttempts           = 10
	DefaultVisibilityTimeoutInMinutes = 1
)

type Client[T any] interface {
	SendMessage(ctx context.Context, params *SendMessageInput[T]) (*SendMessageOutput[T], error)
	ReceiveMessage(ctx context.Context, params *ReceiveMessageInput) (*ReceiveMessageOutput[T], error)
	UpdateMessageAsVisible(ctx context.Context, params *UpdateMessageAsVisibleInput) (*UpdateMessageAsVisibleOutput[T], error)
	DeleteMessage(ctx context.Context, params *DeleteMessageInput) (*DeleteMessageOutput, error)
	MoveMessageToDLQ(ctx context.Context, params *MoveMessageToDLQInput) (*MoveMessageToDLQOutput, error)
	RedriveMessage(ctx context.Context, params *RedriveMessageInput) (*RedriveMessageOutput, error)
	GetMessage(ctx context.Context, params *GetMessageInput) (*GetMessageOutput[T], error)
	GetQueueStats(ctx context.Context) (*QueueStats, error)
	GetDLQStats(ctx context.Context) (*DLQStats, error)
	List(ctx context.Context, size int32) ([]*Message[T], error)
	ListIDs(ctx context.Context, size int32) ([]string, error)
	ListExtendedIDs(ctx context.Context, size int32) ([]string, error)

	Put(ctx context.Context, message *Message[T]) error
	Upsert(ctx context.Context, message *Message[T]) error
	Touch(ctx context.Context, id string) (*Result, error)
	GetDynamodbClient() *dynamodb.Client
}

type client[T any] struct {
	dynamoDB *dynamodb.Client

	tableName                 string
	awsRegion                 string
	awsCredentialsProfileName string
	baseEndpoint              string
	credentialsProvider       aws.CredentialsProvider

	retryMaxAttempts           int
	visibilityTimeoutInMinutes int
	maximumReceives            int
	useFIFO                    bool

	clock clock.Clock
}

type ClientOptions struct {
	tableName                  string
	awsRegion                  string
	awsCredentialsProfileName  string
	credentialsProvider        aws.CredentialsProvider
	baseEndpoint               string
	retryMaxAttempts           int
	visibilityTimeoutInMinutes int
	maximumReceives            int
	useFIFO                    bool
	dynamoDB                   *dynamodb.Client
	clock                      clock.Clock
}

func WithTableName(tableName string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.tableName = tableName
	}
}

func WithAWSRegion(awsRegion string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.awsRegion = awsRegion
	}
}

func WithAWSCredentialsProfileName(awsCredentialsProfileName string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.awsCredentialsProfileName = awsCredentialsProfileName
	}
}

func WithAWSCredentialsProvider(credentialsProvider aws.CredentialsProvider) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.credentialsProvider = credentialsProvider
	}
}

func WithAWSBaseEndpoint(baseEndpoint string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.baseEndpoint = baseEndpoint
	}
}

func WithAWSRetryMaxAttempts(retryMaxAttempts int) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.retryMaxAttempts = retryMaxAttempts
	}
}

func WithAWSVisibilityTimeout(minutes int) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.visibilityTimeoutInMinutes = minutes
	}
}

func WithUseFIFO(useFIFO bool) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.useFIFO = useFIFO
	}
}

func WithAWSDynamoDBClient(client *dynamodb.Client) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.dynamoDB = client
	}
}

func NewFromConfig[T any](ctx context.Context, optFns ...func(*ClientOptions)) (Client[T], error) {
	o := &ClientOptions{
		tableName:                  DefaultTableName,
		awsRegion:                  AwsRegionDefault,
		awsCredentialsProfileName:  AwsProfileDefault,
		retryMaxAttempts:           DefaultRetryMaxAttempts,
		visibilityTimeoutInMinutes: DefaultVisibilityTimeoutInMinutes,
		useFIFO:                    false,
		clock:                      &clock.RealClock{},
	}
	for _, opt := range optFns {
		opt(o)
	}
	c := &client[T]{
		tableName:                  o.tableName,
		awsRegion:                  o.awsRegion,
		awsCredentialsProfileName:  o.awsCredentialsProfileName,
		credentialsProvider:        o.credentialsProvider,
		baseEndpoint:               o.baseEndpoint,
		retryMaxAttempts:           o.retryMaxAttempts,
		visibilityTimeoutInMinutes: o.visibilityTimeoutInMinutes,
		maximumReceives:            o.maximumReceives,
		useFIFO:                    o.useFIFO,
		dynamoDB:                   o.dynamoDB,
		clock:                      o.clock,
	}
	if c.dynamoDB != nil {
		return c, nil
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
		options.RetryMaxAttempts = c.retryMaxAttempts
		if c.baseEndpoint != "" {
			options.BaseEndpoint = aws.String(c.baseEndpoint)
		}
	})
	return c, nil
}

type SendMessageInput[T any] struct {
	ID   string
	Data T
}

// SendMessageOutput represents the result for the SendMessage() API call.
type SendMessageOutput[T any] struct {
	*Result             // Embedded type for inheritance-like behavior in Go
	Message *Message[T] `json:"-"`
}

func (c *client[T]) SendMessage(ctx context.Context, params *SendMessageInput[T]) (*SendMessageOutput[T], error) {
	if params == nil {
		params = &SendMessageInput[T]{}
	}
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: params.ID,
	})
	if err != nil {
		return &SendMessageOutput[T]{}, err
	}
	if retrieved.Message != nil {
		return &SendMessageOutput[T]{}, &IDDuplicatedError{}
	}
	message := NewDefaultMessage(params.ID, params.Data, c.clock.Now())
	err = c.put(ctx, message)
	if err != nil {
		return &SendMessageOutput[T]{}, err
	}
	return &SendMessageOutput[T]{
		Result: &Result{
			ID:                   message.ID,
			Status:               message.Status,
			LastUpdatedTimestamp: message.LastUpdatedTimestamp,
			Version:              message.Version,
		},
		Message: message,
	}, nil
}

type ReceiveMessageInput struct {
}

// ReceiveMessageOutput represents the result for the ReceiveMessage() API call.
type ReceiveMessageOutput[T any] struct {
	*Result                            // Embedded type for inheritance-like behavior in Go
	PeekFromQueueTimestamp string      `json:"queue_peek_timestamp"`
	PeekedMessageObject    *Message[T] `json:"-"`
}

func (c *client[T]) ReceiveMessage(ctx context.Context, params *ReceiveMessageInput) (*ReceiveMessageOutput[T], error) {
	if params == nil {
		params = &ReceiveMessageInput{}
	}
	expr, err := expression.NewBuilder().
		WithKeyCondition(expression.Key("queue_type").Equal(expression.Value(QueueTypeStandard))). // FIXME make DLQs peek-enabled.
		Build()
	if err != nil {
		return &ReceiveMessageOutput[T]{}, &BuildingExpressionError{Cause: err}
	}
	var (
		exclusiveStartKey map[string]types.AttributeValue
		selectedItem      *Message[T]
	)
	visibilityTimeout := time.Duration(c.visibilityTimeoutInMinutes) * time.Minute
	for {
		queryResult, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(QueueingIndexName),
			TableName:                 aws.String(c.tableName),
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Limit:                     aws.Int32(250),
			ScanIndexForward:          aws.Bool(true),
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return &ReceiveMessageOutput[T]{}, handleDynamoDBError(err)
		}
		exclusiveStartKey = queryResult.LastEvaluatedKey
		for _, itemMap := range queryResult.Items {
			item := Message[T]{}
			if err = attributevalue.UnmarshalMap(itemMap, &item); err != nil {
				return &ReceiveMessageOutput[T]{}, &UnmarshalingAttributeError{Cause: err}
			}
			isQueueSelected := item.IsQueueSelected(c.clock.Now(), visibilityTimeout)
			if c.useFIFO && isQueueSelected {
				goto ExitLoop
			}
			if !isQueueSelected {
				selectedItem = &item
				break
			}
		}
		if selectedItem != nil || exclusiveStartKey == nil {
			break
		}
	}
ExitLoop:
	if selectedItem == nil || selectedItem.StartProcessing(c.clock.Now(), visibilityTimeout) != err {
		return &ReceiveMessageOutput[T]{}, &EmptyQueueError{}
	}
	expr, err = expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Add(expression.Name("receive_count"), expression.Value(1)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(selectedItem.LastUpdatedTimestamp)).
			Set(expression.Name("queue_peek_timestamp"), expression.Value(selectedItem.PeekFromQueueTimestamp)).
			Set(expression.Name("status"), expression.Value(selectedItem.Status))).
		WithCondition(expression.Name("version").Equal(expression.Value(selectedItem.Version))).
		Build()
	if err != nil {
		return &ReceiveMessageOutput[T]{}, &BuildingExpressionError{Cause: err}
	}
	peeked, err := c.updateDynamoDBItem(ctx, selectedItem.ID, &expr)
	if err != nil {
		return &ReceiveMessageOutput[T]{}, err
	}
	return &ReceiveMessageOutput[T]{
		Result: &Result{
			ID:                   peeked.ID,
			Status:               peeked.Status,
			LastUpdatedTimestamp: peeked.LastUpdatedTimestamp,
			Version:              peeked.Version,
		},
		PeekFromQueueTimestamp: peeked.PeekFromQueueTimestamp,
		PeekedMessageObject:    peeked,
	}, nil
}

type UpdateMessageAsVisibleInput struct {
	ID string
}

// UpdateMessageAsVisibleOutput represents the result for the UpdateMessageAsVisible() API call.
type UpdateMessageAsVisibleOutput[T any] struct {
	*Result             // Embedded type for inheritance-like behavior in Go
	Message *Message[T] `json:"-"`
}

func (c *client[T]) UpdateMessageAsVisible(ctx context.Context, params *UpdateMessageAsVisibleInput) (*UpdateMessageAsVisibleOutput[T], error) {
	if params == nil {
		params = &UpdateMessageAsVisibleInput{}
	}
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: params.ID,
	})
	if err != nil {
		return &UpdateMessageAsVisibleOutput[T]{}, err
	}
	if retrieved.Message == nil {
		return &UpdateMessageAsVisibleOutput[T]{}, &IDNotFoundError{}
	}
	message := retrieved.Message
	err = message.Ready(c.clock.Now())
	if err != nil {
		return &UpdateMessageAsVisibleOutput[T]{}, err
	}
	expr, err := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(message.LastUpdatedTimestamp)).
			Set(expression.Name("status"), expression.Value(message.Status))).
		WithCondition(expression.Name("version").Equal(expression.Value(message.Version))).
		Build()
	if err != nil {
		return &UpdateMessageAsVisibleOutput[T]{}, &BuildingExpressionError{Cause: err}
	}
	retried, err := c.updateDynamoDBItem(ctx, message.ID, &expr)
	if err != nil {
		return &UpdateMessageAsVisibleOutput[T]{}, err
	}
	return &UpdateMessageAsVisibleOutput[T]{
		Result: &Result{
			ID:                   retried.ID,
			Status:               retried.Status,
			LastUpdatedTimestamp: retried.LastUpdatedTimestamp,
			Version:              retried.Version,
		},
		Message: retried,
	}, nil
}

type DeleteMessageInput struct {
	ID string
}

type DeleteMessageOutput struct{}

func (c *client[T]) DeleteMessage(ctx context.Context, params *DeleteMessageInput) (*DeleteMessageOutput, error) {
	if params == nil {
		params = &DeleteMessageInput{}
	}
	out := &DeleteMessageOutput{}
	if params.ID == "" {
		return out, &IDNotProvidedError{}
	}
	_, err := c.dynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &c.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: params.ID,
			},
		},
	})
	if err != nil {
		return out, handleDynamoDBError(err)
	}
	return out, nil
}

type MoveMessageToDLQInput struct {
	ID string
}

type MoveMessageToDLQOutput struct {
	ID                   string `json:"id"`
	Status               Status `json:"status"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp"`
	Version              int    `json:"version"`
}

func (c *client[T]) MoveMessageToDLQ(ctx context.Context, params *MoveMessageToDLQInput) (*MoveMessageToDLQOutput, error) {
	if params == nil {
		params = &MoveMessageToDLQInput{}
	}
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: params.ID,
	})
	if err != nil {
		return &MoveMessageToDLQOutput{}, err
	}
	if retrieved.Message == nil {
		return &MoveMessageToDLQOutput{}, &IDNotFoundError{}
	}
	message := retrieved.Message
	if message.IsDLQ() {
		return &MoveMessageToDLQOutput{
			ID:                   params.ID,
			Status:               message.Status,
			LastUpdatedTimestamp: message.LastUpdatedTimestamp,
			Version:              message.Version,
		}, nil
	}
	err = message.MoveToDLQ(c.clock.Now())
	if err != nil {
		return &MoveMessageToDLQOutput{}, err
	}
	expr, err := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Set(expression.Name("status"), expression.Value(message.Status)).
			Set(expression.Name("receive_count"), expression.Value(message.ReceiveCount)).
			Set(expression.Name("queue_type"), expression.Value(message.QueueType)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(message.LastUpdatedTimestamp)).
			Set(expression.Name("queue_add_timestamp"), expression.Value(message.AddToQueueTimestamp)).
			Set(expression.Name(" queue_peek_timestamp"), expression.Value(message.AddToQueueTimestamp))).
		WithCondition(expression.Name("version").Equal(expression.Value(message.Version))).
		Build()
	if err != nil {
		return &MoveMessageToDLQOutput{}, &BuildingExpressionError{Cause: err}
	}
	item, err := c.updateDynamoDBItem(ctx, params.ID, &expr)
	if err != nil {
		return &MoveMessageToDLQOutput{}, err
	}
	return &MoveMessageToDLQOutput{
		ID:                   params.ID,
		Status:               item.Status,
		LastUpdatedTimestamp: item.LastUpdatedTimestamp,
		Version:              item.Version,
	}, nil
}

type RedriveMessageInput struct {
	ID string
}

type RedriveMessageOutput struct {
	ID                   string `json:"id"`
	Status               Status `json:"status"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp"`
	Version              int    `json:"version"`
}

func (c *client[T]) RedriveMessage(ctx context.Context, params *RedriveMessageInput) (*RedriveMessageOutput, error) {
	if params == nil {
		params = &RedriveMessageInput{}
	}
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: params.ID,
	})
	if err != nil {
		return &RedriveMessageOutput{}, err
	}
	if retrieved.Message == nil {
		return &RedriveMessageOutput{}, &IDNotFoundError{}
	}
	message := retrieved.Message
	err = message.RestoreFromDLQ(c.clock.Now())
	if err != nil {
		return &RedriveMessageOutput{}, err
	}
	expr, err := expression.NewBuilder().
		WithUpdate(expression.Add(
			expression.Name("version"),
			expression.Value(1),
		).Set(
			expression.Name("queue_type"),
			expression.Value(message.QueueType),
		).Set(
			expression.Name("status"),
			expression.Value(message.Status),
		).Set(
			expression.Name("last_updated_timestamp"),
			expression.Value(message.LastUpdatedTimestamp),
		).Set(
			expression.Name("queue_add_timestamp"),
			expression.Value(message.AddToQueueTimestamp),
		)).
		WithCondition(expression.Name("version").
			Equal(expression.Value(message.Version))).
		Build()
	if err != nil {
		return nil, &BuildingExpressionError{Cause: err}
	}
	updated, err := c.updateDynamoDBItem(ctx, params.ID, &expr)
	if err != nil {
		return &RedriveMessageOutput{}, err
	}
	return &RedriveMessageOutput{
		ID:                   updated.ID,
		Status:               updated.Status,
		LastUpdatedTimestamp: updated.LastUpdatedTimestamp,
		Version:              updated.Version,
	}, nil
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
func (c *client[T]) GetQueueStats(ctx context.Context) (*QueueStats, error) {
	expr, err := expression.NewBuilder().
		WithKeyCondition(expression.KeyEqual(expression.Key("queue_type"), expression.Value(QueueTypeStandard))).
		Build()
	if err != nil {
		return nil, &BuildingExpressionError{Cause: err}
	}
	var totalQueueSize int
	var exclusiveStartKey map[string]types.AttributeValue
	var peekedRecords int
	allQueueIDs := make([]string, 0)
	processingIDs := make([]string, 0)
	for {
		queryOutput, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(QueueingIndexName),
			TableName:                 aws.String(c.tableName),
			ExpressionAttributeNames:  expr.Names(),
			KeyConditionExpression:    expr.KeyCondition(),
			ScanIndexForward:          aws.Bool(true),
			Limit:                     aws.Int32(250),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return nil, handleDynamoDBError(err)
		}
		exclusiveStartKey = queryOutput.LastEvaluatedKey
		for _, itemMap := range queryOutput.Items {
			totalQueueSize++
			item := Message[T]{}
			err := attributevalue.UnmarshalMap(itemMap, &item)
			if err != nil {
				return nil, &UnmarshalingAttributeError{Cause: err}
			}
			if item.Status == StatusProcessing {
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
	return &QueueStats{
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
func (c *client[T]) GetDLQStats(ctx context.Context) (*DLQStats, error) {
	expr, err := expression.NewBuilder().
		WithKeyCondition(expression.KeyEqual(expression.Key("queue_type"), expression.Value(QueueTypeDLQ))).
		Build()
	if err != nil {
		return nil, &BuildingExpressionError{Cause: err}
	}
	var totalDLQSize int
	var lastEvaluatedKey map[string]types.AttributeValue
	listBANs := make([]string, 0)
	for {
		resp, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(QueueingIndexName),
			TableName:                 aws.String(c.tableName),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			KeyConditionExpression:    expr.KeyCondition(),
			Limit:                     aws.Int32(250),
			ScanIndexForward:          aws.Bool(true),
			ExclusiveStartKey:         lastEvaluatedKey,
		})
		if err != nil {
			return nil, handleDynamoDBError(err)
		}
		lastEvaluatedKey = resp.LastEvaluatedKey
		for _, itemMap := range resp.Items {
			totalDLQSize++
			if len(listBANs) < 100 {
				item := Message[T]{}
				err := attributevalue.UnmarshalMap(itemMap, &item)
				if err != nil {
					return nil, &UnmarshalingAttributeError{Cause: err}
				}
				listBANs = append(listBANs, item.ID)
			}
		}
		if lastEvaluatedKey == nil {
			break
		}
	}
	return &DLQStats{
		First100IDsInQueue: listBANs,
		TotalRecordsInDLQ:  totalDLQSize,
	}, nil
}

type GetMessageInput struct {
	ID string
}

type GetMessageOutput[T any] struct {
	Message *Message[T]
}

func (c *client[T]) GetMessage(ctx context.Context, params *GetMessageInput) (*GetMessageOutput[T], error) {
	if params == nil {
		params = &GetMessageInput{}
	}
	if params.ID == "" {
		return &GetMessageOutput[T]{}, &IDNotProvidedError{}
	}
	resp, err := c.dynamoDB.GetItem(ctx, &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: params.ID},
		},
		TableName:      aws.String(c.tableName),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return &GetMessageOutput[T]{}, handleDynamoDBError(err)
	}
	if resp.Item == nil {
		return &GetMessageOutput[T]{}, nil
	}
	item := Message[T]{}
	err = attributevalue.UnmarshalMap(resp.Item, &item)
	if err != nil {
		return &GetMessageOutput[T]{}, &UnmarshalingAttributeError{Cause: err}
	}
	return &GetMessageOutput[T]{
		Message: &item,
	}, nil
}

// Put stores a given Message object in a DynamoDB table using the PutImpl method.
// The object is stored in the table with its specified ID, creating a new entry if it
// doesn't exist. If an entry with the same ID exists, the method will delete it.
//
// Parameters:
//   - ctx: Context used for timeout, cancellation, and value sharing for the operation.
//   - message: The Message object to be stored.
//
// Returns:
//   - error: Returns an error if one occurs, otherwise, it returns nil on successful storage.
func (c *client[T]) Put(ctx context.Context, message *Message[T]) error {
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: message.ID,
	})
	if err != nil {
		return err
	}
	if retrieved.Message != nil {
		_, err := c.dynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(c.tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: message.ID},
			},
		})
		if err != nil {
			return handleDynamoDBError(err)
		}
	}
	return c.put(ctx, message)
}

// Upsert attempts to update an existing Message object in a DynamoDB table or inserts it
// if it doesn't exist. It uses the PutImpl method to perform this upsert operation.
// If an entry with the same ID exists, the method will update it.
//
// Parameters:
//   - ctx: Context used for timeout, cancellation, and value sharing for the operation.
//   - message: The Message object to be upserted.
//
// Returns:
//   - error: Returns an error if one occurs, otherwise, it returns nil on successful upsert.
func (c *client[T]) Upsert(ctx context.Context, message *Message[T]) error {
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: message.ID,
	})
	if err != nil {
		return err
	}
	if retrieved.Message != nil {
		retrieved.Message.Update(message, c.clock.Now())
		message = retrieved.Message
	}
	return c.put(ctx, message)
}

func (c *client[T]) put(ctx context.Context, message *Message[T]) error {
	item, err := message.MarshalMap()
	if err != nil {
		return err
	}
	_, err = c.dynamoDB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      item,
	})
	if err != nil {
		return handleDynamoDBError(err)
	}
	return nil
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
// *Result: A result object that contains updated values and status of the operation.
// - If the given 'id' does not exist, the 'ReturnValue' of the result will be set to 'ReturnStatusFailedIDNotFound'.
// - If the operation succeeds in updating the item, the 'ReturnValue' will be set to 'ReturnStatusSUCCESS'.
// - If there is an error while updating in DynamoDB, the 'ReturnValue' will be set to 'ReturnStatusFailedDynamoError'.
// error: An error object indicating any error that occurred during the operation.
// - If there's an error while building the DynamoDB expression, this error is returned.
// - If there's an error unmarshalling the DynamoDB response, this error is returned.
// Otherwise, if the operation succeeds, the error will be 'nil'.
func (c *client[T]) Touch(ctx context.Context, id string) (*Result, error) {
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: id,
	})
	if err != nil {
		return nil, err
	}
	if retrieved.Message == nil {
		return nil, &IDNotFoundError{}
	}
	message := retrieved.Message
	message.Touch(c.clock.Now())
	expr, err := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(message.LastUpdatedTimestamp))).
		WithCondition(expression.Name("version").Equal(expression.Value(message.Version))).
		Build()
	if err != nil {
		return nil, &BuildingExpressionError{Cause: err}
	}
	item, err := c.updateDynamoDBItem(ctx, id, &expr)
	if err != nil {
		return nil, err
	}
	return &Result{
		ID:                   id,
		Status:               item.Status,
		LastUpdatedTimestamp: item.LastUpdatedTimestamp,
		Version:              item.Version,
	}, nil
}

// List retrieves a list of Messages from the DynamoDB table up to the given size.
// The function constructs a DynamoDB scan with specific projection expressions and
// returns the list of found messages.
//
// Parameters:
//   - ctx: The context to use for the request.
//   - size: The maximum number of items to retrieve.
//
// Returns:
//   - A slice of pointers to Message if found.
//   - error if there's any issue in the operation.
func (c *client[T]) List(ctx context.Context, size int32) ([]*Message[T], error) {
	output, err := c.dynamoDB.Scan(ctx, &dynamodb.ScanInput{
		TableName: &c.tableName,
		Limit:     aws.Int32(size),
	})
	if err != nil {
		return nil, handleDynamoDBError(err)
	}
	var messages []*Message[T]
	err = attributevalue.UnmarshalListOfMaps(output.Items, &messages)
	if err != nil {
		return nil, &UnmarshalingAttributeError{Cause: err}
	}
	return messages, nil
}

// ListIDs retrieves a list of IDs from the Message items in the DynamoDB table
// up to the given size. It uses the List function to retrieve the messages and
// then extracts the IDs from them.
//
// Parameters:
//   - ctx: The context to use for the request.
//   - size: The maximum number of IDs to retrieve.
//
// Returns:
//   - A slice of string IDs if found.
//   - error if there's any issue in the operation.
func (c *client[T]) ListIDs(ctx context.Context, size int32) ([]string, error) {
	messages, err := c.List(ctx, size)
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(messages))
	for i, s := range messages {
		ids[i] = s.ID
	}

	return ids, nil
}

// ListExtendedIDs retrieves a list of extended IDs (formatted as "ID - status: STATUS")
// from the Message items in the DynamoDB table up to the given size.
// It uses the List function to retrieve the messages and then constructs
// the extended ID strings from them.
//
// Parameters:
//   - ctx: The context to use for the request.
//   - size: The maximum number of extended IDs to retrieve.
//
// Returns:
//   - A slice of extended ID strings if found.
//   - error if there's any issue in the operation.
func (c *client[T]) ListExtendedIDs(ctx context.Context, size int32) ([]string, error) {
	messages, err := c.List(ctx, size)
	if err != nil {
		return nil, err
	}

	extendedIDs := make([]string, len(messages))
	for i, s := range messages {
		extendedIDs[i] = fmt.Sprintf("ID: %s, status: %s", s.ID, s.Status)
	}

	return extendedIDs, nil
}

func (c *client[T]) GetDynamodbClient() *dynamodb.Client {
	return c.dynamoDB
}

func (c *client[T]) updateDynamoDBItem(ctx context.Context,
	id string, expr *expression.Expression) (*Message[T], error) {
	outcome, err := c.dynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: id,
			},
		},
		TableName:                 aws.String(c.tableName),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueAllNew,
	})
	if err != nil {
		return nil, handleDynamoDBError(err)
	}
	message := Message[T]{}
	err = attributevalue.UnmarshalMap(outcome.Attributes, &message)
	if err != nil {
		return nil, &UnmarshalingAttributeError{Cause: err}
	}
	return &message, nil
}

func handleDynamoDBError(err error) error {
	var cause *types.ConditionalCheckFailedException
	if errors.As(err, &cause) {
		return &ConditionalCheckFailedError{Cause: cause}
	}
	return &DynamoDBAPIError{Cause: err}
}

type Status string

const (
	StatusReady      Status = "READY"
	StatusProcessing Status = "PROCESSING"
)

type QueueType string

const (
	QueueTypeStandard QueueType = "STANDARD"
	QueueTypeDLQ      QueueType = "DLQ"
)

type Result struct {
	ID                   string `json:"id"`
	Status               Status `json:"status"`
	LastUpdatedTimestamp string `json:"last_updated_timestamp"`
	Version              int    `json:"version"`
}

// QueueStats represents the structure to store Queue depth statistics.
type QueueStats struct {
	First100IDsInQueue         []string `json:"first_100_IDs_in_queue"`
	First100SelectedIDsInQueue []string `json:"first_100_selected_IDs_in_queue"`
	TotalRecordsInQueue        int      `json:"total_records_in_queue"`
	TotalRecordsInProcessing   int      `json:"total_records_in_queue_selected_for_processing"`
	TotalRecordsNotStarted     int      `json:"total_records_in_queue_pending_for_processing"`
}

// DLQStats represents the structure to store DLQ depth statistics.
type DLQStats struct {
	First100IDsInQueue []string `json:"first_100_IDs_in_queue"`
	TotalRecordsInDLQ  int      `json:"total_records_in_DLQ"`
}

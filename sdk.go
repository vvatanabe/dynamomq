package dynamomq

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/dynamomq/internal/clock"
)

const (
	DefaultTableName                  = "dynamo-mq-table"
	DefaultQueueingIndexName          = "dynamo-mq-index-queue_type-queue_add_timestamp"
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
	GetQueueStats(ctx context.Context, params *GetQueueStatsInput) (*GetQueueStatsOutput, error)
	GetDLQStats(ctx context.Context, params *GetDLQStatsInput) (*GetDLQStatsOutput, error)
	ListMessages(ctx context.Context, params *ListMessagesInput) (*ListMessagesOutput[T], error)
	ReplaceMessage(ctx context.Context, params *ReplaceMessageInput[T]) (*ReplaceMessageOutput, error)
	GetDynamodbClient() *dynamodb.Client
}

type ClientOptions struct {
	DynamoDB                   *dynamodb.Client
	TableName                  string
	VisibilityTimeoutInMinutes int
	MaximumReceives            int
	UseFIFO                    bool
	Clock                      clock.Clock

	BaseEndpoint     string
	RetryMaxAttempts int
}

func WithAWSDynamoDBClient(client *dynamodb.Client) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.DynamoDB = client
	}
}

func WithTableName(tableName string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.TableName = tableName
	}
}

func WithAWSVisibilityTimeout(minutes int) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.VisibilityTimeoutInMinutes = minutes
	}
}

func WithUseFIFO(useFIFO bool) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.UseFIFO = useFIFO
	}
}

func WithAWSBaseEndpoint(baseEndpoint string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.BaseEndpoint = baseEndpoint
	}
}

func WithAWSRetryMaxAttempts(retryMaxAttempts int) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.RetryMaxAttempts = retryMaxAttempts
	}
}

func NewFromConfig[T any](cfg aws.Config, optFns ...func(*ClientOptions)) (Client[T], error) {
	o := &ClientOptions{
		TableName:                  DefaultTableName,
		RetryMaxAttempts:           DefaultRetryMaxAttempts,
		VisibilityTimeoutInMinutes: DefaultVisibilityTimeoutInMinutes,
		UseFIFO:                    false,
		Clock:                      &clock.RealClock{},
	}
	for _, opt := range optFns {
		opt(o)
	}
	c := &client[T]{
		tableName:                  o.TableName,
		visibilityTimeoutInMinutes: o.VisibilityTimeoutInMinutes,
		maximumReceives:            o.MaximumReceives,
		useFIFO:                    o.UseFIFO,
		dynamoDB:                   o.DynamoDB,
		clock:                      o.Clock,
	}
	if c.dynamoDB != nil {
		return c, nil
	}
	c.dynamoDB = dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.RetryMaxAttempts = o.RetryMaxAttempts
		if o.BaseEndpoint != "" {
			options.BaseEndpoint = aws.String(o.BaseEndpoint)
		}
	})
	return c, nil
}

type client[T any] struct {
	dynamoDB                   *dynamodb.Client
	tableName                  string
	visibilityTimeoutInMinutes int
	maximumReceives            int
	useFIFO                    bool
	clock                      clock.Clock
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
			IndexName:                 aws.String(DefaultQueueingIndexName),
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

type GetQueueStatsInput struct{}

// GetQueueStatsOutput represents the structure to store Queue depth statistics.
type GetQueueStatsOutput struct {
	First100IDsInQueue         []string `json:"first_100_IDs_in_queue"`
	First100SelectedIDsInQueue []string `json:"first_100_selected_IDs_in_queue"`
	TotalRecordsInQueue        int      `json:"total_records_in_queue"`
	TotalRecordsInProcessing   int      `json:"total_records_in_queue_selected_for_processing"`
	TotalRecordsNotStarted     int      `json:"total_records_in_queue_pending_for_processing"`
}

func (c *client[T]) GetQueueStats(ctx context.Context, params *GetQueueStatsInput) (*GetQueueStatsOutput, error) {
	if params == nil {
		params = &GetQueueStatsInput{}
	}
	expr, err := expression.NewBuilder().
		WithKeyCondition(expression.KeyEqual(expression.Key("queue_type"), expression.Value(QueueTypeStandard))).
		Build()
	if err != nil {
		return &GetQueueStatsOutput{}, &BuildingExpressionError{Cause: err}
	}
	var totalQueueSize int
	var exclusiveStartKey map[string]types.AttributeValue
	var peekedRecords int
	allQueueIDs := make([]string, 0)
	processingIDs := make([]string, 0)
	for {
		queryOutput, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(DefaultQueueingIndexName),
			TableName:                 aws.String(c.tableName),
			ExpressionAttributeNames:  expr.Names(),
			KeyConditionExpression:    expr.KeyCondition(),
			ScanIndexForward:          aws.Bool(true),
			Limit:                     aws.Int32(250),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return &GetQueueStatsOutput{}, handleDynamoDBError(err)
		}
		exclusiveStartKey = queryOutput.LastEvaluatedKey
		for _, itemMap := range queryOutput.Items {
			totalQueueSize++
			item := Message[T]{}
			err := attributevalue.UnmarshalMap(itemMap, &item)
			if err != nil {
				return &GetQueueStatsOutput{}, &UnmarshalingAttributeError{Cause: err}
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
	return &GetQueueStatsOutput{
		TotalRecordsInProcessing:   peekedRecords,
		TotalRecordsInQueue:        totalQueueSize,
		TotalRecordsNotStarted:     totalQueueSize - peekedRecords,
		First100IDsInQueue:         allQueueIDs,
		First100SelectedIDsInQueue: processingIDs,
	}, nil
}

type GetDLQStatsInput struct{}

// GetDLQStatsOutput represents the structure to store DLQ depth statistics.
type GetDLQStatsOutput struct {
	First100IDsInQueue []string `json:"first_100_IDs_in_queue"`
	TotalRecordsInDLQ  int      `json:"total_records_in_DLQ"`
}

func (c *client[T]) GetDLQStats(ctx context.Context, params *GetDLQStatsInput) (*GetDLQStatsOutput, error) {
	if params == nil {
		params = &GetDLQStatsInput{}
	}
	expr, err := expression.NewBuilder().
		WithKeyCondition(expression.KeyEqual(expression.Key("queue_type"), expression.Value(QueueTypeDLQ))).
		Build()
	if err != nil {
		return &GetDLQStatsOutput{}, &BuildingExpressionError{Cause: err}
	}
	var totalDLQSize int
	var lastEvaluatedKey map[string]types.AttributeValue
	listBANs := make([]string, 0)
	for {
		resp, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(DefaultQueueingIndexName),
			TableName:                 aws.String(c.tableName),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			KeyConditionExpression:    expr.KeyCondition(),
			Limit:                     aws.Int32(250),
			ScanIndexForward:          aws.Bool(true),
			ExclusiveStartKey:         lastEvaluatedKey,
		})
		if err != nil {
			return &GetDLQStatsOutput{}, handleDynamoDBError(err)
		}
		lastEvaluatedKey = resp.LastEvaluatedKey
		for _, itemMap := range resp.Items {
			totalDLQSize++
			if len(listBANs) < 100 {
				item := Message[T]{}
				err := attributevalue.UnmarshalMap(itemMap, &item)
				if err != nil {
					return &GetDLQStatsOutput{}, &UnmarshalingAttributeError{Cause: err}
				}
				listBANs = append(listBANs, item.ID)
			}
		}
		if lastEvaluatedKey == nil {
			break
		}
	}
	return &GetDLQStatsOutput{
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

type ListMessagesInput struct {
	Size int32
}

type ListMessagesOutput[T any] struct {
	Messages []*Message[T]
}

func (c *client[T]) ListMessages(ctx context.Context, params *ListMessagesInput) (*ListMessagesOutput[T], error) {
	if params == nil {
		params = &ListMessagesInput{}
	}
	output, err := c.dynamoDB.Scan(ctx, &dynamodb.ScanInput{
		TableName: &c.tableName,
		Limit:     aws.Int32(params.Size),
	})
	if err != nil {
		return &ListMessagesOutput[T]{}, handleDynamoDBError(err)
	}
	var messages []*Message[T]
	err = attributevalue.UnmarshalListOfMaps(output.Items, &messages)
	if err != nil {
		return &ListMessagesOutput[T]{}, &UnmarshalingAttributeError{Cause: err}
	}
	return &ListMessagesOutput[T]{Messages: messages}, nil
}

type ReplaceMessageInput[T any] struct {
	Message *Message[T]
}

type ReplaceMessageOutput struct {
}

func (c *client[T]) ReplaceMessage(ctx context.Context, params *ReplaceMessageInput[T]) (*ReplaceMessageOutput, error) {
	if params == nil {
		params = &ReplaceMessageInput[T]{
			Message: &Message[T]{},
		}
	}
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: params.Message.ID,
	})
	if err != nil {
		return &ReplaceMessageOutput{}, err
	}
	if retrieved.Message != nil {
		_, err := c.dynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(c.tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: params.Message.ID},
			},
		})
		if err != nil {
			return &ReplaceMessageOutput{}, handleDynamoDBError(err)
		}
	}
	return &ReplaceMessageOutput{}, c.put(ctx, params.Message)
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

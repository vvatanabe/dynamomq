package dynamomq

import (
	"context"
	"errors"
	"sort"

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
	DefaultVisibilityTimeoutInSeconds = 30
	DefaultMaxListMessages            = 10
	DefaultQueryLimit                 = 250
)

type Client[T any] interface {
	SendMessage(ctx context.Context, params *SendMessageInput[T]) (*SendMessageOutput[T], error)
	ReceiveMessage(ctx context.Context, params *ReceiveMessageInput) (*ReceiveMessageOutput[T], error)
	ChangeMessageVisibility(ctx context.Context, params *ChangeMessageVisibilityInput) (*ChangeMessageVisibilityOutput[T], error)
	DeleteMessage(ctx context.Context, params *DeleteMessageInput) (*DeleteMessageOutput, error)
	MoveMessageToDLQ(ctx context.Context, params *MoveMessageToDLQInput) (*MoveMessageToDLQOutput, error)
	RedriveMessage(ctx context.Context, params *RedriveMessageInput) (*RedriveMessageOutput, error)
	GetMessage(ctx context.Context, params *GetMessageInput) (*GetMessageOutput[T], error)
	GetQueueStats(ctx context.Context, params *GetQueueStatsInput) (*GetQueueStatsOutput, error)
	GetDLQStats(ctx context.Context, params *GetDLQStatsInput) (*GetDLQStatsOutput, error)
	ListMessages(ctx context.Context, params *ListMessagesInput) (*ListMessagesOutput[T], error)
	ReplaceMessage(ctx context.Context, params *ReplaceMessageInput[T]) (*ReplaceMessageOutput, error)
}

type ClientOptions struct {
	DynamoDB            *dynamodb.Client
	TableName           string
	QueueingIndexName   string
	MaximumReceives     int
	UseFIFO             bool
	BaseEndpoint        string
	RetryMaxAttempts    int
	Clock               clock.Clock
	MarshalMap          func(in interface{}) (map[string]types.AttributeValue, error)
	UnmarshalMap        func(m map[string]types.AttributeValue, out interface{}) error
	UnmarshalListOfMaps func(l []map[string]types.AttributeValue, out interface{}) error
	BuildExpression     func(b expression.Builder) (expression.Expression, error)
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

func WithQueueingIndexName(queueingIndexName string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.QueueingIndexName = queueingIndexName
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
		TableName:           DefaultTableName,
		QueueingIndexName:   DefaultQueueingIndexName,
		RetryMaxAttempts:    DefaultRetryMaxAttempts,
		UseFIFO:             false,
		Clock:               &clock.RealClock{},
		MarshalMap:          attributevalue.MarshalMap,
		UnmarshalMap:        attributevalue.UnmarshalMap,
		UnmarshalListOfMaps: attributevalue.UnmarshalListOfMaps,
		BuildExpression: func(b expression.Builder) (expression.Expression, error) {
			return b.Build()
		},
	}
	for _, opt := range optFns {
		opt(o)
	}
	c := &client[T]{
		tableName:           o.TableName,
		queueingIndexName:   o.QueueingIndexName,
		maximumReceives:     o.MaximumReceives,
		useFIFO:             o.UseFIFO,
		dynamoDB:            o.DynamoDB,
		clock:               o.Clock,
		marshalMap:          o.MarshalMap,
		unmarshalMap:        o.UnmarshalMap,
		unmarshalListOfMaps: o.UnmarshalListOfMaps,
		buildExpression:     o.BuildExpression,
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
	dynamoDB            *dynamodb.Client
	tableName           string
	queueingIndexName   string
	maximumReceives     int
	useFIFO             bool
	clock               clock.Clock
	marshalMap          func(in interface{}) (map[string]types.AttributeValue, error)
	unmarshalMap        func(m map[string]types.AttributeValue, out interface{}) error
	unmarshalListOfMaps func(l []map[string]types.AttributeValue, out interface{}) error
	buildExpression     func(b expression.Builder) (expression.Expression, error)
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
	now := c.clock.Now()
	message := NewMessage(params.ID, params.Data, now)
	err = c.put(ctx, message)
	if err != nil {
		return &SendMessageOutput[T]{}, err
	}
	return &SendMessageOutput[T]{
		Result: &Result{
			ID:                   message.ID,
			Status:               message.GetStatus(now),
			LastUpdatedTimestamp: message.LastUpdatedTimestamp,
			Version:              message.Version,
		},
		Message: message,
	}, nil
}

type ReceiveMessageInput struct {
	QueueType         QueueType
	VisibilityTimeout int
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
	if params.QueueType == "" {
		params.QueueType = QueueTypeStandard
	}

	selected, err := c.selectMessage(ctx, params)
	if err != nil {
		return &ReceiveMessageOutput[T]{}, err
	}

	updated, err := c.processSelectedMessage(ctx, selected)
	if err != nil {
		return &ReceiveMessageOutput[T]{}, err
	}

	return &ReceiveMessageOutput[T]{
		Result: &Result{
			ID:                   updated.ID,
			Status:               updated.GetStatus(c.clock.Now()),
			LastUpdatedTimestamp: updated.LastUpdatedTimestamp,
			Version:              updated.Version,
		},
		PeekFromQueueTimestamp: updated.PeekFromQueueTimestamp,
		PeekedMessageObject:    updated,
	}, nil
}

func (c *client[T]) selectMessage(ctx context.Context, params *ReceiveMessageInput) (*Message[T], error) {
	builder := expression.NewBuilder().
		WithKeyCondition(expression.Key("queue_type").Equal(expression.Value(params.QueueType)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return nil, BuildingExpressionError{Cause: err}
	}

	selected, err := c.executeQuery(ctx, params, expr)
	if err != nil {
		return nil, err
	}

	if selected == nil {
		return nil, &EmptyQueueError{}
	}
	return selected, nil
}

func (c *client[T]) executeQuery(ctx context.Context, params *ReceiveMessageInput, expr expression.Expression) (*Message[T], error) {
	var exclusiveStartKey map[string]types.AttributeValue
	var selectedItem *Message[T]
	for {
		queryResult, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(c.queueingIndexName),
			TableName:                 aws.String(c.tableName),
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Limit:                     aws.Int32(DefaultQueryLimit),
			ScanIndexForward:          aws.Bool(true),
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return nil, handleDynamoDBError(err)
		}

		exclusiveStartKey = queryResult.LastEvaluatedKey

		selectedItem, err = c.processQueryResult(params, queryResult)
		if err != nil {
			return nil, err
		}
		if selectedItem != nil || exclusiveStartKey == nil {
			break
		}
	}
	return selectedItem, nil
}

func (c *client[T]) processQueryResult(params *ReceiveMessageInput, queryResult *dynamodb.QueryOutput) (*Message[T], error) {
	var selected *Message[T]
	for _, itemMap := range queryResult.Items {
		message := Message[T]{}
		if err := c.unmarshalMap(itemMap, &message); err != nil {
			return nil, UnmarshalingAttributeError{Cause: err}
		}

		if err := message.markAsProcessing(c.clock.Now(), params.VisibilityTimeout); err == nil {
			selected = &message
			break
		}
		if c.useFIFO {
			return nil, &EmptyQueueError{}
		}
	}
	return selected, nil
}

func (c *client[T]) processSelectedMessage(ctx context.Context, message *Message[T]) (*Message[T], error) {
	builder := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Add(expression.Name("receive_count"), expression.Value(1)).
			Set(expression.Name("visibility_timeout"), expression.Value(message.VisibilityTimeout)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(message.LastUpdatedTimestamp)).
			Set(expression.Name("queue_peek_timestamp"), expression.Value(message.PeekFromQueueTimestamp))).
		WithCondition(expression.Name("version").Equal(expression.Value(message.Version)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return nil, BuildingExpressionError{Cause: err}
	}
	updated, err := c.updateDynamoDBItem(ctx, message.ID, &expr)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

type ChangeMessageVisibilityInput struct {
	ID                string
	VisibilityTimeout int
}

// ChangeMessageVisibilityOutput represents the result for the ChangeMessageVisibility() API call.
type ChangeMessageVisibilityOutput[T any] struct {
	*Result             // Embedded type for inheritance-like behavior in Go
	Message *Message[T] `json:"-"`
}

func (c *client[T]) ChangeMessageVisibility(ctx context.Context, params *ChangeMessageVisibilityInput) (*ChangeMessageVisibilityOutput[T], error) {
	if params == nil {
		params = &ChangeMessageVisibilityInput{}
	}
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: params.ID,
	})
	if err != nil {
		return &ChangeMessageVisibilityOutput[T]{}, err
	}
	if retrieved.Message == nil {
		return &ChangeMessageVisibilityOutput[T]{}, &IDNotFoundError{}
	}
	message := retrieved.Message
	message.changeVisibilityTimeout(c.clock.Now(), params.VisibilityTimeout)
	builder := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(message.LastUpdatedTimestamp)).
			Set(expression.Name("visibility_timeout"), expression.Value(message.VisibilityTimeout))).
		WithCondition(expression.Name("version").Equal(expression.Value(message.Version)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return &ChangeMessageVisibilityOutput[T]{}, BuildingExpressionError{Cause: err}
	}
	retried, err := c.updateDynamoDBItem(ctx, message.ID, &expr)
	if err != nil {
		return &ChangeMessageVisibilityOutput[T]{}, err
	}
	return &ChangeMessageVisibilityOutput[T]{
		Result: &Result{
			ID:                   retried.ID,
			Status:               retried.GetStatus(c.clock.Now()),
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
	if markedErr := message.markAsMovedToDLQ(c.clock.Now()); markedErr != nil {
		//lint:ignore nilerr reason
		return &MoveMessageToDLQOutput{
			ID:                   params.ID,
			Status:               message.GetStatus(c.clock.Now()),
			LastUpdatedTimestamp: message.LastUpdatedTimestamp,
			Version:              message.Version,
		}, nil
	}
	builder := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Set(expression.Name("visibility_timeout"), expression.Value(message.VisibilityTimeout)).
			Set(expression.Name("receive_count"), expression.Value(message.ReceiveCount)).
			Set(expression.Name("queue_type"), expression.Value(message.QueueType)).
			Set(expression.Name("last_updated_timestamp"), expression.Value(message.LastUpdatedTimestamp)).
			Set(expression.Name("queue_add_timestamp"), expression.Value(message.AddToQueueTimestamp)).
			Set(expression.Name("queue_peek_timestamp"), expression.Value(message.AddToQueueTimestamp))).
		WithCondition(expression.Name("version").Equal(expression.Value(message.Version)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return &MoveMessageToDLQOutput{}, BuildingExpressionError{Cause: err}
	}
	updated, err := c.updateDynamoDBItem(ctx, params.ID, &expr)
	if err != nil {
		return &MoveMessageToDLQOutput{}, err
	}
	return &MoveMessageToDLQOutput{
		ID:                   params.ID,
		Status:               updated.GetStatus(c.clock.Now()),
		LastUpdatedTimestamp: updated.LastUpdatedTimestamp,
		Version:              updated.Version,
	}, nil
}

type RedriveMessageInput struct {
	ID                string
	VisibilityTimeout int
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
	err = message.markAsRestoredFromDLQ(c.clock.Now(), params.VisibilityTimeout)
	if err != nil {
		return &RedriveMessageOutput{}, err
	}
	builder := expression.NewBuilder().
		WithUpdate(expression.Add(
			expression.Name("version"),
			expression.Value(1),
		).Set(
			expression.Name("queue_type"),
			expression.Value(message.QueueType),
		).Set(
			expression.Name("visibility_timeout"),
			expression.Value(message.VisibilityTimeout),
		).Set(
			expression.Name("last_updated_timestamp"),
			expression.Value(message.LastUpdatedTimestamp),
		).Set(
			expression.Name("queue_add_timestamp"),
			expression.Value(message.AddToQueueTimestamp),
		)).
		WithCondition(expression.Name("version").
			Equal(expression.Value(message.Version)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return nil, BuildingExpressionError{Cause: err}
	}
	updated, err := c.updateDynamoDBItem(ctx, params.ID, &expr)
	if err != nil {
		return &RedriveMessageOutput{}, err
	}
	return &RedriveMessageOutput{
		ID:                   updated.ID,
		Status:               updated.GetStatus(c.clock.Now()),
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

func (c *client[T]) GetQueueStats(ctx context.Context, _ *GetQueueStatsInput) (*GetQueueStatsOutput, error) {
	builder := expression.NewBuilder().
		WithKeyCondition(expression.KeyEqual(expression.Key("queue_type"), expression.Value(QueueTypeStandard)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return &GetQueueStatsOutput{}, BuildingExpressionError{Cause: err}
	}

	stats, err := c.queryAndCalculateQueueStats(ctx, expr)
	if err != nil {
		return &GetQueueStatsOutput{}, err
	}

	return stats, nil
}

func (c *client[T]) queryAndCalculateQueueStats(ctx context.Context, expr expression.Expression) (*GetQueueStatsOutput, error) {
	var (
		stats = &GetQueueStatsOutput{
			First100IDsInQueue:         make([]string, 0),
			First100SelectedIDsInQueue: make([]string, 0),
			TotalRecordsInQueue:        0,
			TotalRecordsInProcessing:   0,
			TotalRecordsNotStarted:     0,
		}
		exclusiveStartKey map[string]types.AttributeValue
	)

	for {
		queryOutput, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(c.queueingIndexName),
			TableName:                 aws.String(c.tableName),
			ExpressionAttributeNames:  expr.Names(),
			KeyConditionExpression:    expr.KeyCondition(),
			ScanIndexForward:          aws.Bool(true),
			Limit:                     aws.Int32(DefaultQueryLimit),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return nil, handleDynamoDBError(err)
		}
		exclusiveStartKey = queryOutput.LastEvaluatedKey

		err = c.processQueryItemsForQueueStats(queryOutput.Items, stats)
		if err != nil {
			return nil, err
		}

		if exclusiveStartKey == nil {
			break
		}
	}
	stats.TotalRecordsNotStarted = stats.TotalRecordsInQueue - stats.TotalRecordsInProcessing
	return stats, nil
}

func (c *client[T]) processQueryItemsForQueueStats(items []map[string]types.AttributeValue, stats *GetQueueStatsOutput) error {
	for _, itemMap := range items {
		stats.TotalRecordsInQueue++
		item := Message[T]{}
		err := c.unmarshalMap(itemMap, &item)
		if err != nil {
			return UnmarshalingAttributeError{Cause: err}
		}

		c.updateQueueStatsFromItem(&item, stats)
	}
	return nil
}

const maxFirst100ItemsInQueue = 100

func (c *client[T]) updateQueueStatsFromItem(message *Message[T], stats *GetQueueStatsOutput) {
	if message.GetStatus(c.clock.Now()) == StatusProcessing {
		stats.TotalRecordsInProcessing++
		if len(stats.First100SelectedIDsInQueue) < maxFirst100ItemsInQueue {
			stats.First100SelectedIDsInQueue = append(stats.First100SelectedIDsInQueue, message.ID)
		}
	}
	if len(stats.First100IDsInQueue) < maxFirst100ItemsInQueue {
		stats.First100IDsInQueue = append(stats.First100IDsInQueue, message.ID)
	}
}

type GetDLQStatsInput struct{}

// GetDLQStatsOutput represents the structure to store DLQ depth statistics.
type GetDLQStatsOutput struct {
	First100IDsInQueue []string `json:"first_100_IDs_in_queue"`
	TotalRecordsInDLQ  int      `json:"total_records_in_DLQ"`
}

func (c *client[T]) GetDLQStats(ctx context.Context, _ *GetDLQStatsInput) (*GetDLQStatsOutput, error) {
	builder := expression.NewBuilder().
		WithKeyCondition(expression.KeyEqual(expression.Key("queue_type"), expression.Value(QueueTypeDLQ)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return &GetDLQStatsOutput{}, BuildingExpressionError{Cause: err}
	}

	stats, err := c.queryAndCalculateDLQStats(ctx, expr)
	if err != nil {
		return &GetDLQStatsOutput{}, err
	}

	return stats, nil
}

func (c *client[T]) queryAndCalculateDLQStats(ctx context.Context, expr expression.Expression) (*GetDLQStatsOutput, error) {
	var (
		stats = &GetDLQStatsOutput{
			First100IDsInQueue: make([]string, 0),
			TotalRecordsInDLQ:  0,
		}
		lastEvaluatedKey map[string]types.AttributeValue
	)
	for {
		queryOutput, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(c.queueingIndexName),
			TableName:                 aws.String(c.tableName),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			KeyConditionExpression:    expr.KeyCondition(),
			Limit:                     aws.Int32(DefaultQueryLimit),
			ScanIndexForward:          aws.Bool(true),
			ExclusiveStartKey:         lastEvaluatedKey,
		})
		if err != nil {
			return &GetDLQStatsOutput{}, handleDynamoDBError(err)
		}
		lastEvaluatedKey = queryOutput.LastEvaluatedKey

		err = c.processQueryItemsForDLQStats(queryOutput.Items, stats)
		if err != nil {
			return nil, err
		}

		if lastEvaluatedKey == nil {
			break
		}
	}
	return stats, nil
}

func (c *client[T]) processQueryItemsForDLQStats(items []map[string]types.AttributeValue, stats *GetDLQStatsOutput) error {
	for _, itemMap := range items {
		stats.TotalRecordsInDLQ++
		if len(stats.First100IDsInQueue) < maxFirst100ItemsInQueue {
			item := Message[T]{}
			err := c.unmarshalMap(itemMap, &item)
			if err != nil {
				return UnmarshalingAttributeError{Cause: err}
			}
			stats.First100IDsInQueue = append(stats.First100IDsInQueue, item.ID)
		}
	}
	return nil
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
	err = c.unmarshalMap(resp.Item, &item)
	if err != nil {
		return &GetMessageOutput[T]{}, UnmarshalingAttributeError{Cause: err}
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
	if params.Size <= 0 {
		params.Size = DefaultMaxListMessages
	}
	output, err := c.dynamoDB.Scan(ctx, &dynamodb.ScanInput{
		TableName: &c.tableName,
		Limit:     aws.Int32(params.Size),
	})
	if err != nil {
		return &ListMessagesOutput[T]{}, handleDynamoDBError(err)
	}
	var messages []*Message[T]
	err = c.unmarshalListOfMaps(output.Items, &messages)
	if err != nil {
		return &ListMessagesOutput[T]{}, UnmarshalingAttributeError{Cause: err}
	}
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].LastUpdatedTimestamp < messages[j].LastUpdatedTimestamp
	})
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
		_, delErr := c.dynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(c.tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: params.Message.ID},
			},
		})
		if delErr != nil {
			return &ReplaceMessageOutput{}, handleDynamoDBError(delErr)
		}
	}
	return &ReplaceMessageOutput{}, c.put(ctx, params.Message)
}

func (c *client[T]) put(ctx context.Context, message *Message[T]) error {
	item, err := c.marshalMap(message)
	if err != nil {
		return MarshalingAttributeError{Cause: err}
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
	err = c.unmarshalMap(outcome.Attributes, &message)
	if err != nil {
		return nil, UnmarshalingAttributeError{Cause: err}
	}
	return &message, nil
}

func handleDynamoDBError(err error) error {
	var cause *types.ConditionalCheckFailedException
	if errors.As(err, &cause) {
		return &ConditionalCheckFailedError{Cause: cause}
	}
	return DynamoDBAPIError{Cause: err}
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

package dynamomq

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/constant"
)

const (
	defaultQueryLimit       = 250
	maxFirstMessagesInQueue = 100
)

// Client is an interface for interacting with a DynamoDB-based message queue system.
// It provides methods for various operations on messages within the queue.
// This interface is generic and works with any type T, which represents the structure of the message content.
type Client[T any] interface {
	// SendMessage sends a message to the DynamoDB-based queue.
	SendMessage(ctx context.Context, params *SendMessageInput[T]) (*SendMessageOutput[T], error)
	// ReceiveMessage retrieves and processes a message from a DynamoDB-based queue.
	ReceiveMessage(ctx context.Context, params *ReceiveMessageInput) (*ReceiveMessageOutput[T], error)
	// ChangeMessageVisibility changes the visibility of a specific message in a DynamoDB-based queue.
	ChangeMessageVisibility(ctx context.Context, params *ChangeMessageVisibilityInput) (*ChangeMessageVisibilityOutput[T], error)
	// DeleteMessage deletes a specific message from a DynamoDB-based queue.
	DeleteMessage(ctx context.Context, params *DeleteMessageInput) (*DeleteMessageOutput, error)
	// MoveMessageToDLQ moves a specific message from a DynamoDB-based queue to a Dead Letter Queue (DLQ).
	MoveMessageToDLQ(ctx context.Context, params *MoveMessageToDLQInput) (*MoveMessageToDLQOutput[T], error)
	// RedriveMessage restore a specific message from a DynamoDB-based Dead Letter Queue (DLQ).
	RedriveMessage(ctx context.Context, params *RedriveMessageInput) (*RedriveMessageOutput[T], error)
	// GetMessage get a specific message from a DynamoDB-based queue.
	GetMessage(ctx context.Context, params *GetMessageInput) (*GetMessageOutput[T], error)
	// GetQueueStats is a method for obtaining statistical information about a DynamoDB-based queue.
	GetQueueStats(ctx context.Context, params *GetQueueStatsInput) (*GetQueueStatsOutput, error)
	// GetDLQStats get statistical information about a DynamoDB-based Dead Letter Queue (DLQ).
	GetDLQStats(ctx context.Context, params *GetDLQStatsInput) (*GetDLQStatsOutput, error)
	// ListMessages get a list of messages from a DynamoDB-based queue.
	ListMessages(ctx context.Context, params *ListMessagesInput) (*ListMessagesOutput[T], error)
	// ReplaceMessage replace a specific message within a DynamoDB-based queue.
	ReplaceMessage(ctx context.Context, params *ReplaceMessageInput[T]) (*ReplaceMessageOutput, error)
}

// ClientOptions defines configuration options for the DynamoMQ client.
//
// Note: The following fields are primarily used for testing purposes.
// They allow for stubbing of operations during tests, facilitating the mocking of behavior without relying on a real DynamoDB instance:
//
//   - Clock
//   - MarshalMap
//   - UnmarshalMap
//   - UnmarshalListOfMaps
//   - BuildExpression
//
// In typical use, these testing fields should not be modified. They are provided to support advanced use cases, like unit testing, where control over these operations is necessary.
type ClientOptions struct {
	// DynamoDB is a pointer to the DynamoDB client used for database operations.
	DynamoDB *dynamodb.Client
	// TableName is the name of the DynamoDB table used for the queue.
	TableName string
	// QueueingIndexName is the name of the index used for queueing operations.
	QueueingIndexName string
	// MaximumReceives is the maximum number of times a message is delivered before being moved to the DLQ.
	MaximumReceives int
	// UseFIFO is a boolean indicating if the queue should behave as a First-In-First-Out (FIFO) queue.
	UseFIFO bool
	// BaseEndpoint is the base endpoint URL for DynamoDB requests.
	BaseEndpoint string
	// RetryMaxAttempts is the maximum number of attempts for retrying failed DynamoDB operations.
	RetryMaxAttempts int

	// Clock is an abstraction of time operations, allowing control over time during tests.
	Clock clock.Clock
	// MarshalMap is a function to marshal objects into a map of DynamoDB attribute values.
	MarshalMap func(in interface{}) (map[string]types.AttributeValue, error)
	// UnmarshalMap is a function to unmarshal a map of DynamoDB attribute values into objects.
	UnmarshalMap func(m map[string]types.AttributeValue, out interface{}) error
	// UnmarshalListOfMaps is a function to unmarshal a list of maps of DynamoDB attribute values into objects.
	UnmarshalListOfMaps func(l []map[string]types.AttributeValue, out interface{}) error
	// BuildExpression is a function to build DynamoDB expressions from a builder.
	BuildExpression func(b expression.Builder) (expression.Expression, error)
}

// WithTableName is an option function to set the table name for the DynamoMQ client.
// Use this function to specify the name of the DynamoDB table that the client will use for storing and retrieving messages.
// By default, the table name is set to "dynamo-mq-table".
func WithTableName(tableName string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.TableName = tableName
	}
}

// WithQueueingIndexName is an option function to set the queue index name for the DynamoMQ client.
// This function allows defining a custom index name that the client will use for queue operations, optimizing message handling.
// By default, the index name is set to "dynamo-mq-index-queue_type-sent_at".
func WithQueueingIndexName(queueingIndexName string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.QueueingIndexName = queueingIndexName
	}
}

// WithUseFIFO is an option function to enable FIFO (First-In-First-Out) behavior for the DynamoMQ client.
// Setting this option to true makes the client treat the queue as a FIFO queue; otherwise, it is treated as a standard queue.
// By default, this option is set to false.
func WithUseFIFO(useFIFO bool) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.UseFIFO = useFIFO
	}
}

// WithAWSDynamoDBClient is an option function to set a custom AWS DynamoDB client for the DynamoMQ client.
// This function is used to provide a pre-configured DynamoDB client that the DynamoMQ client will use for all interactions with DynamoDB.
func WithAWSDynamoDBClient(client *dynamodb.Client) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.DynamoDB = client
	}
}

// WithAWSBaseEndpoint is an option function to set a custom base endpoint for AWS services.
// This function is useful when you want the client to interact with a specific AWS service endpoint, such as a local or a different regional endpoint.
// If the DynamoDB client is set using the WithAWSDynamoDBClient function, this option function is ignored.
func WithAWSBaseEndpoint(baseEndpoint string) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.BaseEndpoint = baseEndpoint
	}
}

// WithAWSRetryMaxAttempts is an option function to set the maximum number of retry attempts for AWS service calls.
// Use this function to define how many times the client should retry a failed AWS service call.
// If the DynamoDB client is set using the WithAWSDynamoDBClient function, this option function is ignored.
func WithAWSRetryMaxAttempts(retryMaxAttempts int) func(*ClientOptions) {
	return func(s *ClientOptions) {
		s.RetryMaxAttempts = retryMaxAttempts
	}
}

// NewFromConfig creates a new DynamoMQ client using the provided AWS configuration and any additional client options.
// This function initializes a new client with default settings, which can be customized using option functions.
// It returns an error if the initialization of the DynamoDB client fails.
func NewFromConfig[T any](cfg aws.Config, optFns ...func(*ClientOptions)) (Client[T], error) {
	o := &ClientOptions{
		TableName:           constant.DefaultTableName,
		QueueingIndexName:   constant.DefaultQueueingIndexName,
		RetryMaxAttempts:    constant.DefaultRetryMaxAttempts,
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
	c := &ClientImpl[T]{
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

// ClientImpl is a concrete implementation of the dynamomq.Client interface.
// Note: ClientImpl cannot be used directly. Always use the dynamomq.NewFromConfig function to create an instance.
type ClientImpl[T any] struct {
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

// SendMessageInput represents the input parameters for sending a message to a DynamoDB-based queue.
// This struct uses the generic type T, supporting messages of various data types.
type SendMessageInput[T any] struct {
	// ID is a unique identifier for the message.
	ID string
	// Data is the content of the message to be sent to the queue. The type T determines the format of the message.
	Data T
	// DelaySeconds is the delay time (in seconds) before the message is sent to the queue.
	DelaySeconds int
}

// SendMessageOutput represents the result of a message sending operation.
// This struct also uses the generic type T and contains information about the sent message.
type SendMessageOutput[T any] struct {
	// SentMessage is a pointer to the Message type containing information about the sent message.
	SentMessage *Message[T]
}

// SendMessage sends a message to the DynamoDB-based message queue. It checks for message ID duplication and handles message delays if specified.
// This function takes a context and a SendMessageInput parameter. SendMessageInput contains the message ID, data, and an optional delay in seconds.
// If the message ID already exists in the queue, it returns an IDDuplicatedError. Otherwise, it adds the message to the queue.
// The function also handles message delays. If DelaySeconds is greater than 0 in the input parameter, the message will be delayed accordingly before being sent.
func (c *ClientImpl[T]) SendMessage(ctx context.Context, params *SendMessageInput[T]) (*SendMessageOutput[T], error) {
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
	if params.DelaySeconds > 0 {
		message.delayToSentAt(time.Duration(params.DelaySeconds) * time.Second)
	}
	err = c.put(ctx, message)
	if err != nil {
		return &SendMessageOutput[T]{}, err
	}
	return &SendMessageOutput[T]{
		SentMessage: message,
	}, nil
}

// ReceiveMessageInput represents the input parameters for receiving a message from a DynamoDB-based queue.
type ReceiveMessageInput struct {
	// QueueType is the type of queue from which the message is to be retrieved. QueueType specifies the kind of queue, such as STANDARD or DLQ.
	QueueType QueueType
	// VisibilityTimeout is the timeout in seconds during which the message becomes invisible to other receivers.
	VisibilityTimeout int
}

// ReceiveMessageOutput represents the result of a message receiving operation.
// This struct uses the generic type T and contains information about the received message.
type ReceiveMessageOutput[T any] struct {
	// ReceivedMessage is A pointer to the Message type containing information about the received message.
	// The type T determines the format of the message content.
	ReceivedMessage *Message[T]
}

// ReceiveMessage retrieves and processes a message from a DynamoDB-based queue using the generic type T.
// The selection process involves constructing and executing a DynamoDB query based on the queue type and visibility timeout.
// After a message is selected, its status, including visibility and version, is updated to ensure the message remains invisible and in processing for a defined period. This process is crucial for maintaining queue integrity and preventing duplicate message delivery.
// If no messages are available for reception, an EmptyQueueError is returned. Additionally, when FIFO (First In, First Out) is enabled, the method guarantees that only one valid message is processed at a time.
func (c *ClientImpl[T]) ReceiveMessage(ctx context.Context, params *ReceiveMessageInput) (*ReceiveMessageOutput[T], error) {
	if params == nil {
		params = &ReceiveMessageInput{}
	}
	if params.QueueType == "" {
		params.QueueType = QueueTypeStandard
	}
	if params.VisibilityTimeout <= 0 {
		params.VisibilityTimeout = constant.DefaultVisibilityTimeoutInSeconds
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
		ReceivedMessage: updated,
	}, nil
}

func (c *ClientImpl[T]) selectMessage(ctx context.Context, params *ReceiveMessageInput) (*Message[T], error) {
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

func (c *ClientImpl[T]) executeQuery(ctx context.Context, params *ReceiveMessageInput, expr expression.Expression) (*Message[T], error) {
	var exclusiveStartKey map[string]types.AttributeValue
	var selectedItem *Message[T]
	for {
		queryResult, err := c.dynamoDB.Query(ctx, &dynamodb.QueryInput{
			IndexName:                 aws.String(c.queueingIndexName),
			TableName:                 aws.String(c.tableName),
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Limit:                     aws.Int32(defaultQueryLimit),
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

func (c *ClientImpl[T]) processQueryResult(params *ReceiveMessageInput, queryResult *dynamodb.QueryOutput) (*Message[T], error) {
	var selected *Message[T]
	for _, itemMap := range queryResult.Items {
		message := Message[T]{}
		if err := c.unmarshalMap(itemMap, &message); err != nil {
			return nil, UnmarshalingAttributeError{Cause: err}
		}

		if err := message.markAsProcessing(c.clock.Now(), secToDur(params.VisibilityTimeout)); err == nil {
			selected = &message
			break
		}
		if c.useFIFO {
			return nil, &EmptyQueueError{}
		}
	}
	return selected, nil
}

func (c *ClientImpl[T]) processSelectedMessage(ctx context.Context, message *Message[T]) (*Message[T], error) {
	builder := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Add(expression.Name("receive_count"), expression.Value(1)).
			Set(expression.Name("updated_at"), expression.Value(message.UpdatedAt)).
			Set(expression.Name("received_at"), expression.Value(message.ReceivedAt)).
			Set(expression.Name("invisible_until_at"), expression.Value(message.InvisibleUntilAt))).
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

// ChangeMessageVisibilityInput represents the input parameters for changing the visibility timeout of a specific message in a DynamoDB-based queue.
type ChangeMessageVisibilityInput struct {
	// ID is The unique identifier of the message for which visibility is to be changed.
	ID string
	// VisibilityTimeout is The new timeout in seconds during which the message becomes invisible to other receivers.
	// After this time elapses, the message will become visible in the queue again
	VisibilityTimeout int
}

// ChangeMessageVisibilityOutput represents the result of the operation to change the visibility of a message.
// This struct uses the generic type T and contains information about the message whose visibility has been changed.
type ChangeMessageVisibilityOutput[T any] struct {
	// ChangedMessage is a pointer to the Message type containing information about the message with changed visibility.
	// The type T determines the format of the message content.
	ChangedMessage *Message[T]
}

// ChangeMessageVisibility changes the visibility of a specific message in a DynamoDB-based queue.
// It retrieves the message based on the specified message ID and alters its visibility timeout.
// The visibility timeout specifies the duration during which the message, once retrieved from the queue, becomes invisible to other clients. Modifying this timeout value allows dynamic adjustment of the message processing time.
func (c *ClientImpl[T]) ChangeMessageVisibility(ctx context.Context, params *ChangeMessageVisibilityInput) (*ChangeMessageVisibilityOutput[T], error) {
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
	message.changeVisibility(c.clock.Now(), secToDur(params.VisibilityTimeout))
	builder := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Set(expression.Name("updated_at"), expression.Value(message.UpdatedAt)).
			Set(expression.Name("invisible_until_at"), expression.Value(message.InvisibleUntilAt))).
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
		ChangedMessage: retried,
	}, nil
}

// DeleteMessageInput represents the input parameters for deleting a specific message from a DynamoDB-based queue.
type DeleteMessageInput struct {
	// ID is the unique identifier of the message to be deleted from the queue.
	ID string
}

// DeleteMessageOutput represents the result of the delete message operation.
// This struct is empty as the delete operation does not return any specific information.
type DeleteMessageOutput struct{}

// DeleteMessage deletes a specific message from a DynamoDB-based queue.
// It directly deletes the message from DynamoDB based on the specified message ID.
func (c *ClientImpl[T]) DeleteMessage(ctx context.Context, params *DeleteMessageInput) (*DeleteMessageOutput, error) {
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

// MoveMessageToDLQInput represents the input parameters for moving a specific message from a DynamoDB-based queue to a Dead Letter Queue (DLQ).
type MoveMessageToDLQInput struct {
	// ID is the unique identifier of the message to be moved to the DLQ.
	ID string
}

// MoveMessageToDLQOutput represents the result of the operation to move a message to the DLQ.
// This struct uses the generic type T and contains information about the message that has been moved.
type MoveMessageToDLQOutput[T any] struct {
	// MovedMessage is a pointer to the Message type containing information about the moved message.
	// The type T determines the format of the message content.
	MovedMessage *Message[T]
}

// MoveMessageToDLQ moves a specific message from a DynamoDB-based queue to a Dead Letter Queue (DLQ).
// It locates the message based on the specified message ID and marks it for the DLQ.
// Moving a message to the DLQ allows for the isolation of failed message processing, facilitating later analysis and reprocessing.
func (c *ClientImpl[T]) MoveMessageToDLQ(ctx context.Context, params *MoveMessageToDLQInput) (*MoveMessageToDLQOutput[T], error) {
	if params == nil {
		params = &MoveMessageToDLQInput{}
	}
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: params.ID,
	})
	if err != nil {
		return &MoveMessageToDLQOutput[T]{}, err
	}
	if retrieved.Message == nil {
		return &MoveMessageToDLQOutput[T]{}, &IDNotFoundError{}
	}
	message := retrieved.Message
	if markedErr := message.markAsMovedToDLQ(c.clock.Now()); markedErr != nil {
		//lint:ignore nilerr reason
		return &MoveMessageToDLQOutput[T]{
			MovedMessage: message,
		}, nil
	}
	builder := expression.NewBuilder().
		WithUpdate(expression.
			Add(expression.Name("version"), expression.Value(1)).
			Set(expression.Name("receive_count"), expression.Value(message.ReceiveCount)).
			Set(expression.Name("queue_type"), expression.Value(message.QueueType)).
			Set(expression.Name("updated_at"), expression.Value(message.UpdatedAt)).
			Set(expression.Name("sent_at"), expression.Value(message.SentAt)).
			Set(expression.Name("received_at"), expression.Value(message.ReceivedAt)).
			Set(expression.Name("invisible_until_at"), expression.Value(message.InvisibleUntilAt))).
		WithCondition(expression.Name("version").Equal(expression.Value(message.Version)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return &MoveMessageToDLQOutput[T]{}, BuildingExpressionError{Cause: err}
	}
	updated, err := c.updateDynamoDBItem(ctx, params.ID, &expr)
	if err != nil {
		return &MoveMessageToDLQOutput[T]{}, err
	}
	return &MoveMessageToDLQOutput[T]{
		MovedMessage: updated,
	}, nil
}

// RedriveMessageInput represents the input parameters for restoring a specific message from a DynamoDB-based Dead Letter Queue (DLQ) back to the STANDARD queue.
type RedriveMessageInput struct {
	// ID is the unique identifier of the message to be redriven from the DLQ.
	ID string
}

// RedriveMessageOutput represents the result of the operation to redrive a message from the DLQ.
// This struct uses the generic type T and contains information about the message that has been restored.
type RedriveMessageOutput[T any] struct {
	// RedroveMessage is a pointer to the Message type containing information about the redriven message.
	// The type T determines the format of the message content.
	RedroveMessage *Message[T]
}

// RedriveMessage restore a specific message from a DynamoDB-based Dead Letter Queue (DLQ).
// It locates the message based on the specified message ID and marks it as restored from the DLQ to the standard queue.
// This process is essential for reprocessing messages that have failed to be processed and is a crucial function in error handling within the message queue system.
func (c *ClientImpl[T]) RedriveMessage(ctx context.Context, params *RedriveMessageInput) (*RedriveMessageOutput[T], error) {
	if params == nil {
		params = &RedriveMessageInput{}
	}
	retrieved, err := c.GetMessage(ctx, &GetMessageInput{
		ID: params.ID,
	})
	if err != nil {
		return &RedriveMessageOutput[T]{}, err
	}
	if retrieved.Message == nil {
		return &RedriveMessageOutput[T]{}, &IDNotFoundError{}
	}
	message := retrieved.Message
	err = message.markAsRestoredFromDLQ(c.clock.Now())
	if err != nil {
		return &RedriveMessageOutput[T]{}, err
	}
	builder := expression.NewBuilder().
		WithUpdate(expression.Add(
			expression.Name("version"),
			expression.Value(1),
		).Set(
			expression.Name("queue_type"),
			expression.Value(message.QueueType),
		).Set(
			expression.Name("updated_at"),
			expression.Value(message.UpdatedAt),
		).Set(
			expression.Name("sent_at"),
			expression.Value(message.SentAt),
		).Set(
			expression.Name("invisible_until_at"),
			expression.Value(message.InvisibleUntilAt),
		)).
		WithCondition(expression.Name("version").
			Equal(expression.Value(message.Version)))
	expr, err := c.buildExpression(builder)
	if err != nil {
		return nil, BuildingExpressionError{Cause: err}
	}
	updated, err := c.updateDynamoDBItem(ctx, params.ID, &expr)
	if err != nil {
		return &RedriveMessageOutput[T]{}, err
	}
	return &RedriveMessageOutput[T]{
		RedroveMessage: updated,
	}, nil
}

// GetQueueStatsInput represents the input parameters for obtaining statistical information about a DynamoDB-based queue.
// This struct does not contain any fields as it's used to request general queue statistics without the need for specific parameters.
type GetQueueStatsInput struct{}

// GetQueueStatsOutput represents the output containing statistical information about a DynamoDB-based queue.
type GetQueueStatsOutput struct {
	// First100IDsInQueue is an array of the first 100 message IDs currently in the queue.
	First100IDsInQueue []string `json:"first_100_IDs_in_queue"`
	// First100IDsInQueueProcessing is an array of the first 100 message IDs that are currently being processed.
	First100IDsInQueueProcessing []string `json:"first_100_IDs_in_queue_processing"`
	// TotalMessagesInQueue is the total number of messages present in the queue.
	TotalMessagesInQueue int `json:"total_messages_in_queue"`
	// TotalMessagesInQueueProcessing is the total number of messages that are currently in the process of being handled.
	TotalMessagesInQueueProcessing int `json:"total_messages_in_queue_processing"`
	// TotalMessagesInQueueReady is the total number of messages in the queue that are ready to be processed and have not started processing yet.
	TotalMessagesInQueueReady int `json:"total_messages_in_queue_ready"`
}

// GetQueueStats get statistical information about a DynamoDB-based queue.
// It provides statistics about the messages in the queue and their processing status. This includes the IDs of the first 100 messages in the queue, the first 100 IDs of messages selected for processing, the total number of records in the queue, the number of records currently in processing, and the number of records awaiting processing.
// This function provides essential information for monitoring and analyzing the message queue system, aiding in understanding the status of the queue.
func (c *ClientImpl[T]) GetQueueStats(ctx context.Context, _ *GetQueueStatsInput) (*GetQueueStatsOutput, error) {
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

func (c *ClientImpl[T]) queryAndCalculateQueueStats(ctx context.Context, expr expression.Expression) (*GetQueueStatsOutput, error) {
	var (
		stats = &GetQueueStatsOutput{
			First100IDsInQueue:             make([]string, 0),
			First100IDsInQueueProcessing:   make([]string, 0),
			TotalMessagesInQueue:           0,
			TotalMessagesInQueueProcessing: 0,
			TotalMessagesInQueueReady:      0,
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
			Limit:                     aws.Int32(defaultQueryLimit),
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
	stats.TotalMessagesInQueueReady = stats.TotalMessagesInQueue - stats.TotalMessagesInQueueProcessing
	return stats, nil
}

func (c *ClientImpl[T]) processQueryItemsForQueueStats(items []map[string]types.AttributeValue, stats *GetQueueStatsOutput) error {
	for _, itemMap := range items {
		stats.TotalMessagesInQueue++
		item := Message[T]{}
		err := c.unmarshalMap(itemMap, &item)
		if err != nil {
			return UnmarshalingAttributeError{Cause: err}
		}

		c.updateQueueStatsFromItem(&item, stats)
	}
	return nil
}

func (c *ClientImpl[T]) updateQueueStatsFromItem(message *Message[T], stats *GetQueueStatsOutput) {
	if message.GetStatus(c.clock.Now()) == StatusProcessing {
		stats.TotalMessagesInQueueProcessing++
		if len(stats.First100IDsInQueueProcessing) < maxFirstMessagesInQueue {
			stats.First100IDsInQueueProcessing = append(stats.First100IDsInQueueProcessing, message.ID)
		}
	}
	if len(stats.First100IDsInQueue) < maxFirstMessagesInQueue {
		stats.First100IDsInQueue = append(stats.First100IDsInQueue, message.ID)
	}
}

// GetDLQStatsInput represents the input parameters for obtaining statistical information about a DynamoDB-based Dead Letter Queue (DLQ).
// This struct does not contain any fields as it's used to request general DLQ statistics without the need for specific parameters.
type GetDLQStatsInput struct{}

// GetDLQStatsOutput represents the output containing statistical information about the Dead Letter Queue (DLQ).
type GetDLQStatsOutput struct {
	// First100IDsInQueue is an array of the first 100 message IDs currently in the DLQ.
	First100IDsInQueue []string `json:"first_100_IDs_in_queue"`
	// TotalMessagesInDLQ is the total number of messages present in the DLQ.
	TotalMessagesInDLQ int `json:"total_messages_in_DLQ"`
}

// GetDLQStats get statistical information about a DynamoDB-based Dead Letter Queue (DLQ).
// It provides statistics on the messages within the DLQ. This includes the IDs of the first 100 messages in the queue and the total number of records in the DLQ.
// This functions offers vital information for monitoring and analyzing the message queue system, aiding in understanding the status of the DLQ.
func (c *ClientImpl[T]) GetDLQStats(ctx context.Context, _ *GetDLQStatsInput) (*GetDLQStatsOutput, error) {
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

func (c *ClientImpl[T]) queryAndCalculateDLQStats(ctx context.Context, expr expression.Expression) (*GetDLQStatsOutput, error) {
	var (
		stats = &GetDLQStatsOutput{
			First100IDsInQueue: make([]string, 0),
			TotalMessagesInDLQ: 0,
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
			Limit:                     aws.Int32(defaultQueryLimit),
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

func (c *ClientImpl[T]) processQueryItemsForDLQStats(items []map[string]types.AttributeValue, stats *GetDLQStatsOutput) error {
	for _, itemMap := range items {
		stats.TotalMessagesInDLQ++
		if len(stats.First100IDsInQueue) < maxFirstMessagesInQueue {
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

// GetMessageInput represents the input parameters for retrieving a specific message from a DynamoDB-based queue.
type GetMessageInput struct {
	// ID is the unique identifier of the message to be retrieved from the queue.
	ID string
}

// GetMessageOutput represents the result of the operation to retrieve a message.
// This struct uses the generic type T and contains information about the retrieved message.
type GetMessageOutput[T any] struct {
	// Message is a pointer to the Message type containing information about the retrieved message.
	// The type T determines the format of the message content.
	Message *Message[T]
}

// GetMessage get a specific message from a DynamoDB-based queue.
// It retrieves the message from DynamoDB based on the specified message ID. The retrieved message is then unmarshaled into the specified generic type T.
func (c *ClientImpl[T]) GetMessage(ctx context.Context, params *GetMessageInput) (*GetMessageOutput[T], error) {
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

// ListMessagesInput represents the input parameters for listing messages from a DynamoDB-based queue.
type ListMessagesInput struct {
	// Size is the number of messages to be listed from the queue. It determines the maximum size of the returned message list.
	Size int32
}

// ListMessagesOutput represents the result of the operation to list messages from the queue.
// This struct uses the generic type T and contains an array of messages.
type ListMessagesOutput[T any] struct {
	// Messages is an array of pointers to Message types, containing information about each listed message.
	// The type T determines the format of the message content for each message in the array.
	Messages []*Message[T]
}

// ListMessages get a list of messages from a DynamoDB-based queue.
// It scans and retrieves messages from DynamoDB based on the specified size parameter. If the size is not specified or is zero or less, a default maximum list size of 10 is used.
// The retrieved messages are unmarshaled into an array of the generic type T and are sorted based on the update time.
func (c *ClientImpl[T]) ListMessages(ctx context.Context, params *ListMessagesInput) (*ListMessagesOutput[T], error) {
	if params == nil {
		params = &ListMessagesInput{}
	}
	if params.Size <= 0 {
		params.Size = constant.DefaultMaxListMessages
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
		return messages[i].UpdatedAt < messages[j].UpdatedAt
	})
	return &ListMessagesOutput[T]{Messages: messages}, nil
}

// ReplaceMessageInput represents the input parameters for replacing a specific message in a DynamoDB-based queue.
// This struct uses the generic type T for the message content.
type ReplaceMessageInput[T any] struct {
	// Message is pointer to the Message type containing the new message data that will replace the existing message in the queue.
	// The type T determines the format of the new message content.
	Message *Message[T]
}

// ReplaceMessageOutput represents the result of the operation to replace a message in the queue.
// This struct is empty as the replace message operation does not return any specific information.
type ReplaceMessageOutput struct {
}

// ReplaceMessage replace a specific message within a DynamoDB-based queue.
// It searches for an existing message based on the specified message ID and deletes it if found. Then, a new message is added to the queue.
// If a message with the specified ID does not exist, the new message is directly added to the queue.
func (c *ClientImpl[T]) ReplaceMessage(ctx context.Context, params *ReplaceMessageInput[T]) (*ReplaceMessageOutput, error) {
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

func (c *ClientImpl[T]) put(ctx context.Context, message *Message[T]) error {
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

func (c *ClientImpl[T]) updateDynamoDBItem(ctx context.Context,
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

func secToDur(sec int) time.Duration {
	return time.Duration(sec) * time.Second
}

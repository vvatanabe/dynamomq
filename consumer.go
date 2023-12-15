package dynamomq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvatanabe/dynamomq/internal/constant"
)

const (
	defaultPollingInterval        = time.Second
	defaultMaximumReceives        = 0 // unlimited
	defaultRetryIntervalInSeconds = 1
	defaultQueueType              = QueueTypeStandard
	defaultConcurrency            = 3
)

// ErrConsumerClosed is an error that indicates the Consumer has been closed.
// This error is returned when operations are attempted on a Consumer that has already been shut down.
var ErrConsumerClosed = errors.New("DynamoMQ: Consumer closed")

// ConsumerOptions contains configuration options for a Consumer instance.
// It allows customization of polling intervals, concurrency levels, visibility timeouts, and more.
//
// Consumer functions for setting various ConsumerOptions.
type ConsumerOptions struct {
	// PollingInterval specifies the time interval at which the Consumer polls the DynamoDB queue for new messages.
	PollingInterval time.Duration
	// Concurrency sets the number of concurrent message processing workers.
	Concurrency int
	// MaximumReceives defines the maximum number of times a message can be delivered.
	MaximumReceives int
	// VisibilityTimeout sets the duration (in seconds) a message remains invisible in the queue after being received.
	VisibilityTimeout int
	// RetryInterval defines the time interval (in seconds) before a failed message is retried.
	RetryInterval int
	// QueueType determines the type of queue (STANDARD or DLQ) the Consumer will operate on.
	QueueType QueueType
	// ErrorLog is an optional logger for errors. If nil, the standard logger is used.
	ErrorLog *log.Logger
	// OnShutdown is a slice of functions called when the Consumer is shutting down.
	OnShutdown []func()
}

// WithPollingInterval sets the polling interval for the Consumer.
// This function configures the time interval at which the Consumer polls the DynamoDB queue for new messages.
func WithPollingInterval(pollingInterval time.Duration) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.PollingInterval = pollingInterval
	}
}

// WithConcurrency sets the number of concurrent workers for processing messages in the Consumer.
// This function determines how many messages can be processed at the same time.
func WithConcurrency(concurrency int) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.Concurrency = concurrency
	}
}

// WithMaximumReceives sets the maximum number of times a message can be delivered to the Consumer.
// This function configures the limit on how many times a message will be attempted for delivery
// before being considered a failure or moved to a Dead Letter Queue, if applicable.
func WithMaximumReceives(maximumReceives int) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.MaximumReceives = maximumReceives
	}
}

// WithVisibilityTimeout sets the visibility timeout for messages in the Consumer.
// This function configures the duration (in seconds) a message remains invisible in the queue after being received.
func WithVisibilityTimeout(sec int) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.VisibilityTimeout = sec
	}
}

// WithRetryInterval sets the retry interval for failed messages in the Consumer.
// This function specifies the time interval (in seconds) before a failed message is retried.
func WithRetryInterval(sec int) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.RetryInterval = sec
	}
}

// WithQueueType sets the type of queue (STANDARD or DLQ) for the Consumer.
// This function allows specification of the queue type the Consumer will operate on.
func WithQueueType(queueType QueueType) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.QueueType = queueType
	}
}

// WithErrorLog sets a custom logger for the Consumer.
// This function configures an optional logger for errors. If nil, the standard logger is used.
func WithErrorLog(errorLog *log.Logger) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.ErrorLog = errorLog
	}
}

// WithOnShutdown adds functions to be called during the Consumer's shutdown process.
// This function appends to the list of callbacks executed when the Consumer is shutting down.
func WithOnShutdown(onShutdown []func()) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.OnShutdown = onShutdown
	}
}

// NewConsumer creates a new Consumer instance with the specified client, message processor, and options.
// It configures the Consumer with default values which can be overridden by the provided option functions.
func NewConsumer[T any](client Client[T], processor MessageProcessor[T], opts ...func(o *ConsumerOptions)) *Consumer[T] {
	o := &ConsumerOptions{
		PollingInterval:   defaultPollingInterval,
		Concurrency:       defaultConcurrency,
		MaximumReceives:   defaultMaximumReceives,
		VisibilityTimeout: constant.DefaultVisibilityTimeoutInSeconds,
		RetryInterval:     defaultRetryIntervalInSeconds,
		QueueType:         defaultQueueType,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Consumer[T]{
		client:            client,
		messageProcessor:  processor,
		pollingInterval:   o.PollingInterval,
		concurrency:       o.Concurrency,
		maximumReceives:   o.MaximumReceives,
		visibilityTimeout: o.VisibilityTimeout,
		retryInterval:     o.RetryInterval,
		queueType:         o.QueueType,
		errorLog:          o.ErrorLog,
		onShutdown:        o.OnShutdown,
		inShutdown:        0,
		mu:                sync.Mutex{},
		activeMessages:    make(map[*Message[T]]struct{}),
		activeMessagesWG:  sync.WaitGroup{},
		doneChan:          make(chan struct{}),
	}
}

// MessageProcessor is an interface defining a method to process messages of a generic type T.
// It is used in the context of consuming messages from a DynamoDB-based queue.
type MessageProcessor[T any] interface {
	// Process handles the processing of a message.
	// It takes a pointer to a Message of type T and returns an error if the processing fails.
	Process(msg *Message[T]) error
}

// MessageProcessorFunc is a functional type that implements the MessageProcessor interface.
// It allows using a function as a MessageProcessor.
type MessageProcessorFunc[T any] func(msg *Message[T]) error

// Process calls the MessageProcessorFunc itself to process the message.
// It enables the function type to adhere to the MessageProcessor interface.
func (f MessageProcessorFunc[T]) Process(msg *Message[T]) error {
	return f(msg)
}

// Consumer is a struct responsible for consuming messages from a DynamoDB-based queue.
// It supports generic message types and includes settings such as concurrency, polling intervals, and more.
// Note: To create a new instance of Consumer, it is necessary to use the NewConsumer function.
type Consumer[T any] struct {
	client            Client[T]
	messageProcessor  MessageProcessor[T]
	concurrency       int
	pollingInterval   time.Duration
	maximumReceives   int
	visibilityTimeout int
	retryInterval     int
	queueType         QueueType
	errorLog          *log.Logger
	onShutdown        []func()

	inShutdown       int32
	mu               sync.Mutex
	activeMessages   map[*Message[T]]struct{}
	activeMessagesWG sync.WaitGroup
	doneChan         chan struct{}
}

// StartConsuming starts the message consumption process, polling the queue for messages and processing them.
// The method handles message retrieval, processing, error handling, retries, and moving messages to the DLQ if necessary.
func (c *Consumer[T]) StartConsuming() error {
	msgChan := make(chan *Message[T], c.concurrency)
	defer close(msgChan)

	for i := 0; i < c.concurrency; i++ {
		go func() {
			for msg := range msgChan {
				c.trackAndProcessMessage(context.Background(), msg)
			}
		}()
	}

	for {
		ctx := context.Background()
		r, err := c.client.ReceiveMessage(ctx, &ReceiveMessageInput{
			QueueType:         c.queueType,
			VisibilityTimeout: c.visibilityTimeout,
		})
		if err != nil {
			if c.shuttingDown() {
				return ErrConsumerClosed
			}
			if !isTemporary(err) {
				return fmt.Errorf("DynamoMQ: Failed to receive a message: %w", err)
			}
			time.Sleep(c.pollingInterval)
			continue
		}
		msgChan <- r.ReceivedMessage
	}
}

func (c *Consumer[T]) trackAndProcessMessage(ctx context.Context, msg *Message[T]) {
	c.trackMessage(msg, true)
	c.processMessage(ctx, msg)
	c.trackMessage(msg, false)
}

func (c *Consumer[T]) processMessage(ctx context.Context, msg *Message[T]) {
	if err := c.messageProcessor.Process(msg); err != nil {
		c.handleError(ctx, msg)
		return
	}
	c.deleteMessage(ctx, msg)
}

func (c *Consumer[T]) handleError(ctx context.Context, msg *Message[T]) {
	if c.shouldRetry(msg) {
		c.retryMessage(ctx, msg)
	} else {
		c.handleFailure(ctx, msg)
	}
}

func (c *Consumer[T]) shouldRetry(msg *Message[T]) bool {
	if c.maximumReceives == 0 {
		return true
	}
	if msg.ReceiveCount < c.maximumReceives {
		return true
	}
	return false
}

func (c *Consumer[T]) retryMessage(ctx context.Context, msg *Message[T]) {
	in := &ChangeMessageVisibilityInput{
		ID:                msg.ID,
		VisibilityTimeout: c.retryInterval,
	}
	if _, err := c.client.ChangeMessageVisibility(ctx, in); err != nil {
		c.logf("DynamoMQ: Failed to update a message as visible. %s", err)
	}
}

func (c *Consumer[T]) handleFailure(ctx context.Context, msg *Message[T]) {
	switch c.queueType {
	case QueueTypeStandard:
		c.moveToDLQ(ctx, msg)
	case QueueTypeDLQ:
		c.deleteMessage(ctx, msg)
	}
}

func (c *Consumer[T]) moveToDLQ(ctx context.Context, msg *Message[T]) {
	if _, err := c.client.MoveMessageToDLQ(ctx, &MoveMessageToDLQInput{ID: msg.ID}); err != nil {
		c.logf("DynamoMQ: Failed to move a message to DLQ. %s", err)
	}
}

func (c *Consumer[T]) deleteMessage(ctx context.Context, msg *Message[T]) {
	if _, err := c.client.DeleteMessage(ctx, &DeleteMessageInput{ID: msg.ID}); err != nil {
		c.logf("DynamoMQ: Failed to delete a message. %s", err)
	}
}

func (c *Consumer[T]) trackMessage(msg *Message[T], add bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if add {
		c.activeMessages[msg] = struct{}{}
		c.activeMessagesWG.Add(1)
	} else {
		delete(c.activeMessages, msg)
		c.activeMessagesWG.Done()
	}
}

func (c *Consumer[T]) shuttingDown() bool {
	return atomic.LoadInt32(&c.inShutdown) != 0
}

// Shutdown gracefully shuts down the Consumer, stopping the message consumption and executing any registered shutdown callbacks.
func (c *Consumer[T]) Shutdown(ctx context.Context) error {
	atomic.StoreInt32(&c.inShutdown, 1)

	c.mu.Lock()
	c.closeDoneChanLocked()
	for _, f := range c.onShutdown {
		go f()
	}
	c.mu.Unlock()

	finished := make(chan struct{}, 1)
	go func() {
		c.activeMessagesWG.Wait()
		finished <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-finished:
		return nil
	}
}

func (c *Consumer[T]) closeDoneChanLocked() {
	select {
	case <-c.doneChan:
		// It's already closed. Don't close it again.
	default:
		// We can safely close it here.
		// We are the only closers and are protected by srv.mu.
		close(c.doneChan)
	}
}

func (c *Consumer[T]) logf(format string, args ...any) {
	if c.errorLog != nil {
		c.errorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func isTemporary(err error) bool {
	var (
		conditionalCheckFailedError *ConditionalCheckFailedError
		dynamoDBAPIError            *DynamoDBAPIError
		emptyQueueError             *EmptyQueueError
		idNotProvidedError          *IDNotProvidedError
		idNotFoundError             *IDNotFoundError
	)
	switch {
	case errors.As(err, &conditionalCheckFailedError),
		errors.As(err, &dynamoDBAPIError),
		errors.As(err, &emptyQueueError),
		errors.As(err, &idNotProvidedError),
		errors.As(err, &idNotFoundError):
		return true
	default:
		return false
	}
}

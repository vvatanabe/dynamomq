package dynamomq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultPollingInterval = time.Second
	defaultMaximumReceives = 0 // unlimited
	defaultQueueType       = QueueTypeStandard
)

func WithPollingInterval(pollingInterval time.Duration) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.PollingInterval = pollingInterval
	}
}

func WithMaximumReceives(maximumReceives int) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.MaximumReceives = maximumReceives
	}
}

func WithQueueType(queueType QueueType) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.QueueType = queueType
	}
}

func WithErrorLog(errorLog *log.Logger) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.ErrorLog = errorLog
	}
}

func WithOnShutdown(onShutdown []func()) func(o *ConsumerOptions) {
	return func(o *ConsumerOptions) {
		o.OnShutdown = onShutdown
	}
}

type ConsumerOptions struct {
	PollingInterval time.Duration
	MaximumReceives int
	QueueType       QueueType
	// errorLog specifies an optional logger for errors accepting
	// connections, unexpected behavior from handlers, and
	// underlying FileSystem errors.
	// If nil, logging is done via the log package's standard logger.
	ErrorLog   *log.Logger
	OnShutdown []func()
}

func NewConsumer[T any](client Client[T], processor MessageProcessor[T], opts ...func(o *ConsumerOptions)) *Consumer[T] {
	o := &ConsumerOptions{
		PollingInterval: defaultPollingInterval,
		MaximumReceives: defaultMaximumReceives,
		QueueType:       defaultQueueType,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Consumer[T]{
		client:           client,
		messageProcessor: processor,
		pollingInterval:  o.PollingInterval,
		maximumReceives:  o.MaximumReceives,
		queueType:        o.QueueType,
		errorLog:         o.ErrorLog,
		onShutdown:       o.OnShutdown,
		inShutdown:       0,
		mu:               sync.Mutex{},
		activeMessages:   make(map[*Message[T]]struct{}),
		activeMessagesWG: sync.WaitGroup{},
		doneChan:         make(chan struct{}),
	}
}

type MessageProcessor[T any] interface {
	Process(msg *Message[T]) error
}

type MessageProcessorFunc[T any] func(msg *Message[T]) error

func (f MessageProcessorFunc[T]) Process(msg *Message[T]) error {
	return f(msg)
}

type Consumer[T any] struct {
	client           Client[T]
	messageProcessor MessageProcessor[T]

	pollingInterval time.Duration
	maximumReceives int
	queueType       QueueType
	errorLog        *log.Logger
	onShutdown      []func()

	inShutdown       int32
	mu               sync.Mutex
	activeMessages   map[*Message[T]]struct{}
	activeMessagesWG sync.WaitGroup
	doneChan         chan struct{}
}

var ErrConsumerClosed = errors.New("DynamoMQ: Consumer closed")

func (c *Consumer[T]) StartConsuming() error {
	for {
		ctx := context.Background()
		r, err := c.client.ReceiveMessage(ctx, &ReceiveMessageInput{
			QueueType: c.queueType,
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
		go c.trackAndProcessMessage(ctx, r.PeekedMessageObject)
		time.Sleep(c.pollingInterval)
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
	if _, err := c.client.ChangeMessageVisibility(ctx, &ChangeMessageVisibilityInput{ID: msg.ID}); err != nil {
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
		if !c.shuttingDown() {
			c.activeMessages[msg] = struct{}{}
			c.activeMessagesWG.Add(1)
		}
	} else {
		delete(c.activeMessages, msg)
		c.activeMessagesWG.Done()
	}
}

func (c *Consumer[T]) shuttingDown() bool {
	return atomic.LoadInt32(&c.inShutdown) != 0
}

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
		// We are the only closers and are protected by srv.mu."
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

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
	defaultPollingInterval = time.Second * 10
	defaultMaximumReceives = 0 // unlimited
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
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Consumer[T]{
		client:           client,
		messageProcessor: processor,
		pollingInterval:  o.PollingInterval,
		maximumReceives:  o.MaximumReceives,
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

type Consumer[T any] struct {
	client           Client[T]
	messageProcessor MessageProcessor[T]

	pollingInterval time.Duration
	maximumReceives int
	errorLog        *log.Logger
	onShutdown      []func()

	inShutdown       int32
	mu               sync.Mutex
	activeMessages   map[*Message[T]]struct{}
	activeMessagesWG sync.WaitGroup
	doneChan         chan struct{}
}

var ErrConsumerClosed = errors.New("DynamoMQ: Consumer closed")

func (c *Consumer[T]) Listen() error {
	for {
		ctx := context.Background()
		r, err := c.client.Peek(ctx)
		if err != nil {
			if c.shuttingDown() {
				return ErrConsumerClosed
			}
			if !isTemporary(err) {
				return fmt.Errorf("DynamoMQ: Failed to peek a message. %s", err)
			}
			time.Sleep(c.pollingInterval)
			continue
		}
		go c.listen(ctx, r.PeekedMessageObject)
		time.Sleep(c.pollingInterval)
	}
}

func (c *Consumer[T]) listen(ctx context.Context, msg *Message[T]) {
	c.trackMessage(msg, true)
	c.processMessage(ctx, msg)
	c.trackMessage(msg, false)
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

func (c *Consumer[T]) processMessage(ctx context.Context, msg *Message[T]) {
	err := c.messageProcessor.Process(msg)
	if err != nil {
		if c.shouldRetry(msg) {
			_, err := c.client.Retry(ctx, msg.ID)
			if err != nil {
				c.logf("DynamoMQ: Failed to retry a message. %s", err)
				return
			}
		} else {
			_, err := c.client.SendToDLQ(ctx, msg.ID)
			if err != nil {
				c.logf("DynamoMQ: Failed to send a message to DLQ. %s", err)
				return
			}
		}
		return
	}
	err = c.client.Delete(ctx, msg.ID)
	if err != nil {
		c.logf("DynamoMQ: Failed to delete a message. %s", err)
		return
	}
}

func (c *Consumer[T]) trackMessage(msg *Message[T], add bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeMessages == nil {
		c.activeMessages = make(map[*Message[T]]struct{})
	}
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

func (c *Consumer[T]) getDoneChan() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getDoneChanLocked()
}

func (c *Consumer[T]) getDoneChanLocked() chan struct{} {
	if c.doneChan == nil {
		c.doneChan = make(chan struct{})
	}
	return c.doneChan
}

func (c *Consumer[T]) closeDoneChanLocked() {
	ch := c.getDoneChanLocked()
	select {
	case <-ch:
		// It's already closed. Don't close it again.
	default:
		// We can safely close it here.
		// We are the only closers and are protected by srv.mu."
		close(ch)
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
	switch err.(type) {
	case ConditionalCheckFailedError, DynamoDBAPIError, EmptyQueueError, IDNotProvidedError, IDNotFoundError:
		return true
	default:
		return false
	}
}

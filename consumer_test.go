package dynamomq_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/mock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func TestConsumerStartConsumingShouldReturnErrConsumerClosed(t *testing.T) {
	t.Parallel()
	queue, dlq, store := prepareQueueAndStore(1, 1)
	client := NewClientForConsumerTest(queue, dlq, store, ClientForConsumerTestConfig{
		SimulateReceiveMessageError: true,
	})
	processor := &CountProcessor[test.MessageData]{
		SimulateProcessError: false,
	}
	consumer := dynamomq.NewConsumer[test.MessageData](client, processor)
	_ = consumer.Shutdown(context.Background())
	if err := consumer.StartConsuming(); !errors.Is(err, dynamomq.ErrConsumerClosed) {
		t.Errorf("StartConsuming() error = %v, want = %v", err, dynamomq.ErrConsumerClosed)
		return
	}
}

func TestConsumerStartConsumingShouldReturnNoTemporaryError(t *testing.T) {
	t.Parallel()
	queue, dlq, store := prepareQueueAndStore(1, 1)
	client := NewClientForConsumerTest(queue, dlq, store, ClientForConsumerTestConfig{
		SimulateReceiveMessageNoTemporaryError: true,
	})
	processor := &CountProcessor[test.MessageData]{
		SimulateProcessError: false,
	}
	consumer := dynamomq.NewConsumer[test.MessageData](client, processor)
	if err := consumer.StartConsuming(); !errors.Is(err, test.ErrTest) {
		t.Errorf("StartConsuming() error = %v, want = %v", err, test.ErrTest)
		return
	}
}

func TestConsumerStartConsuming(t *testing.T) {
	t.Parallel()
	type testCase struct {
		Name                        string
		ClientForConsumerTestConfig ClientForConsumerTestConfig
		WantMessageProcessError     bool
		MessageSize                 int
		MessageReceiveCount         int
		MaximumReceives             int
		QueueType                   dynamomq.QueueType
		ExpectedCountSize           int
		ExpectedStoreSize           int
		ExpectedDLQSize             int
	}
	tests := []testCase{
		{
			Name:              "should succeed message process",
			MessageSize:       10,
			QueueType:         dynamomq.QueueTypeStandard,
			ExpectedCountSize: 10,
		},
		{
			Name:                    "should retried message process when maximum receives is 0",
			WantMessageProcessError: true,
			MaximumReceives:         0,
			MessageSize:             10,
			QueueType:               dynamomq.QueueTypeStandard,
			ExpectedStoreSize:       10,
		},
		{
			Name:                    "should retried message process when receive count < maximum receives",
			WantMessageProcessError: true,
			MaximumReceives:         2,
			MessageSize:             10,
			QueueType:               dynamomq.QueueTypeStandard,
			ExpectedStoreSize:       10,
		},
		{
			Name:                    "should moved message to DLQ",
			WantMessageProcessError: true,
			MessageReceiveCount:     1,
			MaximumReceives:         1,
			QueueType:               dynamomq.QueueTypeStandard,
			MessageSize:             10,
			ExpectedStoreSize:       10,
			ExpectedDLQSize:         10,
		},
		{
			Name:                    "should delete message when when receive count < maximum receives",
			WantMessageProcessError: true,
			MessageReceiveCount:     1,
			MaximumReceives:         1,
			QueueType:               dynamomq.QueueTypeDLQ,
			MessageSize:             10,
		},
		{
			Name: "should not return error when receive message is temporary",
			ClientForConsumerTestConfig: ClientForConsumerTestConfig{
				SimulateReceiveMessageError: true,
			},
			QueueType:         dynamomq.QueueTypeStandard,
			MessageSize:       10,
			ExpectedStoreSize: 10,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			queue, dlq, store := prepareQueueAndStore(tt.MessageSize, tt.MessageReceiveCount)
			client := NewClientForConsumerTest(queue, dlq, store, tt.ClientForConsumerTestConfig)
			processor := &CountProcessor[test.MessageData]{
				SimulateProcessError: tt.WantMessageProcessError,
			}
			consumer := dynamomq.NewConsumer[test.MessageData](client, processor,
				dynamomq.WithPollingInterval(0),
				dynamomq.WithMaximumReceives(tt.MaximumReceives),
				dynamomq.WithQueueType(tt.QueueType),
				dynamomq.WithErrorLog(log.New(os.Stderr, "", 0)),
				dynamomq.WithOnShutdown([]func(){}))
			go func() {
				if err := consumer.StartConsuming(); err != nil {
					t.Errorf("StartConsuming() error = %v", err)
					return
				}
			}()

			time.Sleep(time.Second)

			if processor.Count.Load() != int32(tt.ExpectedCountSize) {
				t.Errorf("StartConsuming() count = %v, want %v", processor.Count.Load(), tt.ExpectedCountSize)
			}
			var storeSize int
			store.Range(func(key, value any) bool {
				storeSize++
				return true
			})
			if storeSize != tt.ExpectedStoreSize {
				t.Errorf("StartConsuming() storeSize = %v, want %v", storeSize, tt.ExpectedStoreSize)
			}
			if len(dlq) != tt.ExpectedDLQSize {
				t.Errorf("StartConsuming() dlqSize = %v, want %v", len(dlq), tt.ExpectedDLQSize)
			}
		})
	}
}

func TestConsumerShutdown(t *testing.T) {
	t.Parallel()
	type testCase struct {
		Name              string
		ProcessorSleep    time.Duration
		MessageSize       int
		ExpectedCountSize int
		WantShutdownError bool
	}
	tests := []testCase{
		{
			Name:              "should succeed a graceful shutdown",
			MessageSize:       100,
			ExpectedCountSize: 100,
		},
		{
			Name:              "should timeout during a graceful shutdown",
			ProcessorSleep:    3 * time.Second,
			MessageSize:       100,
			WantShutdownError: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			queue, dlq, store := prepareQueueAndStore(tt.MessageSize, 0)
			client := NewClientForConsumerTest(queue, dlq, store, ClientForConsumerTestConfig{})
			processor := &CountProcessor[test.MessageData]{
				SimulateProcessError: false,
				Sleep:                tt.ProcessorSleep,
			}
			consumer := dynamomq.NewConsumer[test.MessageData](client, processor,
				dynamomq.WithPollingInterval(0),
				dynamomq.WithMaximumReceives(1),
				dynamomq.WithQueueType(dynamomq.QueueTypeStandard),
				dynamomq.WithErrorLog(log.New(os.Stderr, "", 0)),
				dynamomq.WithOnShutdown([]func(){
					func() {},
				}))
			go func() {
				if err := consumer.StartConsuming(); err != nil {
					t.Errorf("StartConsuming() error = %v", err)
					return
				}
			}()

			time.Sleep(time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			err := consumer.Shutdown(ctx)
			if tt.WantShutdownError {
				if err == nil {
					t.Error("Shutdown() error is nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Shutdown() error = %v", err)
				return
			}

			if processor.Count.Load() != int32(tt.ExpectedCountSize) {
				t.Errorf("StartConsuming() count = %v, want %v", processor.Count.Load(), tt.ExpectedCountSize)
			}
		})
	}
}

func prepareQueueAndStore(size, receiveCount int) (queue, dlq chan *dynamomq.Message[test.MessageData], store *sync.Map) {
	var storeMap sync.Map
	queue = make(chan *dynamomq.Message[test.MessageData], size)
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("A-%d", i)
		msg := dynamomq.NewMessage(id, test.NewMessageData(id), test.DefaultTestDate)
		msg.ReceiveCount = receiveCount
		storeMap.Store(id, msg)
		queue <- msg
	}
	return queue, make(chan *dynamomq.Message[test.MessageData], size), &storeMap
}

type CountProcessor[T any] struct {
	Count                atomic.Int32
	SimulateProcessError bool
	Sleep                time.Duration
}

func (p *CountProcessor[T]) Process(_ *dynamomq.Message[T]) error {
	if p.SimulateProcessError {
		return test.ErrTest
	}
	time.Sleep(p.Sleep)
	p.Count.Add(1)
	return nil
}

type ClientForConsumerTestConfig struct {
	SimulateReceiveMessageError            bool
	SimulateReceiveMessageNoTemporaryError bool
	SimulateDeleteMessageError             bool
	SimulateMessageAsVisibleError          bool
	SimulateMoveMessageToDLQError          bool
}

func NewClientForConsumerTest(queue, dlq chan *dynamomq.Message[test.MessageData], store *sync.Map,
	cfg ClientForConsumerTestConfig) dynamomq.Client[test.MessageData] {
	return &mock.Client[test.MessageData]{
		ReceiveMessageFunc: func(ctx context.Context, params *dynamomq.ReceiveMessageInput) (*dynamomq.ReceiveMessageOutput[test.MessageData], error) {
			if cfg.SimulateReceiveMessageNoTemporaryError {
				return nil, test.ErrTest
			}
			if cfg.SimulateReceiveMessageError {
				return nil, &dynamomq.DynamoDBAPIError{Cause: test.ErrTest}
			}
			message := <-queue
			return &dynamomq.ReceiveMessageOutput[test.MessageData]{PeekedMessageObject: message}, nil
		},
		DeleteMessageFunc: func(ctx context.Context, params *dynamomq.DeleteMessageInput) (*dynamomq.DeleteMessageOutput, error) {
			if cfg.SimulateDeleteMessageError {
				return nil, test.ErrTest
			}
			store.Delete(params.ID)
			return &dynamomq.DeleteMessageOutput{}, nil
		},
		ChangeMessageVisibilityFunc: func(ctx context.Context, params *dynamomq.ChangeMessageVisibilityInput) (*dynamomq.ChangeMessageVisibilityOutput[test.MessageData], error) {
			if cfg.SimulateMessageAsVisibleError {
				return nil, test.ErrTest
			}
			return &dynamomq.ChangeMessageVisibilityOutput[test.MessageData]{}, nil
		},
		MoveMessageToDLQFunc: func(ctx context.Context, params *dynamomq.MoveMessageToDLQInput) (*dynamomq.MoveMessageToDLQOutput, error) {
			if cfg.SimulateMoveMessageToDLQError {
				return nil, test.ErrTest
			}
			v, _ := store.Load(params.ID)
			msg, _ := v.(*dynamomq.Message[test.MessageData])
			dlq <- msg
			return &dynamomq.MoveMessageToDLQOutput{
				ID:                   msg.ID,
				Status:               dynamomq.StatusReady,
				LastUpdatedTimestamp: msg.LastUpdatedTimestamp,
				Version:              2,
			}, nil
		},
	}
}

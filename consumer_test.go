package dynamomq

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvatanabe/dynamomq/internal/test"
)

func TestConsumerStartConsuming(t *testing.T) {
	t.Parallel()
	type testCase struct {
		Name                        string
		ClientForConsumerTestConfig ClientForConsumerTestConfig
		WantMessageProcessError     bool
		MessageSize                 int
		MessageReceiveCount         int
		MaximumReceives             int
		QueueType                   QueueType
		ExpectedCountSize           int
		ExpectedStoreSize           int
		ExpectedDLQSize             int
	}
	tests := []testCase{
		{
			Name:              "should succeed message process",
			MessageSize:       10,
			QueueType:         QueueTypeStandard,
			ExpectedCountSize: 10,
		},
		{
			Name:                    "should retried message process when maximum receives is 0",
			WantMessageProcessError: true,
			MaximumReceives:         0,
			MessageSize:             10,
			QueueType:               QueueTypeStandard,
			ExpectedStoreSize:       10,
		},
		{
			Name:                    "should retried message process when receive count < maximum receives",
			WantMessageProcessError: true,
			MaximumReceives:         2,
			MessageSize:             10,
			QueueType:               QueueTypeStandard,
			ExpectedStoreSize:       10,
		},
		{
			Name:                    "should moved message to DLQ",
			WantMessageProcessError: true,
			MessageReceiveCount:     1,
			MaximumReceives:         1,
			QueueType:               QueueTypeStandard,
			MessageSize:             10,
			ExpectedStoreSize:       10,
			ExpectedDLQSize:         10,
		},
		{
			Name:                    "should delete message when when receive count < maximum receives",
			WantMessageProcessError: true,
			MessageReceiveCount:     1,
			MaximumReceives:         1,
			QueueType:               QueueTypeDLQ,
			MessageSize:             10,
		},
		{
			Name: "should not return error when receive message is temporary",
			ClientForConsumerTestConfig: ClientForConsumerTestConfig{
				SimulateReceiveMessageError: true,
			},
			QueueType:         QueueTypeStandard,
			MessageSize:       10,
			ExpectedStoreSize: 10,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			queue, dlq, store := PrepareQueueAndStore(tt.MessageSize, tt.MessageReceiveCount)
			client := NewClientForConsumerTest(queue, dlq, store, tt.ClientForConsumerTestConfig)
			processor := &CountProcessor[test.MessageData]{
				SimulateProcessError: tt.WantMessageProcessError,
			}
			consumer := NewConsumer[test.MessageData](client, processor,
				WithPollingInterval(0),
				WithMaximumReceives(tt.MaximumReceives),
				WithQueueType(tt.QueueType),
				WithErrorLog(log.New(os.Stderr, "", 0)),
				WithOnShutdown([]func(){}))
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

type CountProcessor[T any] struct {
	Count                atomic.Int32
	SimulateProcessError bool
	Sleep                time.Duration
}

func (p *CountProcessor[T]) Process(_ *Message[T]) error {
	if p.SimulateProcessError {
		return ErrorTest
	}
	time.Sleep(p.Sleep)
	p.Count.Add(1)
	return nil
}

type ClientForConsumerTestConfig struct {
	SimulateReceiveMessageError   bool
	SimulateDeleteMessageError    bool
	SimulateMessageAsVisibleError bool
	SimulateMoveMessageToDLQError bool
}

func NewClientForConsumerTest(queue, dlq chan *Message[test.MessageData], store *sync.Map,
	cfg ClientForConsumerTestConfig) Client[test.MessageData] {
	return &MockClient[test.MessageData]{
		ReceiveMessageFunc: func(ctx context.Context, params *ReceiveMessageInput) (*ReceiveMessageOutput[test.MessageData], error) {
			if cfg.SimulateReceiveMessageError {
				return nil, &DynamoDBAPIError{Cause: ErrorTest}
			}
			message := <-queue
			return &ReceiveMessageOutput[test.MessageData]{PeekedMessageObject: message}, nil
		},
		DeleteMessageFunc: func(ctx context.Context, params *DeleteMessageInput) (*DeleteMessageOutput, error) {
			if cfg.SimulateDeleteMessageError {
				return nil, ErrorTest
			}
			store.Delete(params.ID)
			return &DeleteMessageOutput{}, nil
		},
		UpdateMessageAsVisibleFunc: func(ctx context.Context, params *UpdateMessageAsVisibleInput) (*UpdateMessageAsVisibleOutput[test.MessageData], error) {
			if cfg.SimulateMessageAsVisibleError {
				return nil, ErrorTest
			}
			return &UpdateMessageAsVisibleOutput[test.MessageData]{}, nil
		},
		MoveMessageToDLQFunc: func(ctx context.Context, params *MoveMessageToDLQInput) (*MoveMessageToDLQOutput, error) {
			if cfg.SimulateMoveMessageToDLQError {
				return nil, ErrorTest
			}
			v, _ := store.Load(params.ID)
			msg := v.(*Message[test.MessageData])
			dlq <- msg
			return &MoveMessageToDLQOutput{
				ID:                   msg.ID,
				Status:               msg.Status,
				LastUpdatedTimestamp: msg.LastUpdatedTimestamp,
				Version:              2,
			}, nil
		},
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
			ProcessorSleep:    2 * time.Second,
			MessageSize:       100,
			WantShutdownError: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			queue, dlq, store := PrepareQueueAndStore(tt.MessageSize, 0)
			client := NewClientForConsumerTest(queue, dlq, store, ClientForConsumerTestConfig{})
			processor := &CountProcessor[test.MessageData]{
				SimulateProcessError: false,
				Sleep:                tt.ProcessorSleep,
			}
			consumer := NewConsumer[test.MessageData](client, processor,
				WithPollingInterval(0),
				WithMaximumReceives(1),
				WithQueueType(QueueTypeStandard),
				WithErrorLog(log.New(os.Stderr, "", 0)),
				WithOnShutdown([]func(){
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

func PrepareQueueAndStore(size, receiveCount int) (queue, dlq chan *Message[test.MessageData], store *sync.Map) {
	var storeMap sync.Map
	queue = make(chan *Message[test.MessageData], size)
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("A-%d", i)
		msg := NewMessage(id, test.NewMessageData(id), DefaultTestDate)
		msg.ReceiveCount = receiveCount
		storeMap.Store(id, msg)
		queue <- msg
	}
	return queue, make(chan *Message[test.MessageData], size), &storeMap
}

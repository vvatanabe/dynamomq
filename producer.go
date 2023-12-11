package dynamomq

import (
	"context"

	"github.com/google/uuid"
)

// ProducerOptions holds configuration options for a Producer.
type ProducerOptions struct {
	// IDGenerator is function that generates a unique identifier for each message produced by the Producer.
	// The default ID generator is uuid.NewString.
	IDGenerator func() string
}

// WithIDGenerator is an option function to set a custom ID generator for the Producer.
// Use this function to provide a custom function that generates unique identifiers for messages.
// The default ID generator is uuid.NewString.
func WithIDGenerator(idGenerator func() string) func(o *ProducerOptions) {
	return func(o *ProducerOptions) {
		o.IDGenerator = idGenerator
	}
}

// NewProducer creates a new instance of a Producer, which is used to produce messages to a DynamoDB-based queue.
// The Producer can be configured with various options, such as a custom ID generator.
func NewProducer[T any](client Client[T], opts ...func(o *ProducerOptions)) *Producer[T] {
	o := &ProducerOptions{
		IDGenerator: uuid.NewString,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Producer[T]{
		client:      client,
		idGenerator: o.IDGenerator,
	}
}

// Producer is a generic struct responsible for producing messages of any type T to a DynamoDB-based queue.
type Producer[T any] struct {
	client      Client[T]
	idGenerator func() string
}

// ProduceInput represents the input parameters for producing a message.
type ProduceInput[T any] struct {
	// Data is the content of the message to be produced. The type T allows for flexibility in the data type of the message payload.
	Data T
	// DelaySeconds is the delay time (in seconds) before the message is sent to the queue.
	DelaySeconds int
}

// ProduceOutput represents the result of the produce operation.
type ProduceOutput[T any] struct {
	// Message is a pointer to the Message type containing information about the produced message.
	Message *Message[T]
}

// Produce sends a message to the queue using the provided input parameters.
// It generates a unique ID for the message using the Producer's ID generator and delegates to the Client's SendMessage method.
// An error is returned if the SendMessage operation fails.
func (c *Producer[T]) Produce(ctx context.Context, params *ProduceInput[T]) (*ProduceOutput[T], error) {
	if params == nil {
		params = &ProduceInput[T]{}
	}
	out, err := c.client.SendMessage(ctx, &SendMessageInput[T]{
		ID:           c.idGenerator(),
		Data:         params.Data,
		DelaySeconds: params.DelaySeconds,
	})
	if err != nil {
		return &ProduceOutput[T]{}, err
	}
	return &ProduceOutput[T]{
		Message: out.SentMessage,
	}, nil
}

package dynamomq

import (
	"context"

	uuid "github.com/satori/go.uuid"
)

type ProducerOptions struct {
	IDGenerator func() string
}

func WithIDGenerator(idGenerator func() string) func(o *ProducerOptions) {
	return func(o *ProducerOptions) {
		o.IDGenerator = idGenerator
	}
}

func NewProducer[T any](client Client[T], opts ...func(o *ProducerOptions)) *Producer[T] {
	o := &ProducerOptions{
		IDGenerator: uuid.NewV4().String,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Producer[T]{
		client:      client,
		idGenerator: o.IDGenerator,
	}
}

type Producer[T any] struct {
	client      Client[T]
	idGenerator func() string
}

type ProduceInput[T any] struct {
	Data T
}

type ProduceOutput[T any] struct {
	Message *Message[T]
}

func (c *Producer[T]) Produce(ctx context.Context, params *ProduceInput[T]) (*ProduceOutput[T], error) {
	if params == nil {
		params = &ProduceInput[T]{}
	}
	out, err := c.client.SendMessage(ctx, &SendMessageInput[T]{
		ID:   c.idGenerator(),
		Data: params.Data,
	})
	if err != nil {
		return &ProduceOutput[T]{}, err
	}
	return &ProduceOutput[T]{
		Message: out.Message,
	}, nil
}

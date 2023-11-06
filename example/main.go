package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/vvatanabe/dynamomq"
)

func main() {
	ctx := context.Background()

	// ------------------------------
	// Create DynamoMQ Client
	// ------------------------------
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic("failed to load aws config")
	}
	client, err := dynamomq.NewFromConfig[ExampleData](cfg)
	if err != nil {
		panic("AWS session could not be established!")
	}

	// ------------------------------
	// Create DynamoMQ Producer
	// ------------------------------
	producer := dynamomq.NewProducer[ExampleData](client)
	_, err = producer.Produce(ctx, &dynamomq.ProduceInput[ExampleData]{
		Data: ExampleData{
			Data1: "foo",
			Data2: "bar",
			Data3: "baz",
		},
	})
	if err != nil {
		panic("failed to produce message")
	}

	// ------------------------------
	// Create DynamoMQ Consumer
	// ------------------------------
	consumer := dynamomq.NewConsumer[ExampleData](client, &Counter[ExampleData]{})
	go func() {
		err = consumer.StartConsuming()
		if err != nil {
			fmt.Println(err)
		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	<-done

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := consumer.Shutdown(ctx); err != nil {
		fmt.Println("failed to consumer shutdown:", err)
	}
}

type ExampleData struct {
	Data1 string `dynamodbav:"data_1"`
	Data2 string `dynamodbav:"data_2"`
	Data3 string `dynamodbav:"data_3"`
}

type Counter[T any] struct {
	Value int
}

func (c *Counter[T]) Process(msg *dynamomq.Message[T]) error {
	c.Value++
	fmt.Printf("value: %d, message: %v\n", c.Value, msg)
}

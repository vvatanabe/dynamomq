<p align="center">
  <img width="460" height="300" src="https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-192C1.png">
</p>

Implementing message queueing with Amazon DynamoDB in Go.

## Current Status

This project is actively under development, but it is currently in version 0. Please be aware that the public API and exported methods may undergo changes.

## Features

- [x] **Redelivery**: Redeliver messages that have not completed successfully for a specified number of times.
- [x] **Concurrent Execution**: Process concurrently using multiple goroutines.
- [x] **Dead Letter Queue**: Move messages that exceed the maximum number of redeliveries to the dead letter queue.
- [x] **Graceful Shutdown**: Complete processing of messages before shutting down the consumer process.
- [x] **FIFO (First In, First Out)**: Retrieve messages from the message queue on a first-in, first-out basis.
- [x] **Consumer Process Scaling**: Scale out by running multiple consumer processes without duplicating message retrieval from the same message queue.
- [ ] **Deduplication**: Deduplication messages within the message queue.
- [ ] **Randomized Exponential Backoff**: Prevent overlapping redelivery timing.
- [ ] **Batch Message Processing**: Send and delete multiple messages in bulk to/from the message queue.
- [ ] **Message Compression**

## Installation

Requires Go version 1.21 or greater.

### DynamoMQ CLI

This package can be installed as CLI with the go install command:
```
$ go install github.com/vvatanabe/dynamomq/cmd/dynamomq@latest
```

### DynamoMQ Library

This package can be installed as library with the go get command:
```
$ go get -u github.com/vvatanabe/dynamomq@latest
```

## Usage

### DynamoMQ CLI

Coming Soon

### DynamoMQ Library

#### DynamoMQ Client

To begin using DynamoMQ, first import the necessary packages from the AWS SDK for Go v2 and the DynamoMQ library. These imports are required to interact with AWS services and to utilize the DynamoMQ functionalities.

```go
import (
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/vvatanabe/dynamomq"
)
```

The following code block initializes the DynamoMQ client. It loads the AWS configuration and creates a new DynamoMQ client with that configuration. Replace 'ExampleData' with your own data structure as needed.

```go
ctx := context.Background()
cfg, err := config.LoadDefaultConfig(ctx)
if err != nil {
  panic("failed to load aws config")
}
client, err := dynamomq.NewFromConfig[ExampleData](cfg)
if err != nil {
  panic("AWS session could not be established!")
}
```

Define the data structure that will be used with DynamoMQ. Here, 'ExampleData' is a struct that will be used to represent the data in the DynamoDB.

```go
type ExampleData struct {
	Data1 string `dynamodbav:"data_1"`
	Data2 string `dynamodbav:"data_2"`
	Data3 string `dynamodbav:"data_3"`
}
```

#### DynamoMQ Producer

The following snippet creates a DynamoMQ producer for the 'ExampleData' type. It then sends a message with predefined data to the queue. 

```go
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
```

#### DynamoMQ Consumer

To consume messages, instantiate a DynamoMQ consumer for 'ExampleData' and start it in a new goroutine. The consumer will process messages until an interrupt signal is received. The example includes graceful shutdown logic for the consumer.

```go
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
```

Here we define a 'Counter' type that implements the processing logic for consumed messages. Each time a message is processed, the counter is incremented, and the message details are printed.

```go
type Counter[T any] struct {
	Value int
}

func (c *Counter[T]) Process(msg *dynamomq.Message[T]) error {
	c.Value++
	fmt.Printf("value: %d, message: %v\n", c.Value, msg)
	return nil
}
```

## Software Design

### State Machine

![State Machine](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-4B362.png) 

### Table Definition

| Key   | Attributes               | Type   | Example Value                       |
|-------|--------------------------|--------|-------------------------------------|
| PK    | id                       | string | A-101                               |
|       | data                     | any    | any                                 |
|       | status                   | string | READY or PROCESSING                 |
|       | receive_count            | number | 1                                   |
| GSIPK | queue_type               | string | STANDARD or DLQ                     |
|       | version                  | number | 1                                   |
|       | creation_timestamp       | string | 2006-01-02T15:04:05.999999999Z07:00 |
|       | last_updated_timestamp   | string | 2006-01-02T15:04:05.999999999Z07:00 |
| GSISK | queue_add_timestamp      | string | 2006-01-02T15:04:05.999999999Z07:00 |
|       | queue_peek_timestamp     | string | 2006-01-02T15:04:05.999999999Z07:00 |

### Data Transition

![Data Transition](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-D143B.png)

## Authors

* **[vvatanabe](https://github.com/vvatanabe/)** - *Main contributor*
* Currently, there are no other contributors

## License

This project is licensed under the MIT License. For detailed licensing information, refer to the [LICENSE](LICENSE) file included in the repository.

# DynamoMQ: Implementing message queueing with Amazon DynamoDB in Go.

[![Build](https://github.com/vvatanabe/dynamomq/actions/workflows/build.yml/badge.svg)](https://github.com/vvatanabe/dynamomq/actions/workflows/build.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/vvatanabe/dynamomq.svg)](https://pkg.go.dev/github.com/vvatanabe/dynamomq) [![Go Report Card](https://goreportcard.com/badge/github.com/vvatanabe/dynamomq)](https://goreportcard.com/report/github.com/vvatanabe/dynamomq) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq)  [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=bugs)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=coverage)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq)

<p align="center">
  <img height="300" src="https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-192C1.png">
</p>

DynamoMQ is a message queuing library that leverages DynamoDB as storage, implemented in Go. It provides an SDK to support the implementation of consumers and producers in Go, along with a CLI that functions as a management tool.

## Table of Contents

- [Motivation](#motivation)
- [Comparison with Existing AWS Queuing Solutions](#comparison-with-existing-aws-queuing-solutions)
- [Installation DynamoMQ](#installation-dynamomq)
- [Setup DynamoMQ](#setup-dynamomq)
- [Authentication and access credentials](#authentication-and-access-credentials)
- [Usage for DynamoMQ CLI](#usage-for-dynamomq-cli)
- [Usage for DynamoMQ SDK](#usage-for-dynamomq-sdk)
- [About the Design of DynamoMQ](#about-the-design-of-dynamomq)
- [Conclusion](#conclusion)
- [Acknowledgments](#acknowledgments)
- [Authors](#authors)
- [License](#license)

## Motivation

Message Queuing systems are widely adopted for asynchronous message transmission between applications. These systems emphasize high throughput and reliability. In this section, we explore the benefits of integrating the features of DynamoDB with MQ systems.

### Overview of DynamoDB

Amazon DynamoDB is a high-performance NoSQL database service that supports a wide range of data models, from simple key-value stores to complex document-based stores. The main features of DynamoDB include:

- **Performance and Scalability**: DynamoDB responds in milliseconds and can automatically scale up or down its read and write capacity as needed.
- **High Availability and Durability**: Data is automatically replicated across multiple geographically distributed facilities, ensuring high levels of availability and durability.
- **Fully Managed and Serverless**: It eliminates the need for server management and configuration, reducing operational costs.

Reference: [What is Amazon DynamoDB?](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html)

### Advantages of Using DynamoDB as Message Queue

Applying the features of DynamoDB to MQ systems offers the following benefits:

1. **Scalability**: The ability to efficiently handle a large volume of messages is extremely important for MQ systems. DynamoDB's on-demand mode allows for easy adjustment of resources in response to fluctuating demands.
2. **Cost Efficiency**: Using the on-demand mode reduces the risk of over-provisioning or under-provisioning resources, thus lowering costs. Additionally, choosing the provisioning mode when usage can be predicted can further reduce costs.
3. **Reliability and Availability**: Message loss is critical for MQ systems. DynamoDB's high durability and availability ensure the safety of messages.
4. **Flexibility**: Editing items stored in DynamoDB as messages allows for dynamic changes in the message order within the queue, updates to attributes, and cancellation of message processing, offering more flexible operations.

## Comparison with Existing AWS Queuing Solutions

AWS offers several solutions for MQ, including Amazon SQS, Amazon MQ, and Amazon MSK based on Kafka. By comparing these services with DynamoMQ, we can examine the unique advantages of DynamoMQ.

### Comparison with Amazon SQS

Amazon SQS is a fully managed, serverless MQ service widely used for asynchronous message processing in large-scale distributed applications.

<ins>**It excels in scalability, cost efficiency, reliability, and availability, and I generally recommend using Amazon SQS.**</ins>

However, Amazon SQS lacks flexibility in certain aspects. Messages sent to the queue cannot be reordered or have their attributes updated from outside. Also, to reference messages within the queue, they must first be received. These constraints can make it challenging to handle specific problems and hinder investigation and recovery efforts.

### Comparison with Amazon MQ

Amazon MQ is a message broker service based on Apache ActiveMQ and RabbitMQ, primarily aimed at facilitating the migration of existing applications. It supports a variety of protocols such as JMS, AMQP, STOMP, MQTT, OpenWire, and WebSocket.

Amazon MQ offers numerous features provided by its base technologies, ActiveMQ and RabbitMQ. However, it is not serverless, which means setup and management require more effort.

### Comparison with Amazon MSK

Amazon MSK provides a fully managed Apache Kafka service, suitable for processing large volumes of streaming data.

While MSK specializes in real-time data streaming, DynamoMQ focuses on general message queuing needs. MSK allows for advanced configurations, but this can lead to increased complexity and costs.

### Advantages of DynamoMQ

DynamoMQ leverages the scalability, durability, and serverless nature of DynamoDB. Compared to some AWS queuing solutions, it offers flexibility in managing the order and attributes of messages, low-cost operations, and easy setup and management.

## Supported Features

DynamoMQ is equipped with key features that should be provided by a Message Queuing system, supporting the realization of a flexible and reliable system.

### Redelivery

If a message is not processed successfully, it will be redelivered. This approach addresses temporary errors and delays in processing, increasing the chances that messages are processed correctly.

### Concurrent Execution

Multiple goroutines are utilized to process messages concurrently. This feature enables high throughput and efficient use of resources.

### Dead Letter Queue

Messages that exceed the maximum number of redeliveries are moved to the Dead Letter Queue (DLQ). This separates messages with persistent errors, allowing for later analysis or manual processing.

### Graceful Shutdown

Message processing is completed before the shutdown of the consumer process. This prevents the loss of messages that are being processed at the time of shutdown.

### FIFO (First In, First Out)

Messages are retrieved from the queue on a First In, First Out basis. This guarantees that messages are processed in the order they were sent, making it suitable for applications where order is important.

### Consumer Process Scaling

Multiple consumer processes can be launched, enabling scaling out. This prevents duplication in retrieving messages from the same message queue. Consequently, processing capacity can be dynamically adjusted according to the load.

### Visibility Timeout

A visibility timeout is set for a specific period during which the message is invisible to all consumers. This prevents a message from being received by other consumers while it is being processed.

### Delay Queuing

Delay queuing allows the delivery of new messages to consumers to be delayed for a set number of seconds. This feature accommodates the needs of applications that require a delayed message delivery.

## Installation DynamoMQ

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

## Setup DynamoMQ

### Required IAM Policy

Please refer to [dynamomq-iam-policy.json](./dynamomq-iam-policy.json) or [dynamomq-iam-policy.tf](./dynamomq-iam-policy.tf)

### Create Table with AWS CLI

```sh
aws dynamodb create-table --cli-input-json file://dynamomq-table.json 
```
Please refer to [dynamomq-table.json](./dynamomq-table.json).

### Create Table with Terraform

Please refer to [dynamomq-table.tf](./dynamomq-table.tf).

## Authentication and access credentials

DynamoMQ's CLI and library configure AWS Config with credentials obtained from external configuration sources. This setup allows for flexible and secure management of access credentials. The following are the default sources for configuration:

### Environment Variables

- `AWS_REGION` - Specifies the AWS region.
- `AWS_PROFILE` - Identifies the AWS profile to be used.
- `AWS_ACCESS_KEY_ID` - Your AWS access key.
- `AWS_SECRET_ACCESS_KEY` - Your AWS secret key.
- `AWS_SESSION_TOKEN` - Session token for temporary credentials.

## Usage for DynamoMQ CLI 

The `dynamomq` command-line interface provides a range of commands to interact with your DynamoDB-based message queue. Below are the available commands and global flags that can be used with `dynamomq`.

### Available Commands

- `completion`: Generate the autocompletion script for the specified shell to ease command usage.
- `delete`: Delete a message from the queue using its ID.
- `dlq`: Retrieve the statistics for the Dead Letter Queue (DLQ), providing insights into failed message processing.
- `enqueue-test`: Send test messages to the DynamoDB table with IDs A-101, A-202, A-303, and A-404; existing messages with these IDs will be overwritten.
- `fail`: Simulate the failure of message processing, which will return the message to the queue for reprocessing.
- `get`: Fetch a specific message from the DynamoDB table using the application domain ID.
- `help`: Display help information about any command.
- `invalid`: Move a message from the standard queue to the DLQ for manual review and correction.
- `ls`: List all message IDs in the queue, limited to a maximum of 10 elements.
- `purge`: Remove all messages from the DynamoMQ table, effectively clearing the queue.
- `qstat`: Retrieve statistics for the queue, offering an overview of its current state.
- `receive`: Receive a message from the queue; this operation will replace the current message ID with the retrieved one.
- `redrive`: Move a message from the DLQ back to the standard queue for reprocessing.
- `reset`: Reset the system information of a message, typically used in message recovery scenarios.

### Global Flags

- `--endpoint-url`: Override the default URL for commands with a specified endpoint URL.
- `-h`, `--help`: Display help information for `dynamomq`.
- `--queueing-index-name`: Specify the name of the queueing index to use (default is `"dynamo-mq-index-queue_type-sent_at"`).
- `--table-name`: Define the name of the DynamoDB table to contain the items (default is `"dynamo-mq-table"`).

To get more detailed information about a specific command, use `dynamomq [command] --help`.

### Interactive Mode

The DynamoMQ CLI supports an Interactive Mode for an enhanced user experience. To enter the Interactive Mode, simply run the `dynamomq` command without specifying any subcommands.

#### Interactive Mode Commands

Once in Interactive Mode, you will have access to a suite of commands to manage and inspect your message queue:

- `qstat` or `qstats`: Retrieves the queue statistics.
- `dlq`: Retrieves the Dead Letter Queue (DLQ) statistics.
- `enqueue-test` or `et`: Sends test messages to the DynamoDB table with IDs: A-101, A-202, A-303, and A-404; if a message with the same ID already exists, it will be overwritten.
- `purge`: Removes all messages from the DynamoMQ table.
- `ls`: Lists all message IDs, displaying a maximum of 10 elements.
- `receive`: Receives a message from the queue and replaces the current ID with the peeked one.
- `id <id>`: Switches the Interactive Mode to app mode, allowing you to perform various operations on a message identified by the provided app domain ID:
  - `sys`: Displays the system info data in a JSON format.
  - `data`: Prints the data as JSON for the current message record.
  - `info`: Prints all information regarding the Message record, including system_info and data in JSON format.
  - `reset`: Resets the system info of the message.
  - `redrive`: Drives a message from the DLQ back to the STANDARD queue.
  - `delete`: Deletes a message by its ID.
  - `fail`: Simulates the failed processing of a message by putting it back into the queue; the message will need to be received again.
  - `invalid`: Moves a message from the standard queue to the DLQ for manual fixing.

## Usage for DynamoMQ SDK

### DynamoMQ Client

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

### DynamoMQ Producer

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

### DynamoMQ Consumer

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

## About the Design of DynamoMQ

### Message Attributes and Table Definition

Here's a diagram showing the table definition used by DynamoMQ to implement its message queuing mechanism.

| Key   | Attributes         | Type   | Example Value                       |
|-------|--------------------|--------|-------------------------------------|
| PK    | id                 | string | A-101                               |
|       | data               | any    | any                                 |
|       | receive_count      | number | 1                                   |
| GSIPK | queue_type         | string | STANDARD or DLQ                     |
|       | version            | number | 1                                   |
|       | created_at         | string | 2006-01-02T15:04:05.999999999Z07:00 |
|       | updated_at         | string | 2006-01-02T15:04:05.999999999Z07:00 |
| GSISK | sent_at            | string | 2006-01-02T15:04:05.999999999Z07:00 |
|       | received_at        | string | 2006-01-02T15:04:05.999999999Z07:00 |
|       | invisible_until_at | string | 2006-01-02T15:04:05.999999999Z07:00 |

#### id (Partition Key)

This is the unique identifier of the message, serving as the partition key in DynamoDB. It ensures efficient data distribution and access within DynamoDB. When using DynamoMQ's publisher, a UUID is generated by default. Users can also specify their own IDs.

#### data  

This field contains the data included in the message. It can be stored in any format supported by DynamoDB.

References:
- [DynamoDB Data Types](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.DataTypes.html)
- [DynamoDB Item sizes and formats](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html)

#### receive_count

This indicates the number of times the message has been received. It's used for managing retries and moving messages to the Dead Letter Queue.

#### queue_type (Partition Key for GSI)

This attribute shows the type of queue where the message is stored, distinguishing between STANDARD and DLQ.

#### version

This is the version number of the message. It increments each time the message is updated, facilitating optimistic concurrency control.

#### created_at  

The timestamp when the message was created, recorded in ISO 8601 format.

#### updated_at  

The timestamp when the message was last updated, recorded in ISO 8601 format.

#### sent_at (Sort Key for GSI)

The timestamp when the message was sent to the queue, recorded in ISO 8601 format.

#### received_at

The timestamp when the message was received, recorded in ISO 8601 format.

#### invisible_until_at

The timestamp indicating when the message will next become visible in the queue. Once this time passes, the message becomes receivable again.

#### Global Secondary Index (GSI)

A GSI with `queue_type` as the partition key and `sent_at` as the sort key is set up to receive messages in the order they are added to the queue.

### Message State Machine

The following state machine diagram illustrates the lifecycle of messages in DynamoMQ and their possible state transitions. The diagram shows how messages are processed in both STANDARD (`queue_type=STANDARD`) and Dead Letter Queues (`queue_type=DLQ`).

![State Machine](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-9383C.png) 

Additionally, the below diagram illustrates how message attributes change with state transitions. Attributes highlighted in red are updated during these transitions.

![Data Transition](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-DCE15.png)

#### Standard Queue Data Transition Explanation

1. **Message Sending**
   - The producer uses the `SendMessage()` function to send a message to the standard queue.
   - The sent message enters the 'READY' state, indicating it is available for receipt.
   - The message is assigned a unique ID and contains data in the `data` field.
   - The `queue_type` attribute is set to `STANDARD`, and the version starts at `1`.
   - Timestamps for `created_at`, `updated_at`, and `sent_at` are recorded.

2. **Message Receipt**
   - The consumer receives the message using the `ReceiveMessage()` function and begins processing.
   - The message transitions to the 'PROCESSING' state, becoming invisible to other consumers during this period.
   - The

 `receive_count` and `version` of the message increment with each receipt.
   - Timestamps for `updated_at` and `received_at` are updated, and `invisible_until_at` is set with a new timestamp.

3. **Successful Processing**
   - Upon successful processing, the consumer removes the message from the queue using the `DeleteMessage()` function.

4. **Failed Processing**
   - In case of a failure, particularly when a retry is needed, the `ChangeMessageVisibility()` is used to update the `invisible_until_at` attribute of the message for later reprocessing.
   - Once the timestamp set in `invisible_until_at` passes, the message returns to the 'READY' state for potential re-receipt.

#### Dead Letter Queue Data Transition Explanation

1. **Move to DLQ**
   - If processing of a message fails beyond the maximum redelivery attempts, the `MoveMessageToDLQ()` function moves it to the Dead Letter Queue (DLQ).
   - At this point, the `queue_type` of the message changes to `DLQ`, and `receive_count` resets to `0`.
   - The message moved to the DLQ reverts to the 'READY' state.

2. **Receipt of Message**
   - Within the DLQ, the message is again received through `ReceiveMessage()` and transitions to the 'PROCESSING' state.
   - During this period, the message becomes invisible to other consumers.

3. **Successful Processing**
   - Once successfully processed, the message is removed from the DLQ using `DeleteMessage()`.

4. **Return to Standard Queue**
   - Using `RedriveMessage()`, the message can be moved back to the original standard queue.
   - This action resets the `queue_type` to `STANDARD’, and the message reverts to the 'READY' state.

## Conclusion

DynamoMQ is a message queuing library that leverages the features of DynamoDB to achieve high scalability, reliability, and cost efficiency. Notably, its ability to dynamically edit message order and attributes enables flexible adaptation to application requirements.

Compared to existing solutions, DynamoMQ offers ease of management for developers while providing the reliability of fully managed services like Amazon SQS. It also encompasses key functionalities expected from a message queue, such as concurrent processing with multiple goroutines, Dead Letter Queues, and ensuring FIFO (First In, First Out) order.

## Acknowledgments

We extend our deepest gratitude to the AWS official blog, "[Implementing priority queueing with Amazon DynamoDB](https://aws.amazon.com/blogs/database/implementing-priority-queueing-with-amazon-dynamodb/)," which served as a reference in the development of DynamoMQ. This blog provides a detailed explanation of implementing priority queuing using Amazon DynamoDB, and this knowledge has been immensely beneficial in constructing DynamoMQ.

## Authors

* **[vvatanabe](https://github.com/vvatanabe/)** - *Main contributor*
* Currently, there are no other contributors

## License

This project is licensed under the MIT License. For detailed licensing information, refer to the [LICENSE](LICENSE) file included in the repository.

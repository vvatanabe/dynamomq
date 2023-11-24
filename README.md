[![Build](https://github.com/vvatanabe/dynamomq/actions/workflows/build.yml/badge.svg)](https://github.com/vvatanabe/dynamomq/actions/workflows/build.yml) [![Go Report Card](https://goreportcard.com/badge/github.com/vvatanabe/dynamomq)](https://goreportcard.com/report/github.com/vvatanabe/dynamomq) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq)  [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=bugs)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=vvatanabe_dynamomq&metric=coverage)](https://sonarcloud.io/summary/new_code?id=vvatanabe_dynamomq)

<p align="center">
  <img width="460" height="300" src="https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-192C1.png">
</p>

Implementing message queueing with Amazon DynamoDB in Go.

## Table of Contents

- [Current Status](#current-status)
- [Motivation](#motivation)
- [Features](#features)
- [Installation](#installation)
  * [DynamoMQ CLI](#dynamomq-cli)
  * [DynamoMQ Library](#dynamomq-library)
- [Setup DynamoMQ](#setup-dynamomq)
  * [Required IAM Policy](#required-iam-policy)
  * [Create Table with AWS CLI](#create-table-with-aws-cli)
  * [Create Table with Terraform](#create-table-with-terraform)
- [Authentication and access credentials](#authentication-and-access-credentials)
  * [Environment Variables](#environment-variables)
  * [Shared Configuration and Credentials Files](#shared-configuration-and-credentials-files)
- [Usage for DynamoMQ CLI](#usage-for-dynamomq-cli)
  * [Available Commands](#available-commands)
  * [Global Flags](#global-flags)
  * [Example Usage](#example-usage)
  * [Interactive Mode](#interactive-mode)
- [Usage for DynamoMQ Library](#usage-for-dynamomq-library)
  * [DynamoMQ Client](#dynamomq-client)
  * [DynamoMQ Producer](#dynamomq-producer)
  * [DynamoMQ Consumer](#dynamomq-consumer)
- [Software Design](#software-design)
  * [State Machine](#state-machine)
  * [Table Definition](#table-definition)
  * [Data Transition](#data-transition)
- [Authors](#authors)
- [License](#license)


## Current Status

This project is actively under development, but it is currently in version 0. Please be aware that the public API and exported methods may undergo changes.

## Motivation

> DynamoDB is a key-value and document database that delivers single-digit millisecond performance at any scale. It’s a serverless and fully managed service that you can use for mobile, web, gaming, ad tech, IoT, and other applications that need low-latency data access at a large scale.
>
> There are many queuing implementations that offer persistence, single-message processing, and distributed computing. Some popular queuing solutions are Amazon SQS, Amazon MQ, Apache ActiveMQ, RabbitMQ, and Kafka. Those services handle various queuing features and functions with several different characteristics, such as methods of implementation, scaling, and performance.
>
> However, most of those queuing systems cannot easily change the order of the items after they arrive in the queue. Discussed implementation with DynamoDB can change the order in the queue or cancel items before processing.

Quoted from AWS official blog: [Implementing Priority Queueing with Amazon DynamoDB](https://aws.amazon.com/blogs/database/implementing-priority-queueing-with-amazon-dynamodb/)

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

### Shared Configuration and Credentials Files

These files provide a common location for storing AWS credentials and configuration settings, enabling consistent credential management across different AWS tools and applications.

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
- `--queueing-index-name`: Specify the name of the queueing index to use (default is `"dynamo-mq-index-queue_type-queue_add_timestamp"`).
- `--table-name`: Define the name of the DynamoDB table to contain the items (default is `"dynamo-mq-table"`).

To get more detailed information about a specific command, use `dynamomq [command] --help`.

### Example Usage

Here are a few examples of how to use the `dynamomq` commands:

```sh
# Generate autocompletion script for bash
dynamomq completion bash

# Delete a message with ID 'A-123'
dynamomq delete --id A-123

# Retrieve DLQ statistics
dynamomq dlq

# Enqueue test messages
dynamomq enqueue-test

# Get a message by ID
dynamomq get --id A-123

# List the first 10 message IDs in the queue
dynamomq ls

# Receive a message from the queue
dynamomq receive

# Reset system information of a message with ID
dynamomq reset --id A-123
```

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

## Usage for DynamoMQ Library

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

## Software Design

### State Machine

The state machine diagram below illustrates the key steps a message goes through as it traverses the system.

![State Machine](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-4B362.png) 

#### Basic Flow

1. **SendMessage()**: A user sends a message that is placed in the `READY` state in the queue.

2. **ReceiveMessage()**: The message moves from `READY` to `PROCESSING` status as it is picked up for processing.

3. **DeleteMessage()**: If processing is successful, the message is deleted from the queue.

#### Error Handling

1. **UpdateMessageAsVisibility()**: If processing fails, the message is made visible again in the `READY` state for retry, and its visibility timeout is updated.

2. **MoveMessageToDLQ()**: If the message exceeds the retry limit, it is moved to the Dead Letter Queue (DLQ). The DLQ is used to isolate problematic messages for later analysis.

#### Dead Letter Queue (DLQ)

1. **RedriveMessage()**: The system may choose to return a message to the standard queue if it determines that the issues have been resolved. This is achieved through the `Redrive` operation.

2. **ReceiveMessage()**: Messages in the DLQ are also moved from `READY` to `PROCESSING` status, similar to regular queue messages.

3. **DeleteMessage()**: Once a message in the DLQ is successfully processed, it is deleted from the queue.

This design ensures that DynamoMQ maintains message reliability while enabling tracking and analysis of messages in the event of errors. The use of a DLQ minimizes the impact of failures while maintaining system resiliency.

### Table Definition

The DynamoDB table for the DynamoMQ message queue system is designed to efficiently manage and track the status of messages. Here’s a breakdown of the table schema:

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

**PK (Primary Key)** `ID`: A unique identifier for each message, such as 'A-101'. This is a string value that facilitates the retrieval and management of messages.

**GSIPK (Global Secondary Index - Partition Key)** `queue_type`: Used to categorize messages by `queue_type`, such as 'STANDARD' or 'DLQ' (Dead Letter Queue), allowing for quick access and operations on subsets of the queue.

**GSISK (Global Secondary Index - Sort Key)** `queue_add_timestamp`: The timestamp when the message was added to the queue. Facilitates the ordering of messages based on the time they were added to the queue, which is useful for implementing FIFO (First-In-First-Out) or other ordering mechanisms.

**Attributes**: These are the various properties associated with each message:
- `data`: This attribute holds the content of the message and can be of any type.
- `status`: Indicates the current state of the message, either 'READY' for new messages or 'PROCESSING' for messages being processed.
- `receive_count`: A numerical count of how many times the message has been retrieved from the queue.
- `version`: A number that can be used for optimistic locking and to ensure that the message is not being concurrently modified.
- `creation_timestamp`: The date and time when the message was created. ISO 8601 format.
- `last_updated_timestamp`: The date and time when the message was last updated. ISO 8601 format.
- `queue_peek_timestamp`: The timestamp when the message was last viewed without being altered. ISO 8601 format.

### Data Transition

This data transition diagram serves as a map for developers and operators to understand how messages flow through the DynamoMQ system, providing insight into the mechanisms of message processing, failure handling, and retries within a DynamoDB-backed queue.

![Data Transition](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-D143B.png)

#### Initial State

- **SendMessage()**: A message is created with an initial `status` of 'READY'. It includes a unique `id`, arbitrary `data`, and a `receive_count` set to 0, indicating it has not yet been processed. The `queue_type` is 'STANDARD', and timestamps are recorded for creation, last update, and when added to the queue.

#### Processing

- **ReceiveMessage()**: The message `status` changes to 'PROCESSING', the `receive_count` increments to reflect the number of times it's been retrieved, and the `version` number increases to facilitate optimistic locking. Timestamps are updated accordingly.

#### Retry Logic

- **UpdateMessageAsVisible()**: If processing fails, the message's visibility is updated to make it available for retry, and the `receive_count` is incremented. Timestamps are refreshed to reflect the most recent update.

#### Dead Letter Queue

- **MoveMessageToDLQ()**: After the maximum number of retries is reached without successful processing, the message is moved to the DLQ. Its `queue_type` changes to 'DLQ', and `receive_count` is reset, indicating that it's ready for a fresh attempt or investigation.

#### Redrive Policy

- **RedriveMessage()**: If issues are resolved, messages in the DLQ can be sent back to the standard queue for processing. This is depicted by the `RedriveMessage()` operation, which resets the `receive_count` and alters the `queue_type` back to 'STANDARD', along with updating the timestamps.

## Authors

* **[vvatanabe](https://github.com/vvatanabe/)** - *Main contributor*
* Currently, there are no other contributors

## License

This project is licensed under the MIT License. For detailed licensing information, refer to the [LICENSE](LICENSE) file included in the repository.

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

### For library

This package can be installed as library with the go get command:
```
$ go get -u github.com/vvatanabe/dynamomq@latest
```

### For CLI

This package can be installed as CLI with the go install command:
```
$ go install github.com/vvatanabe/dynamomq/cmd/dynamomq@latest
```

## Usage

### For CLI

Coming Soon

### For Library

Coming Soon

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

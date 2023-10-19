# DynamoMQ

Implementing priority queueing with Amazon DynamoDB in Go.

## Status

This project is actively under development, but it is currently in version 0. Please be aware that the public API and exported methods may undergo changes.


## State Machine

![State Machine](https://cacoo.com/diagrams/DjoA2pSKnhCghTYM-452C2.png) 

## Table Definition

| Key     | Attributes               | Type   | Example Value                |
|---------|--------------------------|--------|-----------------------------|
| PK      | id                       | string | A-101                       |
|         | data                     | any    | any                         |
|         | status                   | string | PENDING                     |
|         | receive_count            | number | 1                           |
| GSI2PK  | queued                   | number | 1                           |
| GSI3PK  | DLQ                      | number | 1                           |
|         | version                  | number | 1                           |
|         | creation_timestamp       | string | 2023-12-01T12:00:00Z00:00   |
| SK      | last_updated_timestamp   | string | 2023-12-01T12:00:00Z00:00   |
| GSI2SK  | queue_add_timestamp      | string | 2023-12-01T12:00:00Z00:00   |
|         | queue_peek_timestamp     | string | 2023-12-01T12:00:00Z00:00   |
|         | queue_complete_timestamp | string | 2023-12-01T12:00:00Z00:00   |
| GSI3SK  | dlq_add_timestamp        | string | 2023-12-01T12:00:00Z00:00   |

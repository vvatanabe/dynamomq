package constant

import "time"

const (
	DefaultTableName                  = "dynamo-mq-table"
	DefaultQueueingIndexName          = "dynamo-mq-index-queue_type-sequence_key"
	DefaultRetryMaxAttempts           = 10
	DefaultVisibilityTimeoutInSeconds = 30
	DefaultVisibilityTimeout          = DefaultVisibilityTimeoutInSeconds * time.Second
	DefaultMaxListMessages            = 10
)

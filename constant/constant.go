package constant

const (
	AwsRegionDefault           = "us-east-1"
	AwsProfileDefault          = "default"
	DefaultTableName           = "Shipment"
	QueueingIndexName          = "queueud-last_updated_timestamp-index"
	DlqQueueingIndexName       = "dlq-last_updated_timestamp-index"
	VisibilityTimeoutInMinutes = 1
)

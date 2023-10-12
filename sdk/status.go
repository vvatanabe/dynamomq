package sdk

type Status string

const (
	StatusPending    Status = "PENDING"
	StatusReady      Status = "READY"
	StatusProcessing Status = "PROCESSING"
	StatusCompleted  Status = "COMPLETED"
	StatusInDLQ      Status = "IN_DLQ"
)

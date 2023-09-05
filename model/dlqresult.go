package model

import (
	"encoding/json"
	"fmt"
	"strings"
)

// DLQResult represents the structure to store DLQ depth statistics.
type DLQResult struct {
	First100IDsInQueue []string `json:"first_100_IDs_in_queue"`
	TotalRecordsInDLQ  int      `json:"total_records_in_DLQ"`
}

// NewDLQResult creates a new instance of DLQResult.
func NewDLQResult() *DLQResult {
	return &DLQResult{}
}

// CreateFromJSON creates a DLQResult object from JSON string.
func CreateFromJSON(jsonInString string) (*DLQResult, error) {
	var result DLQResult
	err := json.Unmarshal([]byte(jsonInString), &result)
	return &result, err
}

func (r *DLQResult) String() string {
	var sb strings.Builder
	sb.WriteString("Queue statistics:\n")
	sb.WriteString(fmt.Sprintf(" >> Total records in DLQ: %d\n", r.TotalRecordsInDLQ))
	if len(r.First100IDsInQueue) > 0 {
		sb.WriteString("   >>> BANs in DLQ: { ")
		sb.WriteString(strings.Join(r.First100IDsInQueue, ", "))
		sb.WriteString(" }\n")
	}
	return sb.String()
}

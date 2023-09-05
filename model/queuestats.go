package model

import (
	"encoding/json"
	"fmt"
)

type QueueStats struct {
	First100IDsInQueue         []string `json:"first_100_IDs_in_queue"`
	First100SelectedIDsInQueue []string `json:"first_100_selected_IDs_in_queue"`
	TotalRecordsInQueue        int      `json:"total_records_in_queue"`
	TotalRecordsInProcessing   int      `json:"total_records_in_queue_selected_for_processing"`
	TotalRecordsNotStarted     int      `json:"total_records_in_queue_pending_for_processing"`
}

func NewQueueStats() *QueueStats {
	return &QueueStats{}
}

func CreateObjectFromJson(jsonInString string) (*QueueStats, error) {
	var stats QueueStats
	err := json.Unmarshal([]byte(jsonInString), &stats)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

func (q *QueueStats) String() string {
	sb := fmt.Sprintf("Queue statistics:\n")
	sb += fmt.Sprintf(" >> Total records in the queue: %d\n", q.TotalRecordsInQueue)

	if len(q.First100IDsInQueue) > 0 {
		sb += "   >>> IDs in the queue: { "
		for i, id := range q.First100IDsInQueue {
			if i > 0 {
				sb += ", "
			}
			sb += id
		}
		sb += " }\n"
	}

	sb += fmt.Sprintf("   >>> Records in processing: %d\n", q.TotalRecordsInProcessing)
	if len(q.First100SelectedIDsInQueue) > 0 {
		sb += "   >>> Selected IDs in the queue: { "
		for i, id := range q.First100SelectedIDsInQueue {
			if i > 0 {
				sb += ", "
			}
			sb += id
		}
		sb += " }\n"
	}

	sb += fmt.Sprintf("   >>> Records in queue but not in processing: %d\n", q.TotalRecordsNotStarted)

	return sb
}

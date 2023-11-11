package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/vvatanabe/dynamomq"
)

func printMessageWithData(message string, data any) {
	dump, err := marshalIndent(data)
	if err != nil {
		printError(err)
		return
	}
	fmt.Printf("%s%s\n", message, dump)
}

func marshalIndent(v any) ([]byte, error) {
	dump, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return dump, nil
}

func printCLIModeRestriction(command string) {
	printError(fmt.Sprintf("%s command can be only used in the Interactive's App mode. Call first `id <record-id>", command))
}

func printError(err any) {
	fmt.Printf("ERROR: %v\n", err)
}

func printErrorWithID(err error, id string) {
	fmt.Printf("ERROR: %v, ID: %s\n", err, id)
}

func printQueueStatus(stats *dynamomq.GetQueueStatsOutput) {
	printMessageWithData("Queue status:\n", stats)
}

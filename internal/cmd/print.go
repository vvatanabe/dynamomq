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

func printError(err any) {
	fmt.Printf("ERROR: %v\n", err)
}

func printQueueStatus(stats *dynamomq.GetQueueStatsOutput) {
	printMessageWithData("Queue status:\n", stats)
}

func errorCLIModeRestriction(command string) error {
	return fmt.Errorf("%s command can be only used in the Interactive mode. Call first `id", command)
}

func errorWithID(err error, id string) error {
	return fmt.Errorf("%v, ID: %s", err, id)
}

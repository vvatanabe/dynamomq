package cmd

import (
	"context"
	"fmt"

	"github.com/vvatanabe/dynamomq/internal/clock"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset the system info of the message",
	Long:  `Reset the system info of the message.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			printError(err)
			return
		}
		id := flgs.ID
		retrieved, err := client.GetMessage(ctx, &dynamomq.GetMessageInput{
			ID: id,
		})
		if err != nil {
			printError(err)
			return
		}
		if retrieved.Message == nil {
			printError(fmt.Sprintf("Not found message: %s", id))
			return
		}
		retrieved.Message.ResetSystemInfo(clock.Now())
		_, err = client.ReplaceMessage(ctx, &dynamomq.ReplaceMessageInput[any]{
			Message: retrieved.Message,
		})
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("", ResetResult{
			Message: retrieved.Message,
		})
		return
	},
}

type ResetResult struct {
	Message *dynamomq.Message[any] `json:"message"`
}

func init() {
	rootCmd.AddCommand(resetCmd)
	resetCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	resetCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	resetCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
	resetCmd.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
}

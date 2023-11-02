package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var invalidCmd = &cobra.Command{
	Use:   "invalid",
	Short: "Remove a message from the standard queue to dead letter queue (DLQ) for manual fix",
	Long:  `Remove a message from the standard queue to dead letter queue (DLQ) for manual fix.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			printError(err)
			return
		}
		id := flgs.ID
		_, err = client.MoveMessageToDLQ(ctx, &dynamomq.MoveMessageToDLQInput{
			ID: id,
		})
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("", InvalidResult{ID: id})
		return
	},
}

type InvalidResult struct {
	ID string `json:"id"`
}

func init() {
	rootCmd.AddCommand(invalidCmd)
	invalidCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	invalidCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	invalidCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
	invalidCmd.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
}

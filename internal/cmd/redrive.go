package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var redriveCmd = &cobra.Command{
	Use:   "redrive",
	Short: "Redrive a message to STANDARD from DLQ",
	Long:  `Redrive a message to STANDARD from DLQ.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			printError(err)
			return
		}
		id := flgs.ID
		result, err := client.RedriveMessage(ctx, &dynamomq.RedriveMessageInput{
			ID: id,
		})
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("", result)
		return
	},
}

func init() {
	rootCmd.AddCommand(redriveCmd)
	redriveCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	redriveCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	redriveCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
	redriveCmd.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
}

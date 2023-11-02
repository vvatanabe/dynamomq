package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a message by ID",
	Long:  `Delete a message by ID.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			printError(err)
			return
		}
		id := flgs.ID
		_, err = client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
			ID: id,
		})
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("", DeleteResult{ID: id})
		return
	},
}

type DeleteResult struct {
	ID string `json:"id"`
}

func init() {
	rootCmd.AddCommand(deleteCmd)
	deleteCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	deleteCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	deleteCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
	deleteCmd.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
}

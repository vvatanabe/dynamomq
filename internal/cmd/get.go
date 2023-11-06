package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Get a message the application object from DynamoDB by app domain ID",
	Long:  `Get a message the application object from DynamoDB by app domain ID.`,
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
		printMessageWithData("", ReceiveResult{
			Message: retrieved.Message,
		})
		return
	},
}

type GetResult struct {
	Message *dynamomq.Message[any] `json:"message"`
}

func init() {
	rootCmd.AddCommand(getCmd)
	getCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	getCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	getCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
	getCmd.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
}

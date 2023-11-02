package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var failCmd = &cobra.Command{
	Use:   "fail",
	Short: "Simulate failed message's processing ... put back to the queue; needs to be receive again",
	Long:  `Simulate failed message's processing ... put back to the queue; needs to be receive again.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			printError(err)
			return
		}
		id := flgs.ID
		_, err = client.UpdateMessageAsVisible(ctx, &dynamomq.UpdateMessageAsVisibleInput{
			ID: id,
		})
		if err != nil {
			printError(err)
			return
		}
		retrieved, err := client.GetMessage(ctx, &dynamomq.GetMessageInput{ID: id})
		if err != nil {
			printError(err)
			return
		}
		if retrieved.Message == nil {
			printError(fmt.Sprintf("Message's [%s] not found!", id))
			return
		}
		message := retrieved.Message
		printMessageWithData("", FailResult[any]{Message: message})
		return
	},
}

type FailResult[T any] struct {
	Message *dynamomq.Message[T] `json:"message"`
}

func init() {
	rootCmd.AddCommand(failCmd)
	failCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	failCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	failCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
	failCmd.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
}

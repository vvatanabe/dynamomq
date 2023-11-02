package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var receiveCmd = &cobra.Command{
	Use:   "receive",
	Short: "Receive a message from the Queue .. it will replace the current ID with the peeked one",
	Long:  `Receive a message from the Queue .. it will replace the current ID with the peeked one.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			printError(err)
			return
		}
		rr, err := client.ReceiveMessage(ctx, &dynamomq.ReceiveMessageInput{})
		if err != nil {
			printError(fmt.Sprintf("ReceiveMessage has failed! message: %s", err))
			return
		}
		printMessageWithData("", ReceiveResult{
			Message: rr.PeekedMessageObject,
		})
		return
	},
}

type ReceiveResult struct {
	Message *dynamomq.Message[any] `json:"message"`
}

func init() {
	rootCmd.AddCommand(receiveCmd)
	receiveCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	receiveCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	receiveCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
}

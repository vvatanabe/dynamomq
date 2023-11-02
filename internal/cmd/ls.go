package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List all message IDs ... max 10 elements",
	Long:  `List all message IDs ... max 10 elements.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			printError(err)
			return
		}
		out, err := client.ListMessages(ctx, &dynamomq.ListMessagesInput{Size: 10})
		if err != nil {
			printError(err)
			return
		}
		var result LSResult
		for _, m := range out.Messages {
			result.Statuses = append(result.Statuses, Status{
				ID:        m.ID,
				Status:    m.Status,
				QueueType: m.QueueType,
			})
		}
		printMessageWithData("", result)
		return
	},
}

type LSResult struct {
	Statuses []Status `json:"statuses"`
}

type Status struct {
	ID        string             `json:"id"`
	Status    dynamomq.Status    `json:"status"`
	QueueType dynamomq.QueueType `json:"queue_type"`
}

func init() {
	rootCmd.AddCommand(lsCmd)
	lsCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	lsCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	lsCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
}

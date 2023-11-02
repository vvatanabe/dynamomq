package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "It will remove all message from DynamoMQ table",
	Long:  `It will remove all message from DynamoMQ table.`,
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
		var result PurgeResult
		if len(out.Messages) == 0 {
			printMessageWithData("", result)
			return
		}
		for _, m := range out.Messages {
			_, err := client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
				ID: m.ID,
			})
			if err != nil {
				result.Failures = append(result.Failures, Failure{
					ID:    m.ID,
					Error: err,
				})
				continue
			}
			result.Successes = append(result.Successes, m.ID)
		}
		printMessageWithData("", result)
		return
	},
}

type PurgeResult struct {
	Successes []string  `json:"successes"`
	Failures  []Failure `json:"failures"`
}

type Failure struct {
	ID    string
	Error error
}

func init() {
	rootCmd.AddCommand(purgeCmd)
	purgeCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	purgeCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	purgeCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
}

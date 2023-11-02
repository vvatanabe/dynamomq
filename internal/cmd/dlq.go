package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var dlqCmd = &cobra.Command{
	Use:   "dlq",
	Short: "Retrieves the Dead Letter Queue (DLQ) statistics",
	Long:  `Retrieves the Dead Letter Queue (DLQ) statistics.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			return fmt.Errorf("... %v\n", err)
		}
		stats, err := client.GetDLQStats(ctx, &dynamomq.GetDLQStatsInput{})
		if err != nil {
			return err
		}
		printMessageWithData("", stats)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(dlqCmd)
	dlqCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	dlqCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	dlqCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
}

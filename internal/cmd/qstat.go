package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

var qstatCmd = &cobra.Command{
	Use:   "qstat",
	Short: "Retrieves the Queue statistics",
	Long:  `Retrieves the Queue statistics.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			return fmt.Errorf("... %v\n", err)
		}
		stats, err := client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
		if err != nil {
			return err
		}
		printMessageWithData("", stats)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(qstatCmd)
	qstatCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	qstatCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	qstatCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
}

package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateQueueStatCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "qstat",
		Short: "Retrieves the queue statistics",
		Long:  `Retrieves the queue statistics.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			stats, err := client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
			if err != nil {
				return err
			}
			printMessageWithData("", stats)
			return nil
		},
	}
}

func init() {
	c := defaultCommandFactory.CreateQueueStatCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	rootCmd.AddCommand(c)
}

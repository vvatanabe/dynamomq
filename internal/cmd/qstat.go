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
			return f.executeStatsCommand(flgs, func(ctx context.Context, client dynamomq.Client[any]) (any, error) {
				return client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
			})
		},
	}
}

func (f CommandFactory) executeStatsCommand(flgs *Flags, statsFunc func(context.Context, dynamomq.Client[any]) (any, error)) error {
	ctx := context.Background()
	client, _, err := f.CreateDynamoMQClient(ctx, flgs)
	if err != nil {
		return err
	}
	stats, err := statsFunc(ctx, client)
	if err != nil {
		return err
	}
	printMessageWithData("", stats)
	return nil
}

func init() {
	c := defaultCommandFactory.CreateQueueStatCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.IndexName, flagMap.IndexName.Name, flagMap.IndexName.Value, flagMap.IndexName.Usage)
	root.AddCommand(c)
}

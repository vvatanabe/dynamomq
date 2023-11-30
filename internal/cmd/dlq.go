package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateDLQCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "dlq",
		Short: "Retrieves the Dead Letter Queue (DLQ) statistics",
		Long:  `Retrieves the Dead Letter Queue (DLQ) statistics.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return f.executeStatsCommand(flgs, func(ctx context.Context, client dynamomq.Client[any]) (any, error) {
				return client.GetDLQStats(ctx, &dynamomq.GetDLQStatsInput{})
			})
		},
	}
}

func init() {
	c := defaultCommandFactory.CreateDLQCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
	root.AddCommand(c)
}

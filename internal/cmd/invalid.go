package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateInvalidCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "invalid",
		Short: "Remove a message from the standard queue to dead letter queue (DLQ) for manual fix",
		Long:  `Remove a message from the standard queue to dead letter queue (DLQ) for manual fix.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			id := flgs.ID
			_, err = client.MoveMessageToDLQ(ctx, &dynamomq.MoveMessageToDLQInput{
				ID: id,
			})
			if err != nil {
				return err
			}
			return nil
		},
	}
}

func init() {
	c := defaultCommandFactory.CreateInvalidCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
	rootCmd.AddCommand(c)
}

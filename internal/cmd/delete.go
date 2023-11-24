package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateDeleteCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete",
		Short: "Delete a message by ID",
		Long:  `Delete a message by ID.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			_, err = client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
				ID: flgs.ID,
			})
			if err != nil {
				return err
			}
			return nil
		},
	}
}

func init() {
	c := defaultCommandFactory.CreateDeleteCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
	root.AddCommand(c)
}

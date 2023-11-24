package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateRedriveCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "redrive",
		Short: "Redrive a message to STANDARD from DLQ",
		Long:  `Redrive a message to STANDARD from DLQ.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			result, err := client.RedriveMessage(ctx, &dynamomq.RedriveMessageInput{
				ID: flgs.ID,
			})
			if err != nil {
				return err
			}
			printMessageWithData("", result)
			return nil
		},
	}
}

func init() {
	c := defaultCommandFactory.CreateRedriveCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.IndexName, flagMap.IndexName.Name, flagMap.IndexName.Value, flagMap.IndexName.Usage)
	root.AddCommand(c)
}

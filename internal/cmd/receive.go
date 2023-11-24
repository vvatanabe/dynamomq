package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreatReceiveCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "receive",
		Short: "Receive a message from the queue .. it will replace the current ID with the peeked one",
		Long:  `Receive a message from the queue .. it will replace the current ID with the peeked one.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			rr, err := client.ReceiveMessage(ctx, &dynamomq.ReceiveMessageInput{})
			if err != nil {
				return err
			}
			printMessageWithData("", rr.PeekedMessageObject)
			return nil
		},
	}
}

func init() {
	c := defaultCommandFactory.CreatReceiveCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.IndexName, flagMap.IndexName.Name, flagMap.IndexName.Value, flagMap.IndexName.Usage)
	root.AddCommand(c)
}

package cmd

import (
	"context"
	"fmt"

	"github.com/vvatanabe/dynamomq/internal/clock"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateResetCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "reset",
		Short: "Reset the system info of the message",
		Long:  `Reset the system info of the message.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			id := flgs.ID
			retrieved, err := client.GetMessage(ctx, &dynamomq.GetMessageInput{
				ID: id,
			})
			if err != nil {
				return err
			}
			if retrieved.Message == nil {
				return fmt.Errorf("not found message: %s", id)
			}
			ResetSystemInfo(retrieved.Message, clock.Now())
			_, err = client.ReplaceMessage(ctx, &dynamomq.ReplaceMessageInput[any]{
				Message: retrieved.Message,
			})
			if err != nil {
				return err
			}
			printMessageWithData("", retrieved.Message)
			return nil
		},
	}
}

func init() {
	c := defaultCommandFactory.CreateResetCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
	root.AddCommand(c)
}

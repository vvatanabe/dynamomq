package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateFailCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "fail",
		Short: "Simulate failed message's processing ... put back to the queue; needs to be receive again",
		Long:  `Simulate failed message's processing ... put back to the queue; needs to be receive again.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			_, err = client.ChangeMessageVisibility(ctx, &dynamomq.ChangeMessageVisibilityInput{
				ID: flgs.ID,
			})
			if err != nil {
				return err
			}
			retrieved, err := client.GetMessage(ctx, &dynamomq.GetMessageInput{ID: flgs.ID})
			if err != nil {
				return err
			}
			if retrieved.Message == nil {
				return fmt.Errorf("message's [%s] not found", flgs.ID)
			}
			printMessageWithData("", retrieved.Message)
			return nil
		},
	}
}

func init() {
	c := defaultCommandFactory.CreateFailCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
	root.AddCommand(c)
}

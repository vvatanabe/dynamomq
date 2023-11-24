package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateGetCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Get a message the application object from DynamoDB by app domain ID",
		Long:  `Get a message the application object from DynamoDB by app domain ID.`,
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
			printMessageWithData("", retrieved.Message)
			return nil
		},
	}
}

func init() {
	c := defaultCommandFactory.CreateGetCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
	root.AddCommand(c)
}

package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/test"
)

func (f CommandFactory) CreateEnqueueTestCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "enqueue-test",
		Short: "Send test messages in DynamoDB table: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it",
		Long:  `Send test messages in DynamoDB table: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			var result EnqueueTestResult
			defer printMessageWithData("", result)
			for _, id := range []string{"A-101", "A-202", "A-303", "A-404"} {
				_, err = client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
					ID: id,
				})
				if err != nil {
					return err
				}
				_, err = client.SendMessage(ctx, &dynamomq.SendMessageInput[any]{
					ID:   id,
					Data: test.NewMessageData(id),
				})
				if err != nil {
					return err
				}
				result.Successes = append(result.Successes, id)
			}
			return nil
		},
	}
}

type EnqueueTestResult struct {
	Successes []string `json:"successes"`
}

func init() {
	c := defaultCommandFactory.CreateEnqueueTestCommand(flgs)
	setDefaultFlags(c, flgs)
	c.Flags().StringVar(&flgs.ID, flagMap.ID.Name, flagMap.ID.Value, flagMap.ID.Usage)
	rootCmd.AddCommand(c)
}

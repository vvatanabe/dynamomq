package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreatePurgeCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "purge",
		Short: "It will remove all message from DynamoMQ table",
		Long:  `It will remove all message from DynamoMQ table.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			out, err := client.ListMessages(ctx, &dynamomq.ListMessagesInput{Size: dynamomq.DefaultMaxListMessages})
			if err != nil {
				return err
			}
			var result PurgeResult
			if len(out.Messages) == 0 {
				printMessageWithData("", result)
				return nil
			}
			for _, m := range out.Messages {
				_, delErr := client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
					ID: m.ID,
				})
				if delErr != nil {
					result.Failures = append(result.Failures, Failure{
						ID:    m.ID,
						Error: delErr,
					})
					continue
				}
				result.Successes = append(result.Successes, m.ID)
			}
			printMessageWithData("", result)
			return nil
		},
	}
}

type PurgeResult struct {
	Successes []string  `json:"successes"`
	Failures  []Failure `json:"failures"`
}

type Failure struct {
	ID    string
	Error error
}

func init() {
	c := defaultCommandFactory.CreatePurgeCommand(flgs)
	setDefaultFlags(c, flgs)
	root.AddCommand(c)
}

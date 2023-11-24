package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

func (f CommandFactory) CreateLSCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List all message IDs ... max 10 elements",
		Long:  `List all message IDs ... max 10 elements.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, _, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
			}
			out, err := client.ListMessages(ctx, &dynamomq.ListMessagesInput{Size: 10})
			if err != nil {
				return err
			}
			var result LSResult
			for _, m := range out.Messages {
				result.Statuses = append(result.Statuses, Status{
					ID:        m.ID,
					Status:    m.Status,
					QueueType: m.QueueType,
				})
			}
			printMessageWithData("", result)
			return nil
		},
	}
}

type LSResult struct {
	Statuses []Status `json:"statuses"`
}

type Status struct {
	ID        string             `json:"id"`
	Status    dynamomq.Status    `json:"status"`
	QueueType dynamomq.QueueType `json:"queue_type"`
}

func init() {
	c := defaultCommandFactory.CreateLSCommand(flgs)
	setDefaultFlags(c, flgs)
	root.AddCommand(c)
}

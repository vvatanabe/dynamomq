package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

var enqueueTestCmd = &cobra.Command{
	Use:   "enqueue-test",
	Short: "Send test Message records in DynamoDB table: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it",
	Long:  `Send test Message records in DynamoDB table: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		client, _, err := createDynamoMQClient[any](ctx, flgs)
		if err != nil {
			printError(err)
			return
		}
		var result EnqueueTestResult
		ids := []string{"A-101", "A-202", "A-303", "A-404"}
		for _, id := range ids {
			message := dynamomq.NewDefaultMessage[test.MessageData](id, test.NewMessageData(id), clock.Now())
			item, err := message.MarshalMap()
			if err != nil {
				result.Failures = append(result.Failures, Failure{
					ID:    message.ID,
					Error: err,
				})
				continue
			}
			_, err = client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
				ID: id,
			})
			if err != nil {
				result.Failures = append(result.Failures, Failure{
					ID:    message.ID,
					Error: err,
				})
				continue
			}
			_, err = client.GetDynamodbClient().PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(flgs.TableName),
				Item:      item,
			})
			if err != nil {
				result.Failures = append(result.Failures, Failure{
					ID:    message.ID,
					Error: err,
				})
				continue
			}
			result.Successes = append(result.Successes, message.ID)
		}
		printMessageWithData("", result)
		return
	},
}

type EnqueueTestResult struct {
	Successes []string  `json:"successes"`
	Failures  []Failure `json:"failures"`
}

func init() {
	rootCmd.AddCommand(enqueueTestCmd)
	enqueueTestCmd.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	enqueueTestCmd.Flags().StringVar(&flgs.QueueingIndexName, flagMap.QueueingIndexName.Name, flagMap.QueueingIndexName.Value, flagMap.QueueingIndexName.Usage)
	enqueueTestCmd.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
}

package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"

	"github.com/vvatanabe/dynamomq"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

var flgs = &Flags{}

var rootCmd = &cobra.Command{
	Use:   "dynamomq",
	Short: "dynamomq is a tool for implementing message queueing with Amazon DynamoDB in Go",
	Long: `dynamomq is a tool for implementing message queueing with Amazon DynamoDB in Go.

Environment Variables:
  * AWS_REGION
  * AWS_PROFILE
  * AWS_ACCESS_KEY_ID
  * AWS_SECRET_ACCESS_KEY
  * AWS_SESSION_TOKEN
  refs: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
`,
	Version: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		defer fmt.Printf("... Interactive is ending\n\n\n")

		fmt.Println("===========================================================")
		fmt.Println(">> Welcome to DynamoMQ CLI! [INTERACTIVE MODE]")
		fmt.Println("===========================================================")
		fmt.Println("for help, enter one of the following: ? or h or help")
		fmt.Println("all commands in CLIs need to be typed in lowercase")
		fmt.Println("")

		ctx := context.Background()
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("Failed to load aws config: %s\n", err)
		}

		fmt.Printf("AWSRegion: %s\n", cfg.Region)
		fmt.Printf("TableName: %s\n", flgs.TableName)
		fmt.Printf("EndpointURL: %s\n", flgs.EndpointURL)
		fmt.Println("")

		client, err := dynamomq.NewFromConfig[any](cfg,
			dynamomq.WithTableName(flgs.TableName),
			dynamomq.WithAWSBaseEndpoint(flgs.EndpointURL))
		if err != nil {
			return fmt.Errorf("... AWS session could not be established!: %v\n", err)
		}
		fmt.Println("... AWS session is properly established!")

		c := Interactive{
			TableName: flgs.TableName,
			Client:    client,
			Message:   nil,
		}

		// 1. Create a Scanner using the InputStream available.
		scanner := bufio.NewScanner(os.Stdin)

		for {
			// 2. Don't forget to prompt the user
			if c.Message != nil {
				fmt.Printf("\nID <%s> >> Enter command: ", c.Message.ID)
			} else {
				fmt.Print("\n>> Enter command: ")
			}

			// 3. Use the Scanner to read a line of text from the user.
			scanned := scanner.Scan()
			if !scanned {
				break
			}

			input := scanner.Text()
			if input == "" {
				continue
			}

			command, params := parseInput(input)
			switch command {
			case "":
				continue
			case "quit", "q":
				return nil
			default:
				// 4. Now, you can do anything with the input string that you need to.
				// Like, output it to the user.
				c.Run(context.Background(), command, params)
			}
		}
		return nil
	},
}

func parseInput(input string) (command string, params []string) {
	input = strings.TrimSpace(input)
	arr := strings.Fields(input)

	if len(arr) == 0 {
		return "", nil
	}

	command = strings.ToLower(arr[0])

	if len(arr) > 1 {
		params = make([]string, len(arr)-1)
		for i := 1; i < len(arr); i++ {
			params[i-1] = strings.TrimSpace(arr[i])
		}
	}
	return command, params
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type Flags struct {
	TableName   string
	EndpointURL string
}

func init() {
	rootCmd.Flags().StringVar(&flgs.TableName, "table-name", dynamomq.DefaultTableName, "The name of the table to contain the item.")
	rootCmd.Flags().StringVar(&flgs.EndpointURL, "endpoint-url", "", "Override command's default URL with the given URL.")
}

const (
	needAWSMessage = "Need first to run 'aws' command"
)

type Interactive struct {
	TableName string
	Client    dynamomq.Client[any]
	Message   *dynamomq.Message[any]
}

func (c *Interactive) Run(ctx context.Context, command string, params []string) {
	switch command {
	case "h", "?", "help":
		c.help(ctx, params)
	case "qstat", "qstats":
		c.qstat(ctx, params)
	case "dlq":
		c.dlq(ctx, params)
	case "enqueue-test", "et":
		c.enqueueTest(ctx, params)
	case "purge":
		c.purge(ctx, params)
	case "ls":
		c.ls(ctx, params)
	case "receive":
		c.receive(ctx, params)
	case "id":
		c.id(ctx, params)
	case "sys", "system":
		c.system(ctx, params)
	case "data":
		c.data(ctx, params)
	case "info":
		c.info(ctx, params)
	case "reset":
		c.reset(ctx, params)
	case "redrive":
		c.redrive(ctx, params)
	case "delete":
		c.delete(ctx, params)
	case "fail":
		c.fail(ctx, params)
	case "invalid":
		c.invalid(ctx, params)
	default:
		fmt.Println(" ... unrecognized command!")
	}
}

func (c *Interactive) help(_ context.Context, _ []string) {
	fmt.Println(`... this is Interactive HELP!
  > qstat | qstats                                [Retrieves the Queue statistics (no need to be in App mode)]
  > dlq                                           [Retrieves the Dead Letter Queue (DLQ) statistics]
  > enqueue-test | et                             [SendMessage test Message records in DynamoDB: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it]
  > purge                                         [It will remove all test data from DynamoDB]
  > ls                                            [ListMessages all message IDs ... max 10 elements]
  > receive                                       [ReceiveMessage the Message from the Queue .. it will replace the current ID with the peeked one]
  > id <id>                                       [GetMessage the application object from DynamoDB by app domain ID; Interactive is in the app mode, from that point on]
    > sys                                         [Show system info data in a JSON format]
    > data                                        [Print the data as JSON for the current message record]
    > info                                        [Print all info regarding Message record: system_info and data as JSON]
    > reset                                       [Reset the system info of the current message record]
    > redrive                                     [RedriveMessage the record to STANDARD from DLQ]
    > delete                                      [DeleteMessage current ID]
    > fail                                        [Simulate failed record's processing ... put back to the queue; needs to be peeked again]
    > invalid                                     [Remove record from the regular queue to dead letter queue (DLQ) for manual fix]
  > id`)
}

func (c *Interactive) ls(ctx context.Context, _ []string) {
	out, err := c.Client.ListMessages(ctx, &dynamomq.ListMessagesInput{Size: 10})
	if err != nil {
		printError(err)
		return
	}
	if len(out.Messages) == 0 {
		fmt.Println("Queue is empty!")
		return
	}
	fmt.Println("ListMessages of first 10 IDs:")
	for _, m := range out.Messages {
		fmt.Printf("* ID: %s, status: %s", m.ID, m.Status)
	}
}

func (c *Interactive) purge(ctx context.Context, _ []string) {
	out, err := c.Client.ListMessages(ctx, &dynamomq.ListMessagesInput{Size: 10})
	if err != nil {
		printError(err)
		return
	}
	if len(out.Messages) == 0 {
		fmt.Println("Message table is empty ... nothing to remove!")
		return
	}
	fmt.Println("ListMessages of removed IDs:")
	for _, m := range out.Messages {
		_, err := c.Client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
			ID: m.ID,
		})
		if err != nil {
			printError(err)
			continue
		}
		fmt.Printf("* ID: %s\n", m.ID)
	}
}

func (c *Interactive) enqueueTest(ctx context.Context, _ []string) {
	fmt.Println("SendMessage message with IDs:")
	ids := []string{"A-101", "A-202", "A-303", "A-404"}
	for _, id := range ids {
		message := dynamomq.NewDefaultMessage[test.MessageData](id, test.NewMessageData(id), clock.Now())
		item, err := message.MarshalMap()
		if err != nil {
			fmt.Printf("* ID: %s, error: %s\n", id, err)
			continue
		}
		_, err = c.Client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
			ID: id,
		})
		if err != nil {
			fmt.Printf("* ID: %s, error: %s\n", id, err)
			continue
		}
		_, err = c.Client.GetDynamodbClient().PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(c.TableName),
			Item:      item,
		})
		if err != nil {
			fmt.Printf("* ID: %s, error: %s\n", id, err)
			continue
		}
		fmt.Printf("* ID: %s\n", id)
	}
}

func (c *Interactive) qstat(ctx context.Context, _ []string) {
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue status:\n", stats)
}

func (c *Interactive) dlq(ctx context.Context, _ []string) {
	stats, err := c.Client.GetDLQStats(ctx, &dynamomq.GetDLQStatsInput{})
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("DLQ status:\n", stats)
}

func (c *Interactive) receive(ctx context.Context, _ []string) {
	rr, err := c.Client.ReceiveMessage(ctx, &dynamomq.ReceiveMessageInput{})
	if err != nil {
		printError(fmt.Sprintf("ReceiveMessage has failed! message: %s", err))
		return
	}
	c.Message = rr.PeekedMessageObject
	printMessageWithData(
		fmt.Sprintf("ReceiveMessage was successful ... record peeked is: [%s]\n", c.Message.ID),
		c.Message.GetSystemInfo())
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue stats:\n", stats)
}

func (c *Interactive) id(ctx context.Context, params []string) {
	if len(params) == 0 {
		c.Message = nil
		fmt.Println("Going back to standard Interactive mode!")
		return
	}
	id := params[0]
	var err error
	retrieved, err := c.Client.GetMessage(ctx, &dynamomq.GetMessageInput{
		ID: id,
	})
	if err != nil {
		printError(err)
		return
	}
	if retrieved.Message == nil {
		printError(fmt.Sprintf("Message's [%s] not found!", id))
		return
	}
	c.Message = retrieved.Message
	printMessageWithData(fmt.Sprintf("Message's [%s] record dump:\n", id), c.Message)
}

func (c *Interactive) system(_ context.Context, _ []string) {
	if c.Message == nil {
		printCLIModeRestriction("`system` or `sys`")
		return
	}
	printMessageWithData("ID's system info:\n", c.Message.GetSystemInfo())
}

func (c *Interactive) reset(ctx context.Context, _ []string) {
	if c.Message == nil {
		printCLIModeRestriction("`reset`")
		return
	}
	c.Message.ResetSystemInfo(clock.Now())
	_, err := c.Client.ReplaceMessage(ctx, &dynamomq.ReplaceMessageInput[any]{
		Message: c.Message,
	})
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Reset system info:\n", c.Message.GetSystemInfo())
}

func (c *Interactive) redrive(ctx context.Context, _ []string) {
	if c.Message == nil {
		printCLIModeRestriction("`redrive`")
		return
	}
	result, err := c.Client.RedriveMessage(ctx, &dynamomq.RedriveMessageInput{
		ID: c.Message.ID,
	})
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Ready system info:\n", result)
}

func (c *Interactive) delete(ctx context.Context, _ []string) {
	if c.Message == nil {
		printCLIModeRestriction("`done`")
		return
	}
	_, err := c.Client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
		ID: c.Message.ID,
	})

	if err != nil {
		printError(err)
		return
	}
	fmt.Printf("Processing for ID [%s] is deleted successfully! Remove from the queue!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue status:\n", stats)
}

func (c *Interactive) fail(ctx context.Context, _ []string) {
	if c.Message == nil {
		printCLIModeRestriction("`fail`")
		return
	}
	_, err := c.Client.UpdateMessageAsVisible(ctx, &dynamomq.UpdateMessageAsVisibleInput{
		ID: c.Message.ID,
	})
	if err != nil {
		printError(err)
		return
	}
	retrieved, err := c.Client.GetMessage(ctx, &dynamomq.GetMessageInput{ID: c.Message.ID})
	if err != nil {
		printError(err)
		return
	}
	if retrieved.Message == nil {
		printError(fmt.Sprintf("Message's [%s] not found!", c.Message.ID))
		return
	}
	c.Message = retrieved.Message
	fmt.Printf("Processing for ID [%s] has failed! ReplaceMessage the record back to the queue!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue status:\n", stats)
}

func (c *Interactive) invalid(ctx context.Context, _ []string) {
	if c.Message == nil {
		printCLIModeRestriction("`invalid`")
		return
	}
	_, err := c.Client.MoveMessageToDLQ(ctx, &dynamomq.MoveMessageToDLQInput{
		ID: c.Message.ID,
	})
	if err != nil {
		printError(err)
		return
	}
	fmt.Printf("Processing for ID [%s] has failed .. invalid data! Send record to DLQ!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue status:\n", stats)
}

func (c *Interactive) data(_ context.Context, _ []string) {
	if c.Message == nil {
		printCLIModeRestriction("`data`")
		return
	}
	printMessageWithData("Data info:\n", c.Message.Data)
}

func (c *Interactive) info(_ context.Context, _ []string) {
	if c.Message == nil {
		printCLIModeRestriction("`info`")
		return
	}
	printMessageWithData("Record's dump:\n", c.Message)
}

func printMessageWithData(message string, data any) {
	dump, err := marshalIndent(data)
	if err != nil {
		printError(err)
		return
	}
	fmt.Printf("%s%s\n", message, dump)
}

func marshalIndent(v any) ([]byte, error) {
	dump, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return dump, nil
}

func printCLIModeRestriction(command string) {
	printError(fmt.Sprintf("%s command can be only used in the Interactive's App mode. Call first `id <record-id>", command))
}

func printError(err any) {
	fmt.Printf("ERROR: %v\n", err)
}

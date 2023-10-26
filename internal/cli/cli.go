package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vvatanabe/dynamomq"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

const (
	needAWSMessage = "Need first to run 'aws' command"
)

type CLI struct {
	Region             string
	BaseEndpoint       string
	CredentialsProfile string
	TableName          string

	Client  dynamomq.Client[any]
	Message *dynamomq.Message[any]
}

func (c *CLI) Run(ctx context.Context, command string, params []string) {
	switch command {
	case "h", "?", "help":
		c.help(ctx, params)
	case "aws":
		c.aws(ctx, params)
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
	case "peek":
		c.peek(ctx, params)
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

func (c *CLI) help(_ context.Context, _ []string) {
	fmt.Println(`... this is CLI HELP!
  > aws --profile --region --table --endpoint-url [Establish connection with AWS; Default profile name: 'default' and region: 'us-east-1']
  > qstat | qstats                                [Retrieves the Queue statistics (no need to be in App mode)]
  > dlq                                           [Retrieves the Dead Letter Queue (DLQ) statistics]
  > enqueue-test | et                             [Enqueue test Message records in DynamoDB: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it]
  > purge                                         [It will remove all test data from DynamoDB]
  > ls                                            [List all message IDs ... max 10 elements]
  > peek                                          [Peek the Message from the Queue .. it will replace the current ID with the peeked one]
  > id <id>                                       [Get the application object from DynamoDB by app domain ID; CLI is in the app mode, from that point on]
    > sys                                         [Show system info data in a JSON format]
    > data                                        [Print the data as JSON for the current message record]
    > info                                        [Print all info regarding Message record: system_info and data as JSON]
    > reset                                       [Reset the system info of the current message record]
    > redrive                                     [Redrive the record to STANDARD from DLQ]
    > delete                                      [Delete current ID]
    > fail                                        [Simulate failed record's processing ... put back to the queue; needs to be peeked again]
    > invalid                                     [Remove record from the regular queue to dead letter queue (DLQ) for manual fix]
  > id`)
}

func (c *CLI) aws(ctx context.Context, params []string) {
	if len(params) == 0 {
		printError("aws --profile --region --table --endpoint-url [Establish connection with AWS; Default profile: 'default' and region: 'us-east-1']")
		return
	}
	profile, region, table, endpoint := parseParams(params)
	if region != "" {
		c.Region = region
	}
	if table != "" {
		c.TableName = table
	}
	if profile != "" {
		c.CredentialsProfile = profile
	}
	if endpoint != "" {
		c.BaseEndpoint = endpoint
	}
	client, err := dynamomq.NewFromConfig[any](ctx,
		dynamomq.WithAWSRegion(c.Region),
		dynamomq.WithAWSCredentialsProfileName(profile),
		dynamomq.WithTableName(c.TableName),
		dynamomq.WithAWSBaseEndpoint(c.BaseEndpoint))
	if err != nil {
		fmt.Printf(" ... AWS session could not be established!: %v\n", err)
	} else {
		c.Client = client
		fmt.Println(" ... AWS session is properly established!")
	}
}

func parseParams(params []string) (profile, region, table, endpoint string) {
	// Map to store parsed values
	for i := 0; i < len(params)-1; i++ {
		switch params[i] {
		case "--profile", "-profile":
			profile = params[i+1]
		case "--region", "-region":
			region = params[i+1]
		case "--table", "-table":
			table = params[i+1]
		case "--endpoint-url", "-endpoint-url":
			endpoint = params[i+1]
		}
	}
	return
}

func (c *CLI) ls(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	ids, err := c.Client.ListExtendedIDs(ctx, 10)
	if err != nil {
		printError(err)
		return
	}
	if len(ids) == 0 {
		fmt.Println("Message table is empty!")
		return
	}
	fmt.Println("List of first 10 IDs:")
	for _, id := range ids {
		fmt.Printf("* %s\n", id)
	}
}

func (c *CLI) purge(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	ids, err := c.Client.ListIDs(ctx, 10)
	if err != nil {
		printError(err)
		return
	}
	if len(ids) == 0 {
		fmt.Println("Message table is empty ... nothing to remove!")
		return
	}
	fmt.Println("List of removed IDs:")
	for _, id := range ids {
		err := c.Client.Delete(ctx, id)
		if err != nil {
			printError(err)
			continue
		}
		fmt.Printf("* ID: %s\n", id)
	}
}

func (c *CLI) enqueueTest(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	fmt.Println("Enqueue message with IDs:")
	ids := []string{"A-101", "A-202", "A-303", "A-404"}
	for _, id := range ids {
		message := dynamomq.NewDefaultMessage[test.MessageData](id, test.NewMessageData(id), clock.Now())
		item, err := message.MarshalMap()
		if err != nil {
			fmt.Printf("* ID: %s, error: %s\n", id, err)
			continue
		}
		err = c.Client.Delete(ctx, id)
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

func (c *CLI) qstat(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	stats, err := c.Client.GetQueueStats(ctx)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue status:\n", stats)
}

func (c *CLI) dlq(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	stats, err := c.Client.GetDLQStats(ctx)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("DLQ status:\n", stats)
}

func (c *CLI) peek(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	rr, err := c.Client.Peek(ctx)
	if err != nil {
		printError(fmt.Sprintf("Peek has failed! message: %s", err))
		return
	}
	c.Message = rr.PeekedMessageObject
	printMessageWithData(
		fmt.Sprintf("Peek was successful ... record peeked is: [%s]\n", c.Message.ID),
		c.Message.GetSystemInfo())
	stats, err := c.Client.GetQueueStats(ctx)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue stats:\n", stats)
}

func (c *CLI) id(ctx context.Context, params []string) {
	if len(params) == 0 {
		c.Message = nil
		fmt.Println("Going back to standard CLI mode!")
		return
	}
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	id := params[0]
	var err error
	c.Message, err = c.Client.Get(ctx, id)
	if err != nil {
		printError(err)
		return
	}
	if c.Message == nil {
		printError(fmt.Sprintf("Message's [%s] not found!", id))
		return
	}
	printMessageWithData(fmt.Sprintf("Message's [%s] record dump:\n", id), c.Message)
}

func (c *CLI) system(_ context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Message == nil {
		printCLIModeRestriction("`system` or `sys`")
		return
	}
	printMessageWithData("ID's system info:\n", c.Message.GetSystemInfo())
}

func (c *CLI) reset(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Message == nil {
		printCLIModeRestriction("`reset`")
		return
	}
	c.Message.ResetSystemInfo(clock.Now())
	err := c.Client.Put(ctx, c.Message)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Reset system info:\n", c.Message.GetSystemInfo())
}

func (c *CLI) redrive(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Message == nil {
		printCLIModeRestriction("`redrive`")
		return
	}
	result, err := c.Client.Redrive(ctx, c.Message.ID)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Ready system info:\n", result)
}

func (c *CLI) delete(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Message == nil {
		printCLIModeRestriction("`done`")
		return
	}
	err := c.Client.Delete(ctx, c.Message.ID)
	if err != nil {
		printError(err)
		return
	}
	fmt.Printf("Processing for ID [%s] is deleted successfully! Remove from the queue!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue status:\n", stats)
}

func (c *CLI) fail(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Message == nil {
		printCLIModeRestriction("`fail`")
		return
	}
	_, err := c.Client.Retry(ctx, c.Message.ID)
	if err != nil {
		printError(err)
		return
	}
	c.Message, err = c.Client.Get(ctx, c.Message.ID)
	if err != nil {
		printError(err)
		return
	}
	if c.Message == nil {
		printError(fmt.Sprintf("Message's [%s] not found!", c.Message.ID))
		return
	}
	fmt.Printf("Processing for ID [%s] has failed! Put the record back to the queue!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue status:\n", stats)
}

func (c *CLI) invalid(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Message == nil {
		printCLIModeRestriction("`invalid`")
		return
	}
	_, err := c.Client.SendToDLQ(ctx, c.Message.ID)
	if err != nil {
		printError(err)
		return
	}
	fmt.Printf("Processing for ID [%s] has failed .. invalid data! Send record to DLQ!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue status:\n", stats)
}

func (c *CLI) data(_ context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Message == nil {
		printCLIModeRestriction("`data`")
		return
	}
	printMessageWithData("Data info:\n", c.Message.Data)
}

func (c *CLI) info(_ context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
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
	printError(fmt.Sprintf("%s command can be only used in the CLI's App mode. Call first `id <record-id>", command))
}

func printError(err any) {
	fmt.Printf("ERROR: %v\n", err)
}

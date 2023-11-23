package cmd

import (
	"context"
	"fmt"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
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
  > qstat | qstats                                [Retrieves the queue statistics]
  > dlq                                           [Retrieves the Dead Letter Queue (DLQ) statistics]
  > enqueue-test | et                             [Send test messages in DynamoDB table: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it]
  > purge                                         [It will remove all message from DynamoMQ table]
  > ls                                            [List all message IDs ... max 10 elements]
  > receive                                       [Receive a message from the queue .. it will replace the current ID with the peeked one]
  > id <id>                                       [Get a message the application object from DynamoDB by app domain ID; Interactive is in the app mode, from that point on]
    > sys                                         [Show system info data in a JSON format]
    > data                                        [Print the data as JSON for the current message record]
    > info                                        [Print all info regarding Message record: system_info and data as JSON]
    > reset                                       [Reset the system info of the message]
    > redrive                                     [Redrive a message to STANDARD from DLQ]
    > delete                                      [Delete a message by ID]
    > fail                                        [Simulate failed message's processing ... put back to the queue; needs to be receive again]
    > invalid                                     [Remove a message from the standard queue to dead letter queue (DLQ) for manual fix]
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
	fmt.Println("List messages of first 10 IDs:")
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
	fmt.Println("List messages of removed IDs:")
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
	fmt.Println("Send a message with IDs:")
	ids := []string{"A-101", "A-202", "A-303", "A-404"}
	for _, id := range ids {
		_, err := c.Client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
			ID: id,
		})
		if err != nil {
			printErrorWithID(err, id)
			continue
		}
		_, err = c.Client.SendMessage(ctx, &dynamomq.SendMessageInput[any]{
			ID:   id,
			Data: test.NewMessageData(id),
		})
		if err != nil {
			printErrorWithID(err, id)
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
	printQueueStatus(stats)
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
	printQueueStatus(stats)
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
	printQueueStatus(stats)
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
	printQueueStatus(stats)
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

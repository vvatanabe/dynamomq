package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/vvatanabe/dynamomq"
	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/internal/test"
)

type Interactive struct {
	Client  dynamomq.Client[any]
	Message *dynamomq.Message[any]
}

func (c *Interactive) Run(ctx context.Context, command string, params []string) (err error) {
	switch command {
	case "h", "?", "help":
		err = c.help(ctx, params)
	case "qstat", "qstats":
		err = c.qstat(ctx, params)
	case "dlq":
		err = c.dlq(ctx, params)
	case "enqueue-test", "et":
		err = c.enqueueTest(ctx, params)
	case "purge":
		err = c.purge(ctx, params)
	case "ls":
		err = c.ls(ctx, params)
	case "receive":
		err = c.receive(ctx, params)
	case "id":
		err = c.id(ctx, params)
	case "sys", "system":
		err = c.system(ctx, params)
	case "data":
		err = c.data(ctx, params)
	case "info":
		err = c.info(ctx, params)
	case "reset":
		err = c.reset(ctx, params)
	case "redrive":
		err = c.redrive(ctx, params)
	case "delete":
		err = c.delete(ctx, params)
	case "fail":
		err = c.fail(ctx, params)
	case "invalid":
		err = c.invalid(ctx, params)
	default:
		err = errors.New(" ... unrecognized command!")
	}
	return
}

func (c *Interactive) help(_ context.Context, _ []string) error {
	fmt.Println(`... this is Interactive HELP!
  > qstat                                         [Retrieves the queue statistics]
  > dlq                                           [Retrieves the Dead Letter Queue (DLQ) statistics]
  > enqueue-test                                  [Send test messages in DynamoDB table: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it]
  > purge                                         [It will remove all message from DynamoMQ table]
  > ls                                            [List all message IDs ... max 10 elements]
  > receive                                       [Receive a message from the queue .. it will replace the current ID with the peeked one]
  > id <id>                                       [Get a message the application object from DynamoDB by app domain ID; Interactive is in the app mode, from that point on]
    > system                                      [Show system info data in a JSON format]
    > data                                        [Print the data as JSON for the current message record]
    > info                                        [Print all info regarding Message record: system_info and data as JSON]
    > reset                                       [Reset the system info of the message]
    > redrive                                     [Redrive a message to STANDARD from DLQ]
    > delete                                      [Delete a message by ID]
    > fail                                        [Simulate failed message's processing ... put back to the queue; needs to be receive again]
    > invalid                                     [Remove a message from the standard queue to dead letter queue (DLQ) for manual fix]
  > id`)
	return nil
}

func (c *Interactive) info(_ context.Context, _ []string) error {
	if c.Message == nil {
		return errorCLIModeRestriction("`info`")
	}
	printMessageWithData("Record's dump:\n", c.Message)
	return nil
}

func (c *Interactive) data(_ context.Context, _ []string) error {
	if c.Message == nil {
		return errorCLIModeRestriction("`data`")
	}
	printMessageWithData("Data info:\n", c.Message.Data)
	return nil
}

func (c *Interactive) system(_ context.Context, _ []string) error {
	if c.Message == nil {
		return errorCLIModeRestriction("`system`")
	}
	printMessageWithData("ID's system info:\n", c.Message.GetSystemInfo())
	return nil
}

func (c *Interactive) ls(ctx context.Context, _ []string) error {
	out, err := c.Client.ListMessages(ctx, &dynamomq.ListMessagesInput{Size: 10})
	if err != nil {
		return err
	}
	if len(out.Messages) == 0 {
		fmt.Println("Queue is empty!")
		return nil
	}
	fmt.Println("List messages of first 10 IDs:")
	for _, m := range out.Messages {
		fmt.Printf("* ID: %s, status: %s", m.ID, m.Status)
	}
	return nil
}

func (c *Interactive) purge(ctx context.Context, _ []string) error {
	out, err := c.Client.ListMessages(ctx, &dynamomq.ListMessagesInput{Size: 10})
	if err != nil {
		return err
	}
	if len(out.Messages) == 0 {
		fmt.Println("Message table is empty ... nothing to remove!")
		return nil
	}
	fmt.Println("List messages of removed IDs:")
	for _, m := range out.Messages {
		_, err := c.Client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
			ID: m.ID,
		})
		if err != nil {
			return errorWithID(err, m.ID)
		}
		fmt.Printf("* ID: %s\n", m.ID)
	}
	return nil
}

func (c *Interactive) enqueueTest(ctx context.Context, _ []string) error {
	fmt.Println("Send a message with IDs:")
	ids := []string{"A-101", "A-202", "A-303", "A-404"}
	for _, id := range ids {
		_, err := c.Client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
			ID: id,
		})
		if err != nil {
			return errorWithID(err, id)
		}
		_, err = c.Client.SendMessage(ctx, &dynamomq.SendMessageInput[any]{
			ID:   id,
			Data: test.NewMessageData(id),
		})
		if err != nil {
			return errorWithID(err, id)
		}
		fmt.Printf("* ID: %s\n", id)
	}
	return nil
}

func (c *Interactive) qstat(ctx context.Context, _ []string) error {
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		return err
	}
	printQueueStatus(stats)
	return nil
}

func (c *Interactive) dlq(ctx context.Context, _ []string) error {
	stats, err := c.Client.GetDLQStats(ctx, &dynamomq.GetDLQStatsInput{})
	if err != nil {
		return err
	}
	printMessageWithData("DLQ status:\n", stats)
	return nil
}

func (c *Interactive) receive(ctx context.Context, _ []string) error {
	rr, err := c.Client.ReceiveMessage(ctx, &dynamomq.ReceiveMessageInput{})
	if err != nil {
		return err
	}
	c.Message = rr.PeekedMessageObject
	printMessageWithData(
		fmt.Sprintf("ReceiveMessage was successful ... record peeked is: [%s]\n", c.Message.ID),
		c.Message.GetSystemInfo())
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		return err
	}
	printMessageWithData("Queue stats:\n", stats)
	return nil
}

func (c *Interactive) id(ctx context.Context, params []string) error {
	if len(params) == 0 {
		c.Message = nil
		fmt.Println("Going back to standard Interactive mode!")
		return nil
	}
	id := params[0]
	retrieved, err := c.Client.GetMessage(ctx, &dynamomq.GetMessageInput{
		ID: id,
	})
	if err != nil {
		return err
	}
	if retrieved.Message == nil {
		return errorWithID(errors.New("message not found"), id)
	}
	c.Message = retrieved.Message
	printMessageWithData(fmt.Sprintf("Message's [%s] record dump:\n", id), c.Message)
	return nil
}

func (c *Interactive) reset(ctx context.Context, _ []string) error {
	if c.Message == nil {
		return errorCLIModeRestriction("`reset`")
	}
	c.Message.ResetSystemInfo(clock.Now())
	_, err := c.Client.ReplaceMessage(ctx, &dynamomq.ReplaceMessageInput[any]{
		Message: c.Message,
	})
	if err != nil {
		return err
	}
	printMessageWithData("Reset system info:\n", c.Message.GetSystemInfo())
	return nil
}

func (c *Interactive) redrive(ctx context.Context, _ []string) error {
	if c.Message == nil {
		return errorCLIModeRestriction("`redrive`")
	}
	result, err := c.Client.RedriveMessage(ctx, &dynamomq.RedriveMessageInput{
		ID: c.Message.ID,
	})
	if err != nil {
		return err
	}
	printMessageWithData("Ready system info:\n", result)
	return nil
}

func (c *Interactive) delete(ctx context.Context, _ []string) error {
	if c.Message == nil {
		return errorCLIModeRestriction("`delete`")
	}
	_, err := c.Client.DeleteMessage(ctx, &dynamomq.DeleteMessageInput{
		ID: c.Message.ID,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Processing for ID [%s] is deleted successfully! Remove from the queue!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		return err
	}
	printQueueStatus(stats)
	return nil
}

func (c *Interactive) fail(ctx context.Context, _ []string) error {
	if c.Message == nil {
		return errorCLIModeRestriction("`fail`")
	}
	_, err := c.Client.UpdateMessageAsVisible(ctx, &dynamomq.UpdateMessageAsVisibleInput{
		ID: c.Message.ID,
	})
	if err != nil {
		return err
	}
	retrieved, err := c.Client.GetMessage(ctx, &dynamomq.GetMessageInput{ID: c.Message.ID})
	if err != nil {
		return err
	}
	if retrieved.Message == nil {
		return errorWithID(errors.New("message not found"), c.Message.ID)
	}
	c.Message = retrieved.Message
	fmt.Printf("Processing for ID [%s] has failed! ReplaceMessage the record back to the queue!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		return err
	}
	printQueueStatus(stats)
	return nil
}

func (c *Interactive) invalid(ctx context.Context, _ []string) error {
	if c.Message == nil {
		return errorCLIModeRestriction("`invalid`")
	}
	_, err := c.Client.MoveMessageToDLQ(ctx, &dynamomq.MoveMessageToDLQInput{
		ID: c.Message.ID,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Processing for ID [%s] has failed .. invalid data! Send record to DLQ!\n", c.Message.ID)
	stats, err := c.Client.GetQueueStats(ctx, &dynamomq.GetQueueStatsInput{})
	if err != nil {
		return err
	}
	printQueueStatus(stats)
	return nil
}

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vvatanabe/go82f46979/model"
	"github.com/vvatanabe/go82f46979/sdk"
)

const (
	needAWSMessage = "Need first to run 'aws' command"
)

type CLI struct {
	Region             *string
	CredentialsProfile *string
	TableName          *string

	Client   *sdk.QueueSDKClient
	Shipment *model.Shipment
}

func (c *CLI) Run(ctx context.Context, command string, params []string) {
	switch command {
	case "h", "?", "help":
		fmt.Println(`... this is CLI HELP!
  > aws <profile> [<region>]                      [Establish connection with AWS; Default profile name: 'default' and region: 'us-east-1']
  > qstat | qstats                                [Retrieves the Queue statistics (no need to be in App mode)]
  > dlq                                           [Retrieves the Dead Letter Queue (DLQ) statistics]
  > create-test | ct                              [Create test Shipment records in DynamoDB: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it]
  > purge                                         [It will remove all test data from DynamoDB]
  > ls                                            [List all shipment IDs ... max 10 elements]
  > id <id>                                       [Get the application object from DynamoDB by app domain ID; CLI is in the app mode, from that point on]
    > sys                                         [Show system info data in a JSON format]
    > data                                        [Print the data as JSON for the current shipment record]
    > info                                        [Print all info regarding Shipment record: system_info and data as JSON]
    > update <new Shipment status>                [Update Shipment status .. e.g.: from UNDER_CONSTRUCTION to READY_TO_SHIP]
    > reset                                       [Reset the system info of the current shipment record]
    > ready                                       [Make the record ready for the shipment]
    > enqueue | en                                [Enqueue current ID]
    > peek                                        [Peek the Shipment from the Queue .. it will replace the current ID with the peeked one]
    > done                                        [Simulate successful record processing completion ... remove from the queue]
    > fail                                        [Simulate failed record's processing ... put back to the queue; needs to be peeked again]
    > invalid                                     [Remove record from the regular queue to dead letter queue (DLQ) for manual fix]
  > id`)
	case "aws":
		c.aws(ctx, params)
	case "id":
		c.id(ctx, params)
	case "sys", "system":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("`system` or `sys` command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		printMessageWithData("ID's system info:\n", c.Shipment.SystemInfo)
	case "ls":
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
			fmt.Println("Shipment table is empty!")
			return
		}
		fmt.Println("List of first 10 IDs:")
		for _, id := range ids {
			fmt.Printf("* %s\n", id)
		}
	case "purge":
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
			fmt.Println("Shipment table is empty ... nothing to remove!")
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
	case "create-test", "ct":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		fmt.Println("Creating shipment with IDs:")
		ids := []string{"A-101", "A-202", "A-303", "A-404"}
		for _, id := range ids {
			_, err := c.Client.CreateTestData(ctx, id)
			if err != nil {
				fmt.Printf("* ID: %s, error: %s\n", id, err)
			} else {
				fmt.Printf("* ID: %s\n", id)
			}
		}
	case "qstat", "qstats":
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
	case "dlq":
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
	case "reset":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'reset' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		c.Shipment.ResetSystemInfo()
		err := c.Client.Put(ctx, c.Shipment)
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("Reset system info:\n", c.Shipment.SystemInfo)
	case "ready":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'ready' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		c.Shipment.ResetSystemInfo()
		c.Shipment.SystemInfo.Status = model.StatusReadyToShip
		err := c.Client.Put(ctx, c.Shipment)
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("Ready system info:\n", c.Shipment.SystemInfo)
	case "done":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'done' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		rr, err := c.Client.UpdateStatus(ctx, c.Shipment.ID, model.StatusCompleted)
		if err != nil {
			printError(err)
			return
		}
		if !rr.IsSuccessful() {
			printError(rr.GetErrorMessage())
			return
		}
		rr, err = c.Client.Remove(ctx, c.Shipment.ID)
		if err != nil {
			printError(err)
			return
		}
		if !rr.IsSuccessful() {
			printError(rr.GetErrorMessage())
			return
		}
		shipment, err := c.Client.Get(ctx, c.Shipment.ID)
		if err != nil {
			printError(err)
			return
		}
		if shipment == nil {
			printError(fmt.Sprintf("Shipment's [%s] not found!", shipment.ID))
			return
		}
		fmt.Printf("Processing for ID [%s] is completed successfully! Remove from the queue!\n", shipment.ID)
		stats, err := c.Client.GetQueueStats(ctx)
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("Queue status:\n", stats)
	case "fail":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'fail' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		rr, err := c.Client.Restore(ctx, c.Shipment.ID)
		if err != nil {
			printError(err)
			return
		}
		if !rr.IsSuccessful() {
			printError(rr.GetErrorMessage())
			return
		}
		c.Shipment, err = c.Client.Get(ctx, c.Shipment.ID)
		if err != nil {
			printError(err)
			return
		}
		if c.Shipment == nil {
			printError(fmt.Sprintf("Shipment's [%s] not found!", c.Shipment.ID))
			return
		}
		fmt.Printf("Processing for ID [%s] has failed! Put the record back to the queue!\n", c.Shipment.ID)
		stats, err := c.Client.GetQueueStats(ctx)
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("Queue status:\n", stats)
	case "invalid":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'invalid' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		rr, err := c.Client.SendToDLQ(ctx, c.Shipment.ID)
		if err != nil {
			printError(err)
			return
		}
		if !rr.IsSuccessful() {
			printError(rr.GetErrorMessage())
			return
		}
		fmt.Printf("Processing for ID [%s] has failed .. invalid data! Send record to DLQ!\n", c.Shipment.ID)
		stats, err := c.Client.GetQueueStats(ctx)
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("Queue status:\n", stats)
	case "data":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'data' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		printMessageWithData("Data info:\n", c.Shipment.Data)
	case "info":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'info' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		printMessageWithData("Record's dump:\n", c.Shipment)
	case "enqueue", "en":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'enqueue' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		shipment, err := c.Client.Get(ctx, c.Shipment.ID)
		if err != nil {
			printError(err)
			return
		}
		if shipment == nil {
			printError(fmt.Sprintf("Shipment's [%s] not found!", shipment.ID))
			return
		}
		// convert under_construction to ready to ship
		if shipment.SystemInfo.Status == model.StatusUnderConstruction {
			shipment.ResetSystemInfo()
			shipment.SystemInfo.Status = model.StatusReadyToShip
			err = c.Client.Put(ctx, shipment)
			if err != nil {
				printError(err)
				return
			}
		}
		rr, err := c.Client.Enqueue(ctx, shipment.ID)
		if err != nil {
			printError(err)
			return
		}
		shipment = rr.Shipment
		if rr.IsSuccessful() {
			printMessageWithData("Record's system info:\n", shipment.SystemInfo)
			stats, err := c.Client.GetQueueStats(ctx)
			if err != nil {
				printError(err)
				return
			}
			printMessageWithData("Queue stats:\n", stats)
		} else {
			printError(fmt.Sprintf("Enqueue has failed! message: %s", rr.GetErrorMessage()))
		}
	case "peek":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'peek' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		rr, err := c.Client.Peek(ctx)
		if err != nil {
			printError(err)
			return
		}
		if rr.IsSuccessful() {
			c.Shipment = rr.PeekedShipmentObject
			printMessageWithData(
				fmt.Sprintf("Peek was successful ... record peeked is: [%s]\n", c.Shipment.ID),
				c.Shipment.SystemInfo)
			stats, err := c.Client.GetQueueStats(ctx)
			if err != nil {
				printError(err)
				return
			}
			printMessageWithData("Queue stats:\n", stats)
		} else {
			printError(fmt.Sprintf("Peek has failed! message: %s", rr.GetErrorMessage()))
		}
	case "update":
		if c.Client == nil {
			fmt.Println(needAWSMessage)
			return
		}
		if c.Shipment == nil {
			printError("'update <status>' command can be only used in the CLI's App mode. Call first `id <record-id>`")
			return
		}
		if params == nil {
			printError("'update <status>' command requires a new Status parameter to be specified!")
			return
		}
		statusStr := strings.TrimSpace(strings.ToUpper(params[0]))
		if statusStr == string(model.StatusReadyToShip) {
			c.Shipment.MarkAsReadyForShipment()
			rr, err := c.Client.UpdateStatus(ctx, c.Shipment.ID, model.StatusReadyToShip)
			if err != nil {
				printError(err)
				return
			}
			if !rr.IsSuccessful() {
				printError(rr.GetErrorMessage())
				return
			}
			printMessageWithData("Status changed result:\n", rr)
		} else {
			fmt.Printf("Status change [%s] is not applied!\n", strings.TrimSpace(params[0]))
		}
	default:
		fmt.Println(" ... unrecognized command!")
	}
}

func (c *CLI) aws(ctx context.Context, params []string) {
	if params == nil {
		fmt.Println("ERROR: 'aws <profile> [<region>] [<table>]' command requires parameter(s) to be specified!")
		return
	}
	awsCredentialsProfile := strings.TrimSpace(params[0])
	// specify AWS Region
	if len(params) > 1 {
		temp := strings.TrimSpace(params[1])
		c.Region = &temp
	}
	// specify DynamoDB table name
	if len(params) > 2 {
		temp := strings.TrimSpace(params[2])
		c.TableName = &temp
	}
	if awsCredentialsProfile == "" && (c.CredentialsProfile != nil || *c.CredentialsProfile != "") {
		awsCredentialsProfile = *c.CredentialsProfile
	} else {
		awsCredentialsProfile = "default"
	}
	client, err := sdk.NewBuilder().
		WithRegion(*c.Region).
		WithCredentialsProfileName(awsCredentialsProfile).
		WithTableName(*c.TableName).
		Build(ctx)
	if err != nil {
		fmt.Printf(" ... AWS session could not be established!: %v\n", err)
	} else {
		c.Client = client
		fmt.Println(" ... AWS session is properly established!")
	}
}

func (c *CLI) id(ctx context.Context, params []string) {
	if params == nil || len(params) == 0 {
		c.Shipment = nil
		fmt.Println("Going back to standard CLI mode!")
		return
	}
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	id := params[0]
	var err error
	c.Shipment, err = c.Client.Get(ctx, id)
	if err != nil {
		printError(err)
		return
	}
	if c.Shipment == nil {
		printError(fmt.Sprintf("Shipment's [%s] not found!", id))
		return
	}
	printMessageWithData(fmt.Sprintf("Shipment's [%s] record dump:\n", id), c.Shipment)
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

func printError(err any) {
	fmt.Printf("ERROR: %v\n", err)
}

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vvatanabe/dynamomq/internal/clock"
	"github.com/vvatanabe/dynamomq/sdk"
)

const (
	needAWSMessage = "Need first to run 'aws' command"
)

type CLI struct {
	Region             string
	BaseEndpoint       string
	CredentialsProfile string
	TableName          string

	Client   sdk.QueueSDKClient
	Shipment *sdk.Shipment
}

func (c *CLI) Run(ctx context.Context, command string, params []string) {
	switch command {
	case "h", "?", "help":
		c.help(ctx, params)
	case "aws":
		c.aws(ctx, params)
	case "ls":
		c.ls(ctx, params)
	case "purge":
		c.purge(ctx, params)
	case "create-test", "ct":
		c.createTest(ctx, params)
	case "qstat", "qstats":
		c.qstat(ctx, params)
	case "dlq":
		c.dlq(ctx, params)
	case "peek":
		c.peek(ctx, params)
	case "id":
		c.id(ctx, params)
	case "sys", "system":
		c.system(ctx, params)
	case "reset":
		c.reset(ctx, params)
	case "ready":
		c.ready(ctx, params)
	case "done":
		c.done(ctx, params)
	case "fail":
		c.fail(ctx, params)
	case "invalid":
		c.invalid(ctx, params)
	case "data":
		c.data(ctx, params)
	case "info":
		c.info(ctx, params)
	case "enqueue", "en":
		c.enqueue(ctx, params)
	case "update":
		c.update(ctx, params)
	default:
		fmt.Println(" ... unrecognized command!")
	}
}

func (c *CLI) help(_ context.Context, _ []string) {
	fmt.Println(`... this is CLI HELP!
  > aws --profile --region --table --endpoint-url [Establish connection with AWS; Default profile name: 'default' and region: 'us-east-1']
  > qstat | qstats                                [Retrieves the Queue statistics (no need to be in App mode)]
  > dlq                                           [Retrieves the Dead Letter Queue (DLQ) statistics]
  > create-test | ct                              [Create test Shipment records in DynamoDB: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it]
  > purge                                         [It will remove all test data from DynamoDB]
  > ls                                            [List all shipment IDs ... max 10 elements]
  > peek                                          [Peek the Shipment from the Queue .. it will replace the current ID with the peeked one]
  > id <id>                                       [Get the application object from DynamoDB by app domain ID; CLI is in the app mode, from that point on]
    > sys                                         [Show system info data in a JSON format]
    > data                                        [Print the data as JSON for the current shipment record]
    > info                                        [Print all info regarding Shipment record: system_info and data as JSON]
    > update <new Shipment status>                [Update Shipment status .. e.g.: from UNDER_CONSTRUCTION to READY_TO_SHIP]
    > reset                                       [Reset the system info of the current shipment record]
    > ready                                       [Make the record ready for the shipment]
    > enqueue | en                                [Enqueue current ID]
    > done                                        [Simulate successful record processing completion ... remove from the queue]
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
	client, err := sdk.NewQueueSDKClient(ctx,
		sdk.WithAWSRegion(c.Region),
		sdk.WithAWSCredentialsProfileName(profile),
		sdk.WithTableName(c.TableName),
		sdk.WithAWSBaseEndpoint(c.BaseEndpoint))
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
		fmt.Println("Shipment table is empty!")
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
}

func (c *CLI) createTest(ctx context.Context, _ []string) {
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
}

func (c *CLI) id(ctx context.Context, params []string) {
	if len(params) == 0 {
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

func (c *CLI) system(_ context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`system` or `sys`")
		return
	}
	printMessageWithData("ID's system info:\n", c.Shipment.SystemInfo)
}

func (c *CLI) reset(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`reset`")
		return
	}
	c.Shipment.ResetSystemInfo(clock.Now())
	err := c.Client.Put(ctx, c.Shipment)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Reset system info:\n", c.Shipment.SystemInfo)
}

func (c *CLI) ready(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`ready`")
		return
	}
	now := clock.Now()
	c.Shipment.ResetSystemInfo(now)
	c.Shipment.MarkAsReadyForShipment(now)
	err := c.Client.Put(ctx, c.Shipment)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Ready system info:\n", c.Shipment.SystemInfo)
}

func (c *CLI) done(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`done`")
		return
	}
	_, err := c.Client.UpdateStatus(ctx, c.Shipment.ID, sdk.StatusCompleted)
	if err != nil {
		printError(err)
		return
	}
	_, err = c.Client.Remove(ctx, c.Shipment.ID)
	if err != nil {
		printError(err)
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
}

func (c *CLI) fail(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`fail`")
		return
	}
	_, err := c.Client.Restore(ctx, c.Shipment.ID)
	if err != nil {
		printError(err)
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
}

func (c *CLI) invalid(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`invalid`")
		return
	}
	_, err := c.Client.SendToDLQ(ctx, c.Shipment.ID)
	if err != nil {
		printError(err)
		return
	}
	fmt.Printf("Processing for ID [%s] has failed .. invalid data! Send record to DLQ!\n", c.Shipment.ID)
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
	if c.Shipment == nil {
		printCLIModeRestriction("`data`")
		return
	}
	printMessageWithData("Data info:\n", c.Shipment.Data)
}

func (c *CLI) info(_ context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`info`")
		return
	}
	printMessageWithData("Record's dump:\n", c.Shipment)
}

func (c *CLI) enqueue(ctx context.Context, _ []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`enqueue`")
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
	if shipment.SystemInfo.Status == sdk.StatusPending {
		shipment.ResetSystemInfo(clock.Now())
		shipment.SystemInfo.Status = sdk.StatusReady
		err = c.Client.Put(ctx, shipment)
		if err != nil {
			printError(err)
			return
		}
	}
	rr, err := c.Client.Enqueue(ctx, shipment.ID)
	if err != nil {
		printError(fmt.Sprintf("Enqueue has failed! message: %s", err))
		return
	}
	printMessageWithData("Record's system info:\n", rr.Shipment.SystemInfo)
	stats, err := c.Client.GetQueueStats(ctx)
	if err != nil {
		printError(err)
		return
	}
	printMessageWithData("Queue stats:\n", stats)
}

func (c *CLI) update(ctx context.Context, params []string) {
	if c.Client == nil {
		fmt.Println(needAWSMessage)
		return
	}
	if c.Shipment == nil {
		printCLIModeRestriction("`update <status>`")
		return
	}
	if params == nil {
		printError("'update <status>' command requires a new Status parameter to be specified!")
		return
	}
	statusStr := strings.TrimSpace(strings.ToUpper(params[0]))
	if statusStr == string(sdk.StatusReady) {
		c.Shipment.MarkAsReadyForShipment(clock.Now())
		rr, err := c.Client.UpdateStatus(ctx, c.Shipment.ID, sdk.StatusReady)
		if err != nil {
			printError(err)
			return
		}
		printMessageWithData("Status changed result:\n", rr)
	} else {
		fmt.Printf("Status change [%s] is not applied!\n", strings.TrimSpace(params[0]))
	}
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

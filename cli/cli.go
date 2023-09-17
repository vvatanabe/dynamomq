package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/vvatanabe/go82f46979/constant"
	"github.com/vvatanabe/go82f46979/model"
	"github.com/vvatanabe/go82f46979/sdk"
)

const (
	needAWSMessage = "Need first to run 'aws' command"
)

func Run() {

	fmt.Println("===========================================================")
	fmt.Println(">> Welcome to Priority Queueing CLI Tool!")
	fmt.Println("===========================================================")
	fmt.Println("for help, enter one of the following: ? or h or help")
	fmt.Println("all commands in CLIs need to be typed in lowercase")
	fmt.Println("")

	executionPath, _ := os.Getwd()
	fmt.Printf("current directory is: [%s]\n", executionPath)

	region := flag.String("region", constant.AwsRegionDefault, "AWS region")
	credentialsProfile := flag.String("profile", constant.AwsProfileDefault, "AWS credentials profile")
	tableName := flag.String("table", constant.DefaultTableName, "AWS DynamoDB table name")

	flag.Parse()

	fmt.Printf("profile is: [%s]\n", *credentialsProfile)
	fmt.Printf("region is: [%s]\n", *region)
	fmt.Printf("table is: [%s]\n", *tableName)
	fmt.Println("")

	client, err := sdk.NewBuilder().
		WithRegion(*region).
		WithCredentialsProfileName(*credentialsProfile).
		WithTableName(*tableName).
		Build(context.Background())
	if err != nil {
		fmt.Printf("... AWS session could not be established!: %v\n", err)
	} else {
		fmt.Println("... AWS session is properly established!")
	}

	// 1. Create a Scanner using the InputStream available.
	scanner := bufio.NewScanner(os.Stdin)

	var shipment *model.Shipment

	for {

		// 2. Don't forget to prompt the user
		if shipment != nil {
			fmt.Printf("\nID <%s> >> Enter command: ", shipment.ID)
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

		input = strings.TrimSpace(input)
		arr := strings.Split(input, " ")
		if len(arr) == 0 {
			continue
		}

		command := strings.ToLower(arr[0])
		var params []string = nil
		if len(arr) > 1 {
			params = make([]string, len(arr)-1)
			for i := 1; i < len(arr); i++ {
				params[i-1] = strings.TrimSpace(arr[i])
			}
		}

		// 4. Now, you can do anything with the input string that you need to.
		// Like, output it to the user.

		switch command {
		case "quit", "q":
			goto EndLoop
		case "h", "?", "help":
			fmt.Println("  ... this is CLI HELP!")
			fmt.Println("    > aws <profile> [<region>]                      [Establish connection with AWS; Default profile name: `default` and region: `us-east-1`]")
			fmt.Println("    > qstat | qstats                                [Retrieves the Queue statistics (no need to be in App mode)]")
			fmt.Println("    > dlq                                           [Retrieves the Dead Letter Queue (DLQ) statistics]")
			fmt.Println("    > create-test | ct                              [Create test Shipment records in DynamoDB: A-101, A-202, A-303 and A-404; if already exists, it will overwrite it]")
			fmt.Println("    > purge                                         [It will remove all test data from DynamoDB]")
			fmt.Println("    > ls                                            [List all shipment IDs ... max 10 elements]")
			fmt.Println("    > id <id>                                       [Get the application object from DynamoDB by app domain ID; CLI is in the app mode, from that point on]")
			fmt.Println("      > sys                                         [Show system info data in a JSON format]")
			fmt.Println("      > data                                        [Print the data as JSON for the current shipment record]")
			fmt.Println("      > info                                        [Print all info regarding Shipment record: system_info and data as JSON]")
			fmt.Println("      > update <new Shipment status>                [Update Shipment status .. e.g.: from UNDER_CONSTRUCTION to READY_TO_SHIP]")
			fmt.Println("      > reset                                       [Reset the system info of the current shipment record]")
			fmt.Println("      > ready                                       [Make the record ready for the shipment]")
			fmt.Println("      > enqueue | en                                [Enqueue current ID]")
			fmt.Println("      > peek                                        [Peek the Shipment from the Queue .. it will replace the current ID with the peeked one]")
			fmt.Println("      > done                                        [Simulate successful record processing completion ... remove from the queue]")
			fmt.Println("      > fail                                        [Simulate failed record's processing ... put back to the queue; needs to be peeked again]")
			fmt.Println("      > invalid                                     [Remove record from the regular queue to dead letter queue (DLQ) for manual fix]")
			fmt.Println("    > id")
		case "aws":
			if params == nil {
				fmt.Println("ERROR: 'aws <profile> [<region>] [<table>]' command requires parameter(s) to be specified!")
				continue
			}

			awsCredentialsProfile := strings.TrimSpace(params[0])

			// specify AWS Region
			if len(params) > 1 {
				temp := strings.TrimSpace(params[1])
				region = &temp
			}

			// specify DynamoDB table name
			if len(params) > 2 {
				temp := strings.TrimSpace(params[2])
				tableName = &temp
			}

			if awsCredentialsProfile == "" && (credentialsProfile != nil || *credentialsProfile != "") {
				awsCredentialsProfile = *credentialsProfile
			} else {
				awsCredentialsProfile = "default"
			}

			client, err = sdk.NewBuilder().
				WithRegion(*region).
				WithCredentialsProfileName(*credentialsProfile).
				WithTableName(*tableName).
				Build(context.Background())
			if err != nil {
				fmt.Printf(" ... AWS session could not be established!: %v\n", err)
			} else {
				fmt.Println(" ... AWS session is properly established!")
			}

		case "id":
			if params == nil || len(params) == 0 {
				shipment = nil
				fmt.Println("Going back to standard CLI mode!")
				continue
			}
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			id := params[0]
			shipment, err = client.Get(context.Background(), id)
			if err != nil {
				printError(err)
				continue
			}
			dump, err := marshalIndent(shipment)
			if err != nil {
				printError(err)
				continue
			}
			fmt.Printf("Shipment's [%s] record dump:\n%s\n", id, dump)
		case "sys", "system":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				printError("`system` or `sys` command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			dump, err := marshalIndent(shipment.SystemInfo)
			if err != nil {
				printError(err)
				continue
			}
			fmt.Printf("ID's system info:\n%s\n", dump)
		case "ls":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			ids, err := client.ListExtendedIDs(context.Background(), 10)
			if err != nil {
				printError(err)
				continue
			}
			if len(ids) == 0 {
				fmt.Println("Shipment table is empty!")
				continue
			}
			fmt.Println("List of first 10 IDs:")
			for _, id := range ids {
				fmt.Printf("* %s\n", id)
			}
		case "purge":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			ctx := context.Background()
			ids, err := client.ListIDs(ctx, 10)
			if err != nil {
				printError(err)
				continue
			}
			if len(ids) == 0 {
				fmt.Println("Shipment table is empty ... nothing to remove!")
				continue
			}
			fmt.Println("List of removed IDs:")
			for _, id := range ids {
				err := client.Delete(ctx, id)
				if err != nil {
					printError(err)
					continue
				}
				fmt.Printf("* ID: %s\n", id)
			}
		case "create-test", "ct":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			ctx := context.Background()
			ids := []string{"A-101", "A-202", "A-303", "A-404"}
			for _, id := range ids {
				_, err := client.CreateTestData(ctx, id)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Printf(" >> Creating shipment with ID : %s\n", id)
			}
		case "qstat", "stat":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if command == "stat" && shipment == nil {
				fmt.Println("ERROR: 'stat' command can be only used in the ID mode. Use 'qstat' instead!")
				continue
			}
			stats, err := client.GetQueueStats(context.Background())
			if err != nil {
				fmt.Println(err)
				continue
			}
			dump, err := json.MarshalIndent(stats, "", "  ")
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Print(string(dump))
		case "dlq":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			stats, err := client.GetDLQStats(context.Background())
			if err != nil {
				fmt.Println(err)
				continue
			}
			dump, err := json.MarshalIndent(stats, "", "  ")
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Print(string(dump))
		case "reset":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'reset' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			shipment.ResetSystemInfo()
			err := client.Put(context.Background(), shipment)
			if err != nil {
				fmt.Println(err)
				continue
			}
			dump, err := json.MarshalIndent(shipment.SystemInfo, "", "  ")
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Print(string(dump))
		case "ready":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'ready' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			shipment.ResetSystemInfo()
			shipment.SystemInfo.Status = model.StatusEnumReadyToShip
			err := client.Put(context.Background(), shipment)
			if err != nil {
				fmt.Println(err)
				continue
			}
			dump, err := json.MarshalIndent(shipment.SystemInfo, "", "  ")
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Print(string(dump))
		case "done":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'done' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			ctx := context.Background()
			_, err := client.UpdateStatus(ctx, shipment.ID, model.StatusEnumCompleted)
			if err != nil {
				fmt.Println(err)
				continue
			}
			_, err = client.Remove(ctx, shipment.ID)
			if err != nil {
				fmt.Println(err)
				continue
			}
			shipment, err := client.Get(ctx, shipment.ID)
			if err != nil {
				fmt.Println(err)
				continue
			}

			fmt.Printf("Processing for ID [%s] is completed successfully! Remove from the queue!\n", shipment.ID)

			stats, err := client.GetQueueStats(ctx)
			if err != nil {
				fmt.Println(err)
				continue
			}
			dump, err := json.Marshal(stats)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Queue status\n%s\n", dump)
		case "fail":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'fail' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}

			ctx := context.Background()
			_, err := client.Restore(ctx, shipment.ID)
			if err != nil {
				fmt.Println(err)
				continue
			}
			shipment, err := client.Get(ctx, shipment.ID)
			if err != nil {
				fmt.Println(err)
				continue
			}

			fmt.Printf("Processing for ID [%s] has failed! Put the record back to the queue!\n", shipment.ID)

			stats, err := client.GetQueueStats(ctx)
			if err != nil {
				fmt.Println(err)
				continue
			}
			dump, err := json.Marshal(stats)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Queue status\n%s\n", dump)
		case "invalid":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'invalid' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}

			ctx := context.Background()
			_, err := client.SendToDLQ(ctx, shipment.ID)
			if err != nil {
				fmt.Println(err)
				continue
			}

			fmt.Printf("Processing for ID [%s] has failed .. invalid data! Send record to DLQ!\n", shipment.ID)

			stats, err := client.GetQueueStats(ctx)
			if err != nil {
				fmt.Println(err)
				continue
			}
			dump, err := json.Marshal(stats)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Queue status\n%s\n", dump)
		case "data":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'data' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			dump, err := json.MarshalIndent(shipment.Data, "", "  ")
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Data info:\n%s\n", dump)
		case "info":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'info' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			dump, err := json.MarshalIndent(shipment, "", "  ")
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Record's dump:\n%s\n", dump)
		case "enqueue", "en":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'enqueue' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			ctx := context.Background()
			shipment, err := client.Get(ctx, shipment.ID)
			if err != nil {
				fmt.Println(err)
				continue
			}
			// convert under_construction to ready to ship
			if shipment.SystemInfo.Status == model.StatusEnumUnderConstruction {
				shipment.ResetSystemInfo()
				shipment.SystemInfo.Status = model.StatusEnumReadyToShip

				err = client.Put(ctx, shipment)
				if err != nil {
					fmt.Println(err)
					continue
				}
			}
			result, err := client.Enqueue(ctx, shipment.ID)
			if err != nil {
				fmt.Println(err)
				continue
			}
			shipment = result.Shipment
			if result.IsSuccessful() {

				systemDump, err := json.MarshalIndent(shipment.SystemInfo, "", "  ")
				if err != nil {
					fmt.Println(err)
					continue
				}

				fmt.Printf("Record's system info:\n%s\n", systemDump)

				queueStatsResult, err := client.GetQueueStats(ctx)
				if err != nil {
					fmt.Println(err)
					continue
				}

				statsDump, err := json.MarshalIndent(queueStatsResult, "", "  ")
				if err != nil {
					fmt.Println(err)
					continue
				}

				fmt.Printf("Queue stats:\n%s\n", statsDump)
			} else {
				resultDump, err := json.Marshal(result)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Printf("Enqueue has failed!\n Error message:\n%s\n", resultDump)
			}
		case "peek":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'peek' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}

			ctx := context.Background()
			result, err := client.Peek(ctx)
			if err != nil {
				fmt.Println(err)
				continue
			}

			if result.IsSuccessful() {
				shipment = result.PeekedShipmentObject

				sysDump, err := json.MarshalIndent(shipment.SystemInfo, "", " ")
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Printf("Peek was successful ... record peeked is: [%s]\n%s\n", shipment.ID, sysDump)

				stats, err := client.GetQueueStats(ctx)
				if err != nil {
					fmt.Println(err)
					continue
				}
				statsDump, err := json.MarshalIndent(stats, "", "  ")
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Printf("Queue stats\n%s", statsDump)

			} else {
				fmt.Printf("peek() has failed!\n Error message:\n%s", result.ReturnValue.GetErrorMessage())
			}

		case "update":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("ERROR: 'update <status>' command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			if params == nil {
				fmt.Println("ERROR: 'update <status>' command requires a new Status parameter to be specified!%n")
				continue
			}
			statusStr := strings.TrimSpace(strings.ToUpper(params[0]))
			if statusStr == string(model.StatusEnumReadyToShip) {

				shipment.MarkAsReadyForShipment()
				rr, err := client.UpdateStatus(context.Background(), shipment.ID, model.StatusEnumReadyToShip)
				if err != nil {
					fmt.Println(err)
					continue
				}
				dump, err := json.Marshal(rr)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Printf("Status changed result:\n%s\n", dump)
			} else {
				fmt.Printf("Status change [%s] is not applied!\n", strings.TrimSpace(params[0]))
			}
		default:
			fmt.Println(" ... unrecognized command!")
		}

	}
EndLoop:

	fmt.Printf(" ... CLI is ending\n\n\n")
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

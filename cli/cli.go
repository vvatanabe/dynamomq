package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/vvatanabe/go82f46979/model"

	"github.com/vvatanabe/go82f46979/constant"

	"github.com/vvatanabe/go82f46979/sdk"
)

const (
	needAWSMessage = "     Need first to run 'aws' command"
)

func Run() {

	fmt.Println("===========================================================")
	fmt.Println(">> Welcome to Priority Queueing CLI Tool!")
	fmt.Println("===========================================================")
	fmt.Println(" for help, enter one of the following: ? or h or help")
	fmt.Println(" all commands in CLIs need to be typed in lowercase")

	executionPath, _ := os.Getwd()
	fmt.Printf(" current directory is: [%s]\n", executionPath)

	region := flag.String("region", constant.AwsRegionDefault, "AWS region")
	credentialsProfile := flag.String("profile", constant.AwsProfileDefault, "AWS credentials profile")

	flag.Parse()

	fmt.Printf(" profile is: [%s]\n", *credentialsProfile)
	fmt.Printf(" region is: [%s]\n", *region)

	client := sdk.NewBuilder().
		WithRegion(*region).
		WithCredentialsProfileName(*credentialsProfile).
		Build()

	// 1. Create a Scanner using the InputStream available.
	scanner := bufio.NewScanner(os.Stdin)

	for {
		var shipment *model.Shipment

		// 2. Don't forget to prompt the user
		if shipment != nil {
			fmt.Printf("\nID <%s> >> Enter command: ", shipment.ID)
		} else {
			fmt.Print("\n >> Enter command: ")
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
			// TODO
		case "id":
			if params == nil || len(params) == 0 {
				shipment = nil
				fmt.Println("     Going back to standard CLI mode!")
				continue
			}

			if client == nil {
				fmt.Println(needAWSMessage)
			} else {
				id := params[0]
				shipment, err := client.Get(context.Background(), id)
				if err != nil {
					fmt.Println(err)
					continue
				}

				dump, err := json.Marshal(shipment)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Printf("     Shipment's [%s] record dump\n%s", id, dump) // Replace "Utils.toJSON(shipment)" with actual JSON conversion function.
			}
		case "sys", "system":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			if shipment == nil {
				fmt.Println("     ERROR: `system` or `sys` command can be only used in the CLI's App mode. Call first `id <record-id>`")
				continue
			}
			dump, err := json.Marshal(shipment.SystemInfo)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("     ID's system info:\n%s\n", dump)
		case "ls":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}

			ids, err := client.ListExtendedIDs(context.Background(), 10)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if len(ids) == 0 {
				fmt.Println("     Shipment table is empty!")
				continue
			}
			fmt.Println("     List of first 10 IDs:")
			for _, id := range ids {
				fmt.Printf("      >> ID : %s\n", id)
			}
		case "purge":
			if client == nil {
				fmt.Println(needAWSMessage)
				continue
			}
			ids, err := client.ListIDs(context.Background(), 10)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if len(ids) == 0 {
				fmt.Println("     Shipment table is empty ... nothing to remove!")
				continue
			}
			fmt.Println("     List of removed IDs:")
			for _, id := range ids {

				err := client.Delete(context.Background(), id)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Printf("      >> Removed ID : %s\n", id)
			}
		}

	}
EndLoop:

	fmt.Printf(" ... CLI is ending\n\n\n")
}

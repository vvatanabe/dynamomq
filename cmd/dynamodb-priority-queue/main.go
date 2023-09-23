package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/vvatanabe/go82f46979/cli"
	"github.com/vvatanabe/go82f46979/sdk"
)

func main() {
	defer fmt.Printf("... CLI is ending\n\n\n")

	fmt.Println("===========================================================")
	fmt.Println(">> Welcome to Priority Queueing CLI Tool!")
	fmt.Println("===========================================================")
	fmt.Println("for help, enter one of the following: ? or h or help")
	fmt.Println("all commands in CLIs need to be typed in lowercase")
	fmt.Println("")

	executionPath, _ := os.Getwd()
	fmt.Printf("current directory is: [%s]\n", executionPath)

	region := flag.String("region", sdk.AwsRegionDefault, "AWS region")
	credentialsProfile := flag.String("profile", sdk.AwsProfileDefault, "AWS credentials profile")
	tableName := flag.String("table", sdk.DefaultTableName, "AWS DynamoDB table name")
	endpoint := flag.String("endpoint-url", "", "AWS DynamoDB base endpoint url")

	flag.Parse()

	fmt.Printf("profile is: [%s]\n", *credentialsProfile)
	fmt.Printf("region is: [%s]\n", *region)
	fmt.Printf("table is: [%s]\n", *tableName)
	fmt.Printf("endpoint is: [%s]\n", *endpoint)
	fmt.Println("")

	client, err := sdk.NewQueueSDKClient(context.Background(),
		sdk.WithAWSRegion(*region),
		sdk.WithAWSCredentialsProfileName(*credentialsProfile),
		sdk.WithTableName(*tableName),
		sdk.WithAWSBaseEndpoint(*endpoint))
	if err != nil {
		fmt.Printf("... AWS session could not be established!: %v\n", err)
	} else {
		fmt.Println("... AWS session is properly established!")
	}

	c := cli.CLI{
		Region:             region,
		CredentialsProfile: credentialsProfile,
		TableName:          tableName,
		Client:             client,
		Shipment:           nil,
	}

	// 1. Create a Scanner using the InputStream available.
	scanner := bufio.NewScanner(os.Stdin)

	for {
		// 2. Don't forget to prompt the user
		if c.Shipment != nil {
			fmt.Printf("\nID <%s> >> Enter command: ", c.Shipment.ID)
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
			return
		default:
			// 4. Now, you can do anything with the input string that you need to.
			// Like, output it to the user.
			c.Run(context.Background(), command, params)
		}
	}
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

package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

type CommandFactory struct {
	CreateDynamoMQClient func(ctx context.Context, flags *Flags) (dynamomq.Client[any], aws.Config, error)
}

var defaultCommandFactory = CommandFactory{
	CreateDynamoMQClient: createDynamoMQClient[any],
}

var root = defaultCommandFactory.CreateRootCommand(flgs)

func setDefaultFlags(c *cobra.Command, flgs *Flags) {
	c.Flags().StringVar(&flgs.TableName, flagMap.TableName.Name, flagMap.TableName.Value, flagMap.TableName.Usage)
	c.Flags().StringVar(&flgs.EndpointURL, flagMap.EndpointURL.Name, flagMap.EndpointURL.Value, flagMap.EndpointURL.Usage)
}

func (f CommandFactory) CreateRootCommand(flgs *Flags) *cobra.Command {
	return &cobra.Command{
		Use:     "dynamomq",
		Short:   "DynamoMQ is a tool for implementing message queueing with Amazon DynamoDB in Go",
		Long:    `DynamoMQ is a tool for implementing message queueing with Amazon DynamoDB in Go.`,
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
			client, cfg, err := createDynamoMQClient[any](ctx, flgs)
			if err != nil {
				return fmt.Errorf("... %v\n", err)
			}

			fmt.Println("... AWS session is properly established!")

			fmt.Printf("AWSRegion: %s\n", cfg.Region)
			fmt.Printf("TableName: %s\n", flgs.TableName)
			fmt.Printf("EndpointURL: %s\n", flgs.EndpointURL)
			fmt.Println("")

			c := Interactive{
				Client:  client,
				Message: nil,
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
					err := c.Run(context.Background(), command, params)
					if err != nil {
						printError(err)
					}
				}
			}
			return nil
		},
	}
}

func createDynamoMQClient[T any](ctx context.Context, flags *Flags) (dynamomq.Client[T], aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, cfg, fmt.Errorf("failed to load aws config: %s", err)
	}
	client, err := dynamomq.NewFromConfig[T](cfg,
		dynamomq.WithTableName(flags.TableName),
		dynamomq.WithQueueingIndexName(flags.IndexName),
		dynamomq.WithAWSBaseEndpoint(flags.EndpointURL))
	if err != nil {
		return nil, cfg, fmt.Errorf("AWS session could not be established!: %v", err)
	}
	return client, cfg, nil
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
	if err := root.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	setDefaultFlags(root, flgs)
	root.Flags().StringVar(&flgs.IndexName, flagMap.IndexName.Name, flagMap.IndexName.Value, flagMap.IndexName.Usage)
}

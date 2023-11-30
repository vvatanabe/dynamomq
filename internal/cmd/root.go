package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	"github.com/vvatanabe/dynamomq"
)

type CommandFactory struct {
	CreateDynamoMQClient func(ctx context.Context, flags *Flags) (dynamomq.Client[any], aws.Config, error)
	Stdin                io.Reader
}

var defaultCommandFactory = CommandFactory{
	CreateDynamoMQClient: createDynamoMQClient[any],
	Stdin:                os.Stdin,
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
			client, cfg, err := f.CreateDynamoMQClient(ctx, flgs)
			if err != nil {
				return err
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
			scanner := bufio.NewScanner(f.Stdin)

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

				var quit bool
				command, params := ParseInput(input)
				switch command {
				case "":
					continue
				case "quit", "q":
					quit = true
				default:
					// 4. Now, you can do anything with the input string that you need to.
					// Like, output it to the user.
					runErr := c.Run(context.Background(), command, params)
					if runErr != nil {
						printError(runErr)
					}
				}
				if quit {
					break
				}
			}
			return nil
		},
	}
}

func createDynamoMQClient[T any](ctx context.Context, flags *Flags) (dynamomq.Client[T], aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, cfg, fmt.Errorf("failed to load aws config: %w", err)
	}
	client, err := dynamomq.NewFromConfig[T](cfg,
		dynamomq.WithTableName(flags.TableName),
		dynamomq.WithQueueingIndexName(flags.IndexName),
		dynamomq.WithAWSBaseEndpoint(flags.EndpointURL))
	if err != nil {
		return nil, cfg, fmt.Errorf("AWS session could not be established!: %w", err)
	}
	return client, cfg, nil
}

func ParseInput(input string) (command string, params []string) {
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
		printError(err)
		os.Exit(1)
	}
}

type SystemInfo struct {
	ID                     string             `json:"id"`
	Status                 dynamomq.Status    `json:"status"`
	ReceiveCount           int                `json:"receive_count"`
	QueueType              dynamomq.QueueType `json:"queue_type"`
	Version                int                `json:"version"`
	CreationTimestamp      string             `json:"creation_timestamp"`
	LastUpdatedTimestamp   string             `json:"last_updated_timestamp"`
	AddToQueueTimestamp    string             `json:"queue_add_timestamp"`
	PeekFromQueueTimestamp string             `json:"queue_peek_timestamp"`
}

func GetSystemInfo[T any](m *dynamomq.Message[T]) *SystemInfo {
	return &SystemInfo{
		ID:                     m.ID,
		Status:                 m.Status,
		ReceiveCount:           m.ReceiveCount,
		QueueType:              m.QueueType,
		Version:                m.Version,
		CreationTimestamp:      m.CreationTimestamp,
		LastUpdatedTimestamp:   m.LastUpdatedTimestamp,
		AddToQueueTimestamp:    m.AddToQueueTimestamp,
		PeekFromQueueTimestamp: m.PeekFromQueueTimestamp,
	}
}

func ResetSystemInfo[T any](m *dynamomq.Message[T], now time.Time) {
	msg := dynamomq.NewMessage[T](m.ID, m.Data, now)
	m.Status = msg.Status
	m.QueueType = msg.QueueType
	m.ReceiveCount = msg.ReceiveCount
	m.Version = msg.Version
	m.CreationTimestamp = msg.CreationTimestamp
	m.LastUpdatedTimestamp = msg.LastUpdatedTimestamp
	m.AddToQueueTimestamp = msg.AddToQueueTimestamp
	m.PeekFromQueueTimestamp = msg.PeekFromQueueTimestamp
}

func init() {
	setDefaultFlags(root, flgs)
	root.Flags().StringVar(&flgs.IndexName, flagMap.IndexName.Name, flagMap.IndexName.Value, flagMap.IndexName.Usage)
}

package cmd

import (
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
	"github.com/vvatanabe/dynamomq/internal/clock"
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
			return f.runRootCommand(flgs)
		},
	}
}

func (f CommandFactory) runRootCommand(flgs *Flags) error {
	defer fmt.Printf("... Interactive is ending\n\n\n")
	printWelcomeMessage()

	ctx := context.Background()
	client, cfg, err := f.CreateDynamoMQClient(ctx, flgs)
	if err != nil {
		return err
	}
	printAWSConfig(cfg, flgs)

	interactive := &Interactive{Client: client, Message: nil}
	return interactive.Start(f.Stdin)
}

func printWelcomeMessage() {
	fmt.Println("===========================================================")
	fmt.Println(">> Welcome to DynamoMQ CLI! [INTERACTIVE MODE]")
	fmt.Println("===========================================================")
	fmt.Println("for help, enter one of the following: ? or h or help")
	fmt.Println("all commands in CLIs need to be typed in lowercase")
	fmt.Println("")
}

func printAWSConfig(cfg aws.Config, flgs *Flags) {
	fmt.Println("... AWS session is properly established!")
	fmt.Printf("AWSRegion: %s\n", cfg.Region)
	fmt.Printf("TableName: %s\n", flgs.TableName)
	fmt.Printf("EndpointURL: %s\n", flgs.EndpointURL)
	fmt.Println("")
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
	ID           string             `json:"id"`
	Status       dynamomq.Status    `json:"status"`
	ReceiveCount int                `json:"receive_count"`
	QueueType    dynamomq.QueueType `json:"queue_type"`
	Version      int                `json:"version"`
	CreatedAt    string             `json:"created_at"`
	UpdatedAt    string             `json:"updated_at"`
	SentAt       string             `json:"sent_at"`
	ReceivedAt   string             `json:"received_at"`
}

func GetSystemInfo[T any](m *dynamomq.Message[T]) *SystemInfo {
	return &SystemInfo{
		ID:           m.ID,
		Status:       m.GetStatus(clock.Now()),
		ReceiveCount: m.ReceiveCount,
		QueueType:    m.QueueType,
		Version:      m.Version,
		CreatedAt:    m.CreatedAt,
		UpdatedAt:    m.UpdatedAt,
		SentAt:       m.SentAt,
		ReceivedAt:   m.ReceivedAt,
	}
}

func ResetSystemInfo[T any](m *dynamomq.Message[T], now time.Time) {
	msg := dynamomq.NewMessage[T](m.ID, m.Data, now)
	m.QueueType = msg.QueueType
	m.VisibilityTimeout = msg.VisibilityTimeout
	m.ReceiveCount = msg.ReceiveCount
	m.Version = msg.Version
	m.CreatedAt = msg.CreatedAt
	m.UpdatedAt = msg.UpdatedAt
	m.SentAt = msg.SentAt
	m.ReceivedAt = msg.ReceivedAt
}

func init() {
	setDefaultFlags(root, flgs)
	root.Flags().StringVar(&flgs.IndexName, flagMap.IndexName.Name, flagMap.IndexName.Value, flagMap.IndexName.Usage)
}

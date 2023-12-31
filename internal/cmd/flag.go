package cmd

import (
	"github.com/vvatanabe/dynamomq/internal/constant"
)

var flgs = &Flags{}

type Flags struct {
	TableName   string
	IndexName   string
	EndpointURL string

	ID string
}

var flagMap = FlagMap{
	TableName: FlagSet[string]{
		Name:  "table-name",
		Usage: "The name of the table to contain the item.",
		Value: constant.DefaultTableName,
	},
	IndexName: FlagSet[string]{
		Name:  "index-name",
		Usage: "The name of the queueing index.",
		Value: constant.DefaultQueueingIndexName,
	},
	EndpointURL: FlagSet[string]{
		Name:  "endpoint-url",
		Usage: "Override command's default URL with the given URL.",
		Value: "",
	},
	ID: FlagSet[string]{
		Name:  "id",
		Usage: "Message ID in queue.",
		Value: "",
	},
}

type FlagSet[T any] struct {
	Name  string
	Usage string
	Value T
}

type FlagMap struct {
	TableName   FlagSet[string]
	IndexName   FlagSet[string]
	EndpointURL FlagSet[string]
	ID          FlagSet[string]
}

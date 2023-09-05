package sdk

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	go82f46979 "github.com/vvatanabe/go82f46979"
	"github.com/vvatanabe/go82f46979/appdata"
	"github.com/vvatanabe/go82f46979/model"
)

type QueueSDKClient struct {
	db     *dynamodb.Client
	key    interface{} // Placeholder for ConfigField type
	config interface{} // Placeholder for Configuration type

	actualTableName           string
	logicalTableName          string
	awsRegion                 string
	awsCredentialsProfileName string

	retryPolicyRetryCount int
}

func (c *QueueSDKClient) getDLQStats(ctx context.Context) (*model.DLQResult, error) {
	var totalDLQSize int
	var lastEvaluatedKey map[string]types.AttributeValue

	var listBANs []string

	keyCondition := expression.KeyEqual(expression.Key("DLQ"), expression.Value("1"))
	proj := expression.NamesList(expression.Name("id"), expression.Name("DLQ"), expression.Name("system_info"))

	expr, err := expression.NewBuilder().WithKeyCondition(keyCondition).WithProjection(proj).Build()
	if err != nil {
		return nil, fmt.Errorf("error building expression: %w", err)
	}

	for {
		input := &dynamodb.QueryInput{
			ProjectionExpression:      expr.Projection(),
			IndexName:                 aws.String(go82f46979.DlqQueueingIndexName),
			TableName:                 aws.String(c.actualTableName),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			KeyConditionExpression:    expr.KeyCondition(),
			Limit:                     aws.Int32(250),
			ExclusiveStartKey:         lastEvaluatedKey,
		}

		resp, err := c.db.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("error querying dynamodb: %w", err)
		}

		lastEvaluatedKey = resp.LastEvaluatedKey

		for _, itemMap := range resp.Items {
			totalDLQSize++

			if len(listBANs) < 100 {
				item := appdata.Shipment{}
				err := attributevalue.UnmarshalMap(itemMap, &item)
				if err != nil {
					return nil, fmt.Errorf("failed to unmarshal map: %s", err)
				}
				listBANs = append(listBANs, item.ID)
			}
		}

		if lastEvaluatedKey == nil {
			break
		}
	}

	return &model.DLQResult{
		First100IDsInQueue: listBANs,
		TotalRecordsInDLQ:  totalDLQSize,
	}, nil
}

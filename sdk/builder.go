package sdk

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type Builder struct {
	awsRegion                 string
	awsCredentialsProfileName string
	tableName                 string
	credentialsProvider       aws.CredentialsProvider
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Build(ctx context.Context) (QueueSDKClient, error) {
	if b.awsRegion == "" {
		b.awsRegion = AwsRegionDefault
	}
	if b.awsCredentialsProfileName == "" {
		b.awsCredentialsProfileName = AwsProfileDefault
	}
	if b.tableName == "" {
		b.tableName = DefaultTableName
	}
	return initialize(ctx, b)
}

func (b *Builder) WithRegion(region string) *Builder {
	b.awsRegion = region
	return b
}

func (b *Builder) WithCredentialsProfileName(profile string) *Builder {
	b.awsCredentialsProfileName = profile
	return b
}

func (b *Builder) WithTableName(tableName string) *Builder {
	b.tableName = tableName
	return b
}

func (b *Builder) WithCredentialsProvider(creds aws.CredentialsProvider) *Builder {
	b.credentialsProvider = creds
	return b
}

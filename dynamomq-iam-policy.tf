resource "aws_iam_policy" "dynamomq_policy" {
  name        = "DynamoMQPolicy"
  path        = "/"
  description = "IAM policy for DynamoMQ access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "dynamodb:Query",
          "dynamodb:GetItem",
          "dynamodb:BatchGetItem",
          "dynamodb:Scan",
          "dynamodb:DeleteItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:TransactWriteItems",
          "dynamodb:DescribeTable",
          "dynamodb:CreateTable"
        ]
        Resource = [
          "arn:aws:dynamodb:${var.region}:${var.account_id}:table/dynamo-mq-table",
          "arn:aws:dynamodb:${var.region}:${var.account_id}:table/dynamo-mq-table/index/dynamo-mq-index-queue_type-queue_add_timestamp"
        ]
      }
    ]
  })
}

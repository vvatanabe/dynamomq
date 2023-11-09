resource "aws_dynamodb_table" "dynamo_mq_table" {
  name           = "dynamo-mq-table"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "id"
  deletion_protection_enabled = true

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "queue_type"
    type = "S"
  }

  attribute {
    name = "queue_add_timestamp"
    type = "S"
  }

  global_secondary_index {
    name               = "dynamo-mq-index-queue_type-queue_add_timestamp"
    hash_key           = "queue_type"
    range_key          = "queue_add_timestamp"
    projection_type    = "ALL"
  }
}

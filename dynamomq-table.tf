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
    name = "sequence_key"
    type = "S"
  }

  global_secondary_index {
    name               = "dynamo-mq-index-queue_type-sequence_key"
    hash_key           = "queue_type"
    range_key          = "sequence_key"
    projection_type    = "ALL"
  }
}

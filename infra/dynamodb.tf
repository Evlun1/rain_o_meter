resource "aws_dynamodb_table" "rainfall" {
  name         = "rainfall"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "timestamp_id"

  attribute {
    name = "timestamp_id"
    type = "S"
  }

  tags = {
    Name = "rainfall"
  }
}

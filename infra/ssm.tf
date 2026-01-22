data "aws_kms_key" "this" {
  key_id = "alias/aws/ssm"
}

resource "aws_ssm_parameter" "token" {
  name   = "rain_o_meter_mf_token"
  type   = "SecureString"
  value  = var.mf_token
  key_id = data.aws_kms_key.this.key_id

  tags = {
    Name = "rain_o_meter_mf_token"
  }
}

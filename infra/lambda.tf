data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "../backend/zip_build_dir"
  output_path = "${path.module}/lambda_package.zip"
}

resource "aws_lambda_function" "api" {
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  function_name    = var.project_name
  role             = aws_iam_role.lambda_exec.arn
  handler          = "src.api.handler"
  runtime          = "python3.13"

  environment {
    variables = {
      MF_CLIMATE_APP_ID      = aws_ssm_parameter.token.value
      ENVIRONMENT            = "DEPLOYED"
      YEAR_BEG_INCL          = 1990
      YEAR_END_INCL          = 2020
      BACKEND_TABLE_NAME     = aws_dynamodb_table.rainfall.name
      BACKEND_TABLE_KEY_NAME = aws_dynamodb_table.rainfall.hash_key
    }
  }

  tags = {
    Name = "rain_o_meter"
  }
}

resource "aws_lambda_function_url" "api_url" {
  function_name      = aws_lambda_function.api.function_name
  authorization_type = "NONE"
  cors {
    allow_origins = ["*"] # TODO: replace by front URL once deployed
    allow_methods = ["GET"]
  }
}

# Initialize API on deployment
resource "terraform_data" "api_init" {
  triggers_replace = [
    aws_lambda_function.api.last_modified
  ]

  provisioner "local-exec" {
    command = "curl -X POST ${aws_lambda_function_url.api_url.function_url}/initialize"
  }

  depends_on = [aws_lambda_function_url.api_url]
}

# =============================================================================
# lambda.tf — Lambda de trigger de Glue Crawler + EventBridge
# =============================================================================

# ── Empacota o código da Lambda ───────────────────────────────────────────────
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.root}/../../lambda/fn_start_glue_crawler/lambda_function.py"
  output_path = "${path.root}/lambda_fn_start_glue_crawler.zip"
}

# ── Lambda Function ───────────────────────────────────────────────────────────
resource "aws_lambda_function" "glue_crawler_trigger" {
  function_name    = "fn-start-glue-crawler-cc"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      ENVIRONMENT = var.environment
    }
  }

  tags = {
    Component = "crawler-trigger"
  }
}

# ── CloudWatch Log Group da Lambda ────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${aws_lambda_function.glue_crawler_trigger.function_name}"
  retention_in_days = var.cloudwatch_retention_days
}

# ── EventBridge Rule — S3 PutObject → Lambda ─────────────────────────────────
resource "aws_cloudwatch_event_rule" "s3_bronze_put" {
  name        = "rule-cc-s3-bronze-put"
  description = "Dispara Lambda quando objetos chegam no prefixo bronze/ do S3"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.bucket_name] }
      object = { key = [{ prefix = "bronze/" }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.s3_bronze_put.name
  target_id = "TriggerLambdaCrawler"
  arn       = aws_lambda_function.glue_crawler_trigger.arn
}

# ── Permissão para EventBridge invocar a Lambda ───────────────────────────────
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.glue_crawler_trigger.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.s3_bronze_put.arn
}

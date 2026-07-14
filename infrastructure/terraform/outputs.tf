# =============================================================================
# outputs.tf — Valores exportados após o apply
# =============================================================================

output "s3_bucket_name" {
  description = "Nome do bucket S3 do lakehouse"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "s3_bucket_arn" {
  description = "ARN do bucket S3"
  value       = aws_s3_bucket.lakehouse.arn
}

output "glue_role_arn" {
  description = "ARN da IAM Role dos Glue Jobs"
  value       = aws_iam_role.glue.arn
}

output "lambda_role_arn" {
  description = "ARN da IAM Role da Lambda"
  value       = aws_iam_role.lambda.arn
}

output "quicksight_role_arn" {
  description = "ARN da IAM Role do QuickSight"
  value       = aws_iam_role.quicksight.arn
}

output "lambda_function_arn" {
  description = "ARN da Lambda fn-start-glue-crawler-cc"
  value       = aws_lambda_function.glue_crawler_trigger.arn
}

output "glue_workflow_name" {
  description = "Nome do Glue Workflow principal"
  value       = aws_glue_workflow.pipeline.name
}

output "glue_databases" {
  description = "Glue Catalog Databases criados"
  value = {
    bronze = aws_glue_catalog_database.bronze.name
    silver = aws_glue_catalog_database.silver.name
    gold   = aws_glue_catalog_database.gold.name
  }
}

output "bronze_jobs_count" {
  description = "Total de Glue Jobs Bronze→Silver criados"
  value       = length(aws_glue_job.bronze_to_silver)
}

output "gold_jobs_count" {
  description = "Total de Glue Jobs Silver→Gold criados"
  value       = length(aws_glue_job.silver_to_gold)
}

output "athena_output_location" {
  description = "S3 path de output do Athena"
  value       = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"
}

output "account_id" {
  description = "ID da conta AWS"
  value       = local.account_id
  sensitive   = true
}

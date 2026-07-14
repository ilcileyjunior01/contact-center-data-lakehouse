# =============================================================================
# cloudwatch.tf — Log Groups dos Glue Jobs
# =============================================================================

# Log Group dos Glue Jobs Bronze→Silver
resource "aws_cloudwatch_log_group" "glue_bronze" {
  for_each          = toset(local.bronze_jobs)
  name              = "/aws-glue/jobs/${each.value}"
  retention_in_days = var.cloudwatch_retention_days
}

# Log Group dos Glue Jobs Silver→Gold
resource "aws_cloudwatch_log_group" "glue_gold" {
  for_each          = toset(local.all_gold_jobs)
  name              = "/aws-glue/jobs/${each.value}"
  retention_in_days = var.cloudwatch_retention_days
}

# Log Group do Glue Workflow
resource "aws_cloudwatch_log_group" "glue_workflow" {
  name              = "/aws-glue/workflows/wf-cc-pipeline-diario"
  retention_in_days = var.cloudwatch_retention_days
}

# Alarme de falha no pipeline (SNS opcional)
resource "aws_cloudwatch_metric_alarm" "glue_job_failures" {
  alarm_name          = "alarm-cc-glue-job-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Um ou mais Glue Jobs do Contact Center Lakehouse falharam"
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = "wf-cc-pipeline-diario"
  }

  # Opcional: adicionar SNS topic para notificação por e-mail
  # alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

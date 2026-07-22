# =============================================================================
# variables.tf — Variáveis configuráveis do projeto
# =============================================================================

variable "aws_region" {
  description = "Região AWS onde os recursos serão criados"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Ambiente de deploy (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment deve ser: dev, staging ou prod"
  }
}

variable "bucket_name" {
  description = "Nome do bucket S3 do lakehouse"
  type        = string
  default     = "act-cc-dev-lakehouse"
}

variable "glue_role_name" {
  description = "Nome da IAM Role usada pelos Glue Jobs"
  type        = string
  default     = "GlueExecutionRole-ContactCenter"
}

variable "lambda_role_name" {
  description = "Nome da IAM Role usada pela Lambda de trigger"
  type        = string
  default     = "LambdaGlueTriggerRole-ContactCenter"
}

variable "quicksight_role_name" {
  description = "Nome da IAM Role usada pelo QuickSight"
  type        = string
  default     = "QuickSightServiceRole-ContactCenter"
}

variable "cloudwatch_retention_days" {
  description = "Retenção dos logs do CloudWatch em dias"
  type        = number
  default     = 7

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365], var.cloudwatch_retention_days)
    error_message = "Retenção deve ser um valor válido do CloudWatch (ex: 1, 3, 7, 14, 30...)"
  }
}

variable "s3_quarantine_glacier_days" {
  description = "Dias para mover arquivos de quarentena para Glacier"
  type        = number
  default     = 30
}

variable "glue_version" {
  description = "Versão do Glue usada nos jobs"
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Tipo de worker dos Glue Jobs"
  type        = string
  default     = "G.1X"
}

variable "glue_num_workers" {
  description = "Número de workers por Glue Job"
  type        = number
  default     = 2
}

variable "redshift_role_name" {
  description = "Nome da IAM Role usada pelo Redshift para COPY do S3"
  type        = string
  default     = "RedshiftS3CopyRole-ContactCenter"
}

variable "redshift_admin_password" {
  description = "Senha do admin do Redshift Serverless (use tfvars ou variavel de ambiente)"
  type        = string
  sensitive   = true
  default     = "Admin123!"
}

variable "redshift_base_capacity" {
  description = "Capacidade base do Redshift Serverless em RPUs (minimo: 8)"
  type        = number
  default     = 8

  validation {
    condition     = var.redshift_base_capacity >= 8 && var.redshift_base_capacity <= 512
    error_message = "redshift_base_capacity deve estar entre 8 e 512 RPUs."
  }
}

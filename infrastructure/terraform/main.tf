# =============================================================================
# main.tf — Contact Center Data Lakehouse
# =============================================================================
# Provisiona toda a infraestrutura AWS do projeto via Terraform.
#
# Recursos criados:
#   - S3 bucket (lakehouse + estrutura de prefixos)
#   - Glue Catalog Databases (db_bronze, db_silver, db_gold)
#   - IAM Roles (Glue, Lambda, QuickSight)
#   - Lambda function (trigger de crawler)
#   - EventBridge rule (S3 → Lambda)
#   - CloudWatch Log Groups (retenção 14 dias)
#
# Uso:
#   terraform init
#   terraform plan
#   terraform apply
#   terraform destroy   # para economizar — dados S3 são removidos
# =============================================================================

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }

  # Backend local para desenvolvimento (portfólio)
  # Em produção, substituir por S3 backend:
  # backend "s3" {
  #   bucket = "meu-terraform-state"
  #   key    = "contact-center/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "contact-center-data-lakehouse"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "ilciley-junior"
    }
  }
}

# =============================================================================
# Data sources
# =============================================================================

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
  bucket_arn = "arn:aws:s3:::${var.bucket_name}"
}

# =========================================================
# Terraform — Redshift Serverless
# Contact Center Data Lakehouse
# =========================================================
# Provisiona:
#   - IAM Role para COPY S3 -> Redshift
#   - Redshift Serverless Namespace
#   - Redshift Serverless Workgroup
# =========================================================

# ─── IAM Role — Redshift COPY S3 ─────────────────────────────────────────────

resource "aws_iam_role" "redshift" {
  name = var.redshift_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "redshift.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = { Name = var.redshift_role_name }
}

resource "aws_iam_role_policy" "redshift_s3_copy" {
  name = "S3CopyAndGlueReadPolicy"
  role = aws_iam_role.redshift.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3GoldRead"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/gold/*",
        ]
      },
      {
        Sid    = "GlueCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartitions",
        ]
        Resource = ["*"]
      }
    ]
  })
}

# ─── Redshift Serverless — Namespace ─────────────────────────────────────────

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "cc-lakehouse-namespace"
  db_name             = "dev"
  admin_username      = "admin"
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift.arn]

  tags = { Name = "cc-lakehouse-namespace" }

  lifecycle {
    ignore_changes = [admin_user_password]
  }
}

# ─── Redshift Serverless — Workgroup ─────────────────────────────────────────

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "cc-lakehouse-workgroup"
  base_capacity  = var.redshift_base_capacity

  publicly_accessible = false

  # Auto-pause: pausa automaticamente após 30 minutos de inatividade.
  # Cobra apenas pelo tempo em que há queries ativas (RPU × segundos de uso).
  # Sem isso, o workgroup permanece "warm" e cobra continuamente.
  config_parameter {
    parameter_key   = "max_query_execution_time"
    parameter_value = "3600"
  }

  tags = { Name = "cc-lakehouse-workgroup" }

  depends_on = [aws_redshiftserverless_namespace.main]
}

# ─── Outputs ─────────────────────────────────────────────────────────────────

output "redshift_namespace_id" {
  description = "ID do Redshift Serverless Namespace"
  value       = aws_redshiftserverless_namespace.main.id
}

output "redshift_workgroup_endpoint" {
  description = "Endpoint do Redshift Serverless (host:port)"
  value       = "${aws_redshiftserverless_workgroup.main.endpoint[0].address}:${aws_redshiftserverless_workgroup.main.endpoint[0].port}"
}

output "redshift_iam_role_arn" {
  description = "ARN da IAM Role usada pelo Redshift para COPY do S3"
  value       = aws_iam_role.redshift.arn
}

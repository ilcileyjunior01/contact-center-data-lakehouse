# =============================================================================
# iam.tf — IAM Roles e Policies do projeto
# =============================================================================

# ── IAM Role: Glue Jobs ───────────────────────────────────────────────────────

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = var.glue_role_name
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  description        = "Role para Glue Jobs do Contact Center Lakehouse"
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "GlueS3AccessPolicy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3FullAccessLakehouse"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:GetBucketLocation", "s3:ListBucket",
          "s3:ListBucketMultipartUploads", "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "${local.bucket_arn}",
          "${local.bucket_arn}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogsAccess"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream",
          "logs:PutLogEvents", "logs:AssociateKmsKey"
        ]
        Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:GetPartition", "glue:GetPartitions",
          "glue:CreateTable", "glue:UpdateTable",
          "glue:BatchCreatePartition", "glue:BatchDeletePartition"
        ]
        Resource = "*"
      }
    ]
  })
}

# ── IAM Role: Lambda (trigger de Glue Crawler) ───────────────────────────────

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = var.lambda_role_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  description        = "Role para Lambda de trigger de Glue Crawler"
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_glue" {
  name = "LambdaGlueCrawlerPolicy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCrawlerAccess"
        Effect = "Allow"
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlerMetrics"
        ]
        Resource = "arn:aws:glue:${local.region}:${local.account_id}:crawler/*"
      }
    ]
  })
}

# ── IAM Role: QuickSight ──────────────────────────────────────────────────────

data "aws_iam_policy_document" "quicksight_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["quicksight.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "quicksight" {
  name               = var.quicksight_role_name
  assume_role_policy = data.aws_iam_policy_document.quicksight_assume_role.json
  description        = "Role para QuickSight acessar Athena e S3 do Lakehouse"
}

resource "aws_iam_role_policy" "quicksight_access" {
  name = "QuickSightContactCenterPolicy"
  role = aws_iam_role.quicksight.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:BatchGetQueryExecution", "athena:GetQueryExecution",
          "athena:GetQueryResults", "athena:StartQueryExecution",
          "athena:StopQueryExecution", "athena:ListWorkGroups",
          "athena:GetWorkGroup", "athena:GetDataCatalog",
          "athena:ListDataCatalogs", "athena:ListDatabases",
          "athena:GetDatabase", "athena:ListTableMetadata",
          "athena:GetTableMetadata"
        ]
        Resource = "*"
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabases", "glue:GetDatabase",
          "glue:GetTables", "glue:GetTable",
          "glue:GetPartitions", "glue:GetPartition"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          "arn:aws:glue:${local.region}:${local.account_id}:database/db_gold",
          "arn:aws:glue:${local.region}:${local.account_id}:table/db_gold/*"
        ]
      },
      {
        Sid    = "S3DataAccess"
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket",
          "s3:PutObject", "s3:DeleteObject"
        ]
        Resource = [
          "${local.bucket_arn}",
          "${local.bucket_arn}/*"
        ]
      }
    ]
  })
}

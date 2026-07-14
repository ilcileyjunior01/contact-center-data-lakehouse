# =============================================================================
# s3.tf — Bucket S3 do Lakehouse
# =============================================================================

# ── Bucket principal ──────────────────────────────────────────────────────────
resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name

  # Evita destruição acidental em produção
  # force_destroy = false (padrão) — impede `terraform destroy` se tiver objetos
  force_destroy = var.environment == "dev" ? true : false
}

# ── Block Public Access ───────────────────────────────────────────────────────
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Versionamento ─────────────────────────────────────────────────────────────
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ── Criptografia (SSE-S3) ─────────────────────────────────────────────────────
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# ── Lifecycle rules ───────────────────────────────────────────────────────────
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Regra 1: Quarentena → Glacier após N dias
  rule {
    id     = "quarantine-to-glacier"
    status = "Enabled"

    filter {
      prefix = "quarantine/"
    }

    transition {
      days          = var.s3_quarantine_glacier_days
      storage_class = "GLACIER"
    }
  }

  # Regra 2: Athena results → expirar após 30 dias
  rule {
    id     = "athena-results-expiry"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 30
    }
  }

  # Regra 3: Checkpoints (watermarks) — manter versões antigas por 7 dias
  rule {
    id     = "checkpoints-versions"
    status = "Enabled"

    filter {
      prefix = "checkpoints/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# ── EventBridge notifications (S3 → EventBridge) ─────────────────────────────
resource "aws_s3_bucket_notification" "lakehouse" {
  bucket      = aws_s3_bucket.lakehouse.id
  eventbridge = true   # encaminha todos os eventos para EventBridge
}

# ── Objetos marcadores de prefixo (.keep) ────────────────────────────────────
# S3 não tem "pastas" reais — esses objetos tornam os prefixos visíveis no console
locals {
  s3_prefixes = [
    "bronze/operacao/chamada/",
    "bronze/operacao/ticket/",
    "bronze/operacao/chat/",
    "bronze/operacao/whatsapp/",
    "bronze/operacao/ura/",
    "bronze/operacao/discagem/",
    "bronze/operacao/metricas_operacionais/",
    "bronze/operacao/gravacao_chamada/",
    "bronze/cadastro/cliente/",
    "bronze/cadastro/endereco_cliente/",
    "bronze/cadastro/operador/",
    "bronze/cadastro/skill_operador/",
    "bronze/cadastro/fila_atendimento/",
    "bronze/qualidade/avaliacao_qualidade/",
    "bronze/qualidade/jornada_operador/",
    "bronze/qualidade/interacao_ticket/",
    "bronze/marketing/campanha/",
    "bronze/suporte/mensagem_chat/",
    "silver/",
    "gold/dimensoes/",
    "gold/fatos/",
    "quarantine/",
    "checkpoints/",
    "scripts/bronze_to_silver/",
    "scripts/silver_to_gold/",
    "logs/",
    "athena-results/",
  ]
}

resource "aws_s3_object" "prefix_markers" {
  for_each = toset(local.s3_prefixes)

  bucket  = aws_s3_bucket.lakehouse.id
  key     = "${each.value}.keep"
  content = ""

  # Não recriar se o objeto já existir com conteúdo diferente
  lifecycle {
    ignore_changes = [content]
  }
}

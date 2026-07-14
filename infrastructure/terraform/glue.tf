# =============================================================================
# glue.tf — Glue Catalog Databases e Workflow
# =============================================================================

# ── Glue Databases ────────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "bronze" {
  name        = "db_bronze"
  description = "Camada Bronze — dados brutos particionados por data de ingestão"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "db_silver"
  description = "Camada Silver — dados limpos, deduplicados e mascarados (Iceberg v2)"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "db_gold"
  description = "Camada Gold — Star Schema: 11 dimensões + 11 fatos (Iceberg v2)"
}

# ── Glue Workflow ─────────────────────────────────────────────────────────────

resource "aws_glue_workflow" "pipeline" {
  name        = "wf-cc-pipeline-diario"
  description = "Pipeline event-driven: Bronze→Silver→Gold (40 jobs)"

  default_run_properties = {
    BUCKET_NAME = var.bucket_name
    ENV         = var.environment
  }
}

# ── Glue Jobs — Bronze → Silver (18 jobs) ────────────────────────────────────

locals {
  bronze_jobs = [
    "job-tb-chamada-bronze-to-silver",
    "job-tb-gravacao-chamada-bronze-to-silver",
    "job-tb-ura-navegacao-bronze-to-silver",
    "job-tb-chat-bronze-to-silver",
    "job-tb-mensagem-chat-bronze-to-silver",
    "job-tb-whatsapp-atendimento-bronze-to-silver",
    "job-tb-ticket-bronze-to-silver",
    "job-tb-interacao-ticket-bronze-to-silver",
    "job-tb-campanha-bronze-to-silver",
    "job-tb-discagem-bronze-to-silver",
    "job-tb-cliente-bronze-to-silver",
    "job-tb-endereco-cliente-bronze-to-silver",
    "job-tb-operador-bronze-to-silver",
    "job-tb-skill-operador-bronze-to-silver",
    "job-tb-jornada-operador-bronze-to-silver",
    "job-tb-fila-atendimento-bronze-to-silver",
    "job-tb-avaliacao-qualidade-bronze-to-silver",
    "job-tb-metricas-operacionais-bronze-to-silver",
  ]

  gold_dim_jobs = [
    "job-dim-data-gold",
    "job-dim-cliente-gold",
    "job-dim-operador-gold",
    "job-dim-fila-gold",
    "job-dim-campanha-gold",
    "job-dim-canal-gold",
    "job-dim-skill-gold",
    "job-dim-status-chamada-gold",
    "job-dim-status-ticket-gold",
    "job-dim-categoria-ticket-gold",
    "job-dim-prioridade-ticket-gold",
  ]

  gold_fato_wave1_jobs = [
    "job-fato-chamada-gold",
    "job-fato-chat-gold",
    "job-fato-ticket-gold",
    "job-fato-whatsapp-gold",
    "job-fato-discagem-gold",
    "job-fato-jornada-operador-gold",
    "job-fato-metricas-operacionais-gold",
  ]

  gold_fato_wave2_jobs = [
    "job-fato-qualidade-gold",
    "job-fato-mensagem-chat-gold",
    "job-fato-interacao-ticket-gold",
    "job-fato-ura-navegacao-gold",
  ]

  all_gold_jobs = concat(
    local.gold_dim_jobs,
    local.gold_fato_wave1_jobs,
    local.gold_fato_wave2_jobs
  )
}

# Script base path em S3
locals {
  scripts_base = "s3://${var.bucket_name}/scripts"
}

resource "aws_glue_job" "bronze_to_silver" {
  for_each = toset(local.bronze_jobs)

  name         = each.value
  role_arn     = aws_iam_role.glue.arn
  glue_version = var.glue_version
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers

  command {
    name            = "glueetl"
    script_location = "${local.scripts_base}/bronze_to_silver/${each.value}.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"        = "job-bookmark-enable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"            = "false"
    "--TempDir"                    = "s3://${var.bucket_name}/logs/glue-temp/"
    "--BUCKET_NAME"                = var.bucket_name
    "--ENV"                        = var.environment
    "--conf"                       = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Layer    = "bronze-to-silver"
    Pipeline = "contact-center"
  }
}

resource "aws_glue_job" "silver_to_gold" {
  for_each = toset(local.all_gold_jobs)

  name         = each.value
  role_arn     = aws_iam_role.glue.arn
  glue_version = var.glue_version
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers

  command {
    name            = "glueetl"
    script_location = "${local.scripts_base}/silver_to_gold/${each.value}.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"        = "job-bookmark-enable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                    = "s3://${var.bucket_name}/logs/glue-temp/"
    "--BUCKET_NAME"                = var.bucket_name
    "--ENV"                        = var.environment
    "--conf"                       = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${var.bucket_name}/gold/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Layer    = "silver-to-gold"
    Pipeline = "contact-center"
  }
}

# ── Glue Triggers (encadeamento do workflow) ──────────────────────────────────

# Trigger de início — manual ou agendado
resource "aws_glue_trigger" "start_bronze" {
  name          = "trig-start-bronze"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.pipeline.name

  actions {
    job_name = local.bronze_jobs[0]
  }
}

# Trigger Wave Gold Dims — dispara após todos os Bronze→Silver concluírem
resource "aws_glue_trigger" "start_gold_dims" {
  name          = "trig-start-gold-dims"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name

  predicate {
    logical = "AND"

    dynamic "conditions" {
      for_each = local.bronze_jobs
      content {
        job_name         = conditions.value
        state            = "SUCCEEDED"
        logical_operator = "EQUALS"
      }
    }
  }

  dynamic "actions" {
    for_each = local.gold_dim_jobs
    content {
      job_name = actions.value
    }
  }
}

# Trigger Wave Fatos 1 — dispara após todas as dimensões
resource "aws_glue_trigger" "start_gold_fatos_wave1" {
  name          = "trig-start-gold-fatos-wave1"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name

  predicate {
    logical = "AND"

    dynamic "conditions" {
      for_each = local.gold_dim_jobs
      content {
        job_name         = conditions.value
        state            = "SUCCEEDED"
        logical_operator = "EQUALS"
      }
    }
  }

  dynamic "actions" {
    for_each = local.gold_fato_wave1_jobs
    content {
      job_name = actions.value
    }
  }
}

# Trigger Wave Fatos 2 — dispara após fatos base (dependência cruzada)
resource "aws_glue_trigger" "start_gold_fatos_wave2" {
  name          = "trig-start-gold-fatos-wave2"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name

  predicate {
    logical = "AND"

    dynamic "conditions" {
      for_each = local.gold_fato_wave1_jobs
      content {
        job_name         = conditions.value
        state            = "SUCCEEDED"
        logical_operator = "EQUALS"
      }
    }
  }

  dynamic "actions" {
    for_each = local.gold_fato_wave2_jobs
    content {
      job_name = actions.value
    }
  }
}

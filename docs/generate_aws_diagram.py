"""
Gera o diagrama de arquitetura AWS com ícones oficiais via biblioteca 'diagrams'
Contact Center Data Lakehouse
"""
import os
import sys

# Garante que o Graphviz está no PATH
graphviz_bin = r"C:\Program Files\Graphviz\bin"
if graphviz_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = graphviz_bin + os.pathsep + os.environ.get("PATH", "")

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage      import S3
from diagrams.aws.analytics    import (
    Glue, Athena, KinesisDataStreams, KinesisDataFirehose, EMR
)
from diagrams.aws.compute      import Lambda
from diagrams.aws.database     import Redshift, RDS
from diagrams.aws.integration  import Eventbridge, SNS
from diagrams.aws.management   import Cloudwatch, Cloudtrail
from diagrams.aws.security     import IAM
from diagrams.aws.ml           import Sagemaker          # QuickSight placeholder
from diagrams.aws.general      import General            # fallback
from diagrams.aws.network      import CloudFront         # fallback
from diagrams.onprem.workflow  import Airflow
from diagrams.onprem.iac       import Terraform
from diagrams.onprem.vcs       import Github
from diagrams.onprem.database  import Postgresql

OUT = "docs/architecture_aws_icons"   # .png será adicionado automaticamente

graph_attr = {
    "bgcolor":    "#0D1B2A",
    "fontcolor":  "#FFFFFF",
    "fontname":   "Arial",
    "fontsize":   "14",
    "pad":        "0.6",
    "nodesep":    "0.6",
    "ranksep":    "0.9",
    "splines":    "ortho",
    "rankdir":    "LR",
}

node_attr = {
    "fontcolor": "#C8D6E5",
    "fontname":  "Arial",
    "fontsize":  "11",
}

edge_attr = {
    "color":    "#7F9EBB",
    "fontcolor":"#7F9EBB",
    "fontname": "Arial",
    "fontsize": "9",
}

with Diagram(
    "Contact Center Data Lakehouse — AWS",
    filename=OUT,
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):

    # ── FONTE ────────────────────────────────────────────────────────────
    with Cluster("Fonte de Dados", graph_attr={"bgcolor":"#1C2B39","color":"#34495E","fontcolor":"#AABBCC"}):
        pg = Postgresql("PostgreSQL\n18 tabelas\nWAL CDC")

    # ── INGESTÃO CDC ─────────────────────────────────────────────────────
    with Cluster("Ingestão (CDC)", graph_attr={"bgcolor":"#1C2030","color":"#E7157B","fontcolor":"#E7157B"}):
        dms      = RDS("AWS DMS\n18 tasks")
        kds      = KinesisDataStreams("Kinesis Streams\n18 streams")
        firehose = KinesisDataFirehose("Kinesis Firehose\nParquet+Snappy")
        dms >> kds >> firehose

    # ── BRONZE ───────────────────────────────────────────────────────────
    with Cluster("Bronze Layer", graph_attr={"bgcolor":"#261500","color":"#FF9900","fontcolor":"#FF9900"}):
        s3_bronze   = S3("S3 Bronze\nParquet+Snappy")
        eventbridge = Eventbridge("EventBridge\nS3 PutObject")
        lmb         = Lambda("Lambda\nfn_start_crawler")
        crawler     = Glue("Glue Crawler\n18 crawlers")
        cat_bronze  = Glue("Glue Catalog\ndb_bronze")
        s3_bronze >> eventbridge >> lmb >> crawler >> cat_bronze

    # ── SILVER ───────────────────────────────────────────────────────────
    with Cluster("Silver Layer — Iceberg v2", graph_attr={"bgcolor":"#0A1F14","color":"#2ECC71","fontcolor":"#2ECC71"}):
        glue_silver = Glue("Glue 4.0\n18 jobs PySpark\nBronze→Silver")
        s3_silver   = S3("S3 Silver\nIceberg v2 ACID")
        cat_silver  = Glue("Glue Catalog\ndb_silver")
        glue_silver >> s3_silver >> cat_silver

    # ── GOLD ─────────────────────────────────────────────────────────────
    with Cluster("Gold Layer — Star Schema", graph_attr={"bgcolor":"#201600","color":"#FFD700","fontcolor":"#FFD700"}):
        glue_gold   = Glue("Glue 4.0\n22 jobs PySpark\nSilver→Gold")
        s3_gold     = S3("S3 Gold\n11 dims + 11 fatos")
        cat_gold    = Glue("Glue Catalog\ndb_gold")
        glue_gold >> s3_gold >> cat_gold

    # ── ANALYTICS ────────────────────────────────────────────────────────
    with Cluster("Analytics Layer", graph_attr={"bgcolor":"#00142B","color":"#009BFF","fontcolor":"#009BFF"}):
        athena      = Athena("Amazon Athena\nSQL serverless\n12 KPI queries")
        s3_staging  = S3("S3 Staging\nCSV (UNLOAD)")
        redshift    = Redshift("Redshift Serverless\n8 RPUs · auto-pause\n22 tabelas + 9 views")
        quicksight  = Sagemaker("Amazon QuickSight\n5 datasets SPICE\nDashboards BI")
        emr         = EMR("EMR Serverless\nSpark jobs")
        athena >> Edge(label="UNLOAD") >> s3_staging >> Edge(label="COPY") >> redshift
        athena >> quicksight

    # ── ORQUESTRAÇÃO ─────────────────────────────────────────────────────
    with Cluster("Orquestração", graph_attr={"bgcolor":"#001930","color":"#017CEE","fontcolor":"#017CEE"}):
        airflow = Airflow("Apache Airflow 2.9.3\nDocker Compose\n2 DAGs · 40 jobs")

    # ── GOVERNANÇA ───────────────────────────────────────────────────────
    with Cluster("Governança & Monitoramento", graph_attr={"bgcolor":"#1A0005","color":"#DD344C","fontcolor":"#DD344C"}):
        cw      = Cloudwatch("CloudWatch\nLogs + Alarmes")
        sns     = SNS("SNS\nAlertas")
        ct      = Cloudtrail("CloudTrail\nAuditoria")
        iam     = IAM("IAM Roles\nGlue·Lambda·RS")
        tf      = Terraform("Terraform\nIaC completo")
        gh      = Github("GitHub Actions\nCI/CD 7 jobs")
        cw >> sns

    # ── FLUXO PRINCIPAL ───────────────────────────────────────────────────
    pg          >> Edge(label="WAL",    color="#E7157B", style="bold") >> dms
    firehose    >> Edge(label="→ S3",   color="#FF9900", style="bold") >> s3_bronze
    cat_bronze  >> Edge(label="Trigger",color="#2ECC71", style="bold") >> glue_silver
    cat_silver  >> Edge(label="spark.table()", color="#FFD700", style="bold") >> glue_gold
    cat_gold    >> Edge(label="SELECT", color="#009BFF", style="bold") >> athena

    # Airflow orquestra tudo
    airflow >> Edge(label="Glue jobs", color="#017CEE", style="dashed") >> glue_silver
    airflow >> Edge(label="Trigger",   color="#017CEE", style="dashed") >> redshift

    # CloudWatch monitora jobs
    glue_silver >> Edge(color="#DD344C", style="dashed", label="logs") >> cw
    glue_gold   >> Edge(color="#DD344C", style="dashed", label="logs") >> cw

print(f"OK  Diagrama salvo em: {OUT}.png")

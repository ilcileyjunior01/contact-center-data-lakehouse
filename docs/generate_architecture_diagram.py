"""
Gera o diagrama de arquitetura AWS em PNG
Contact Center Data Lakehouse
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

# ── Cores ──────────────────────────────────────────────────────────────────
BG          = "#0D1B2A"
CARD        = "#142A42"
TEXT_W      = "#FFFFFF"
TEXT_L      = "#C8D6E5"
TEXT_M      = "#7F9EBB"

C_BRONZE    = "#7D4B00"
C_SILVER    = "#1E5A3A"
C_GOLD      = "#7D6000"
C_BLUE      = "#00539B"
C_PINK      = "#E7157B"
C_PURPLE    = "#8C4FFF"
C_GREEN     = "#2ECC71"
C_ORANGE    = "#FF9900"
C_TEAL      = "#00D4AA"
C_YELLOW    = "#FFD700"

# ── Ícones representativos dos serviços AWS (texto/emoji embutido) ────────
ICONS = {
    "S3":          ("S3",       C_ORANGE),
    "DMS":         ("DMS",      C_PINK),
    "Kinesis\nStr":("KDS",      C_PINK),
    "Firehose":    ("KDF",      C_PINK),
    "Lambda":      ("λ",        C_ORANGE),
    "EventBridge": ("EB",       C_PINK),
    "Crawler":     ("srch",       C_PURPLE),
    "Glue\nJobs":  ("⚙",       C_PURPLE),
    "Glue\nCatalog":("CAT",      C_PURPLE),
    "Athena":      ("Athena",   C_PURPLE),
    "Redshift":    ("RS",       C_PURPLE),
    "QuickSight":  ("QS",       C_PURPLE),
    "EMR":         ("EMR",      C_PINK),
    "Airflow":     ("✈",        "#017CEE"),
    "CloudWatch":  ("CW",       C_PINK),
    "SNS":         ("SNS",      C_PINK),
    "CloudTrail":  ("CT",       C_PINK),
    "LakeForm":    ("LF",       C_GREEN),
    "IAM":         ("IAM",      "#DD344C"),
}


def rounded_rect(ax, x, y, w, h, color, alpha=1.0, radius=0.3, zorder=2):
    rect = FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        facecolor=color, edgecolor="none", alpha=alpha, zorder=zorder
    )
    ax.add_patch(rect)
    return rect


def service_box(ax, cx, cy, label, badge, badge_color, w=2.2, h=1.4):
    """Caixa de serviço: badge colorido + nome."""
    bx = cx - w / 2
    by = cy - h / 2
    rounded_rect(ax, bx, by, w, h, CARD, radius=0.2)
    # Badge no topo
    rounded_rect(ax, bx, by + h - 0.35, w, 0.35, badge_color, radius=0.15)
    ax.text(cx, by + h - 0.175, badge,
            ha="center", va="center", fontsize=8, fontweight="bold",
            color=TEXT_W, zorder=4)
    # Label
    ax.text(cx, cy - 0.22, label,
            ha="center", va="center", fontsize=8.5, color=TEXT_L,
            zorder=4, linespacing=1.35)


def layer_box(ax, x, y, w, h, color, title):
    rounded_rect(ax, x, y, w, h, color, alpha=0.18, radius=0.4, zorder=1)
    # Borda colorida
    rect = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0,rounding_size=0.4",
        facecolor="none", edgecolor=color, linewidth=1.5,
        alpha=0.6, zorder=1
    )
    ax.add_patch(rect)
    ax.text(x + w / 2, y + h - 0.25, title,
            ha="center", va="center", fontsize=9, fontweight="bold",
            color=color, zorder=3)


def arrow(ax, x1, y1, x2, y2, color="#AABBCC", lw=1.5, label=""):
    ax.annotate("",
        xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(
            arrowstyle="-|>",
            color=color, lw=lw,
            mutation_scale=12,
        ), zorder=5
    )
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mx, my + 0.12, label,
                ha="center", va="center", fontsize=7, color=color, zorder=6)


# ══════════════════════════════════════════════════════════════════════════════
def build_diagram(out_path="docs/architecture_diagram.png"):

    fig, ax = plt.subplots(figsize=(24, 14))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 24)
    ax.set_ylim(0, 14)
    ax.axis("off")

    # ── Título ────────────────────────────────────────────────────────────
    ax.text(12, 13.5, "Contact Center Data Lakehouse — AWS Architecture",
            ha="center", va="center", fontsize=16, fontweight="bold",
            color=TEXT_W, zorder=10)
    ax.plot([1, 23], [13.1, 13.1], color=C_TEAL, lw=2)

    # ══════════════════════════════════════════════════════════════════════
    # CAMADA: FONTE  (x: 0.3 → 2.8)
    # ══════════════════════════════════════════════════════════════════════
    layer_box(ax, 0.3, 2.0, 2.5, 10.7, "#34495E", "FONTE")
    service_box(ax, 1.55, 10.2, "PostgreSQL\n18 tabelas", "RDS", "#C7131F")
    service_box(ax, 1.55, 8.4,  "WAL CDC\nReplicação lógica", "CDC", "#34495E")
    service_box(ax, 1.55, 6.5,  "s3_data_loader\nCSV → S3 (demo)", "py", C_GREEN)

    # ══════════════════════════════════════════════════════════════════════
    # CAMADA: INGESTÃO  (x: 3.0 → 6.2)
    # ══════════════════════════════════════════════════════════════════════
    layer_box(ax, 3.0, 2.0, 3.2, 10.7, C_PINK, "INGESTÃO (CDC)")
    service_box(ax, 4.0, 10.2, "AWS DMS\n18 tasks",            "DMS",  C_PINK)
    service_box(ax, 5.8, 10.2, "Kinesis\nData Streams",        "KDS",  C_PINK)
    service_box(ax, 4.9, 8.3,  "Kinesis Data\nFirehose",       "KDF",  C_PINK)
    service_box(ax, 4.9, 6.3,  "Parquet + Snappy\n→ S3 Bronze","fmt",  "#7D4B00")

    # Setas ingestão
    arrow(ax, 2.05, 10.2, 3.25, 10.2, C_PINK, label="WAL")
    arrow(ax, 4.78, 10.2, 5.02, 10.2, C_PINK)
    arrow(ax, 4.9, 9.6, 4.9, 9.05, C_PINK)
    arrow(ax, 4.9, 8.02, 4.9, 7.05, C_PINK)

    # ══════════════════════════════════════════════════════════════════════
    # CAMADA: BRONZE  (x: 6.4 → 9.7)
    # ══════════════════════════════════════════════════════════════════════
    layer_box(ax, 6.4, 2.0, 3.3, 10.7, C_ORANGE, "BRONZE")
    service_box(ax, 7.5, 10.2,  "Amazon S3\nBronze Layer",  "S3",  C_ORANGE)
    service_box(ax, 9.1, 10.2,  "Amazon\nEventBridge",      "EB",  C_PINK)
    service_box(ax, 7.5,  8.3,  "AWS Lambda\nfn_start_crawler", "λ",  C_ORANGE)
    service_box(ax, 9.1,  8.3,  "Glue Crawler\n18 crawlers",    "srch", C_PURPLE)
    service_box(ax, 8.3,  6.3,  "Glue Data Catalog\ndb_bronze", "CAT", C_PURPLE)

    arrow(ax, 6.2,  7.5,  6.62,  9.0,  C_ORANGE, label="Parquet")
    arrow(ax, 7.5, 10.2,  8.32, 10.2,  C_ORANGE)
    arrow(ax, 9.1, 10.2,  9.1,   9.05, C_ORANGE)
    arrow(ax, 9.1,  8.3,  8.78,  8.3,  C_ORANGE)
    arrow(ax, 7.5,  8.02, 7.85,  7.05, C_ORANGE)
    arrow(ax, 9.1,  8.02, 8.75,  7.05, C_ORANGE)

    # ══════════════════════════════════════════════════════════════════════
    # CAMADA: SILVER  (x: 9.9 → 13.2)
    # ══════════════════════════════════════════════════════════════════════
    layer_box(ax, 9.9, 2.0, 3.3, 10.7, C_GREEN, "SILVER")
    service_box(ax, 11.0, 10.2, "AWS Glue 4.0\n18 jobs PySpark",  "⚙", C_PURPLE)
    service_box(ax, 12.6, 10.2, "Amazon S3\nSilver / Iceberg v2", "S3", C_ORANGE)
    service_box(ax, 11.0,  8.3, "Dedup CDC\nPII Mask · Quarent.", "QA", C_GREEN)
    service_box(ax, 12.6,  8.3, "MERGE ACID\nHash MD5 · Idem.",   "IDMP", C_TEAL)
    service_box(ax, 11.8,  6.3, "Glue Data Catalog\ndb_silver",   "CAT", C_PURPLE)

    arrow(ax, 9.55, 9.5, 10.22, 10.2, C_GREEN, label="Trigger")
    arrow(ax, 11.78, 10.2, 11.82, 10.2, C_GREEN)
    arrow(ax, 11.0, 10.02, 11.0,  9.05, C_GREEN)
    arrow(ax, 12.6, 10.02, 12.6,  9.05, C_GREEN)
    arrow(ax, 11.0,  8.02, 11.45, 7.05, C_GREEN)
    arrow(ax, 12.6,  8.02, 12.15, 7.05, C_GREEN)

    # ══════════════════════════════════════════════════════════════════════
    # CAMADA: GOLD  (x: 13.4 → 16.7)
    # ══════════════════════════════════════════════════════════════════════
    layer_box(ax, 13.4, 2.0, 3.3, 10.7, C_YELLOW, "GOLD")
    service_box(ax, 14.5, 10.2, "AWS Glue 4.0\n22 jobs PySpark",    "⚙",  C_PURPLE)
    service_box(ax, 16.1, 10.2, "Amazon S3\nGold / Star Schema",     "S3",  C_ORANGE)
    service_box(ax, 14.5,  8.3, "11 Dimensões\nSK · NK · Sentinela", "DIM", C_YELLOW)
    service_box(ax, 16.1,  8.3, "11 Fatos\nMono_inc_id · AQE",       "FAT", C_YELLOW)
    service_box(ax, 15.3,  6.3, "Glue Data Catalog\ndb_gold",        "CAT",  C_PURPLE)

    arrow(ax, 13.1, 6.5, 13.62, 6.5, C_YELLOW, label="spark.table()")
    arrow(ax, 14.5, 10.02, 14.5,  9.05, C_YELLOW)
    arrow(ax, 16.1, 10.02, 16.1,  9.05, C_YELLOW)
    arrow(ax, 14.5,  8.02, 14.9,  7.05, C_YELLOW)
    arrow(ax, 16.1,  8.02, 15.7,  7.05, C_YELLOW)

    # ══════════════════════════════════════════════════════════════════════
    # CAMADA: ANALYTICS  (x: 16.9 → 23.2)
    # ══════════════════════════════════════════════════════════════════════
    layer_box(ax, 16.9, 2.0, 6.8, 10.7, C_BLUE, "ANALYTICS")

    # Athena UNLOAD → S3 Staging → Redshift COPY
    service_box(ax, 18.1, 10.2, "Amazon Athena\nSQL serverless",        "Athena", C_PURPLE)
    service_box(ax, 20.0, 10.2, "Amazon S3\nStaging (CSV)",             "S3",     C_ORANGE)
    service_box(ax, 21.9, 10.2, "Redshift Serverless\n8 RPUs · auto-pause","RS",  C_PURPLE)

    service_box(ax, 18.1,  8.0, "Amazon QuickSight\n5 datasets SPICE",  "QS",    C_PURPLE)
    service_box(ax, 20.0,  8.0, "EMR Serverless\nSpark sem cluster",     "EMR",   C_PINK)
    service_box(ax, 21.9,  8.0, "9 Views KPI\n22 tabelas Gold",          "KPI",   C_TEAL)

    # Notebooks
    service_box(ax, 19.3, 5.9, "Notebooks Jupyter\n5 análises EDA+KPI", "NB",    "#333333")

    arrow(ax, 16.6, 6.5, 17.12, 6.5, C_BLUE, label="SELECT")
    arrow(ax, 18.78, 10.2, 19.22, 10.2, C_BLUE, label="UNLOAD")
    arrow(ax, 20.78, 10.2, 21.22, 10.2, C_BLUE, label="COPY")
    arrow(ax, 21.9,  9.5, 21.9,   8.7,  C_TEAL)
    arrow(ax, 18.1,  9.5, 18.1,   8.7,  C_BLUE)

    # ══════════════════════════════════════════════════════════════════════
    # ORQUESTRAÇÃO (faixa inferior)
    # ══════════════════════════════════════════════════════════════════════
    layer_box(ax, 0.3, 0.2, 16.4, 1.6, "#017CEE", "ORQUESTRAÇÃO")
    ax.text(1.5, 1.05,
            "Apache Airflow 2.9.3  (Docker Compose)",
            ha="left", va="center", fontsize=9, fontweight="bold",
            color="#017CEE", zorder=4)
    ax.text(1.5, 0.62,
            "DAG 1: cc_pipeline_diario  [0 2 * * *]  "
            "→  Wave 1 (18 jobs B→S)  →  Wave 2 (11 dims)  "
            "→  Wave 3 (7 fatos base)  →  Wave 4 (4 fatos dep.)  "
            "→  Trigger DAG 2",
            ha="left", va="center", fontsize=8.5, color=TEXT_L, zorder=4)
    ax.text(1.5, 0.32,
            "DAG 2: cc_carga_redshift  [schedule=None]  "
            "→  TRUNCATE + Athena UNLOAD → S3 staging → Redshift COPY  "
            "|  start_layer: bronze | silver | gold | redshift",
            ha="left", va="center", fontsize=8.5, color=TEXT_M, zorder=4)

    # ══════════════════════════════════════════════════════════════════════
    # GOVERNANÇA & IaC (canto direito inferior)
    # ══════════════════════════════════════════════════════════════════════
    layer_box(ax, 16.9, 0.2, 6.8, 1.6, "#DD344C", "GOVERNANÇA & IaC")

    gov_items = [
        ("CloudWatch\n+ Alarmes", "CW",  C_PINK,   17.7, 0.98),
        ("SNS\nAlertas",          "SNS", C_PINK,   19.0, 0.98),
        ("CloudTrail\nAuditoria", "CT",  C_PINK,   20.3, 0.98),
        ("Lake Formation\nPII ACL","LF",  C_GREEN,  21.6, 0.98),
        ("IAM Roles\nGlue·RS·QS", "IAM", "#DD344C", 22.9, 0.98),
    ]
    for label, badge, color, gx, gy in gov_items:
        service_box(ax, gx, gy, label, badge, color, w=1.8, h=1.4)

    # ══════════════════════════════════════════════════════════════════════
    # LEGENDA
    # ══════════════════════════════════════════════════════════════════════
    legend_items = [
        (C_ORANGE, "Bronze"),
        (C_GREEN,  "Silver (Iceberg v2)"),
        (C_YELLOW, "Gold (Star Schema)"),
        (C_BLUE,   "Analytics"),
        ("#017CEE","Orchestration"),
        ("#DD344C","Governance"),
    ]
    lx, ly = 0.4, 1.95
    ax.text(lx, ly + 0.15, "Legenda:", fontsize=8, color=TEXT_M, va="center")
    for i, (color, label) in enumerate(legend_items):
        lix = lx + i * 2.65 + 0.8
        rounded_rect(ax, lix, ly, 0.3, 0.3, color, radius=0.05)
        ax.text(lix + 0.38, ly + 0.15, label,
                fontsize=8, color=TEXT_L, va="center")

    # Rodapé
    ax.text(12, 0.07,
            "Contact Center Data Lakehouse  ·  Ilciley Junior  ·  "
            "github.com/ilcileyjunior01",
            ha="center", va="center", fontsize=8, color=TEXT_M,
            fontstyle="italic")

    plt.tight_layout(pad=0)
    fig.savefig(out_path, dpi=180, bbox_inches="tight",
                facecolor=BG, edgecolor="none")
    plt.close(fig)
    print(f"OK  Diagrama salvo em: {out_path}")


if __name__ == "__main__":
    build_diagram()

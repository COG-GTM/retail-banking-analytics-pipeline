#!/usr/bin/env python3
"""
Data Lineage & Dependency Chart Generator
==========================================
Generates a visual PNG chart showing the complete data flow
of the Retail Banking Analytics Pipeline.

Usage:
    pip install graphviz
    python generate_lineage_chart.py

Output:
    data_lineage.png  (in the same directory)
"""
from pathlib import Path

try:
    import graphviz
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "graphviz"])
    import graphviz

OUTPUT_DIR = Path(__file__).resolve().parent


def create_lineage_chart():
    """Build the full pipeline lineage as a Graphviz directed graph."""

    dot = graphviz.Digraph(
        "data_lineage",
        format="png",
        engine="dot",
        graph_attr={
            "rankdir": "TB",
            "fontname": "Helvetica",
            "fontsize": "14",
            "label": "Retail Banking Analytics Pipeline — Data Lineage & Dependencies\n"
                     "BTEQ/SAS → PySpark Migration\n",
            "labelloc": "t",
            "labeljust": "c",
            "bgcolor": "white",
            "pad": "0.5",
            "nodesep": "0.6",
            "ranksep": "1.0",
            "dpi": "150",
        },
        node_attr={
            "fontname": "Helvetica",
            "fontsize": "11",
            "style": "filled",
            "shape": "box",
            "penwidth": "1.5",
        },
        edge_attr={
            "fontname": "Helvetica",
            "fontsize": "9",
            "color": "#555555",
            "arrowsize": "0.8",
        },
    )

    # ══════════════════════════════════════════════════════════════════
    # Layer 1: Source Tables (CORE_BANKING_DB / TXN_PROCESSING_DB)
    # ══════════════════════════════════════════════════════════════════
    with dot.subgraph(name="cluster_source") as s:
        s.attr(
            label="SOURCE TABLES  (Read-Only Operational Systems)",
            style="filled",
            color="#E3F2FD",
            fillcolor="#E3F2FD",
            fontcolor="#1565C0",
            fontsize="12",
        )
        source_style = {"fillcolor": "#BBDEFB", "color": "#1565C0"}

        s.node("CUSTOMERS", "CUSTOMERS\n(500 rows)", **source_style)
        s.node("ACCOUNTS", "ACCOUNTS\n(1,251 rows)", **source_style)
        s.node("ADDRESSES", "ADDRESSES\n(1,000 rows)", **source_style)
        s.node("TRANSACTIONS", "TRANSACTIONS\n(80,528 rows)", **source_style)
        s.node("TXN_TYPES", "TRANSACTION_TYPES\n(8 rows)", **source_style)
        s.node("BUREAU_SCORES", "CUSTOMER_BUREAU\n_SCORES (500 rows)", **source_style)

    # ══════════════════════════════════════════════════════════════════
    # Layer 2: BTEQ Staging (ETL_STAGING_DB) — Now PySpark
    # ══════════════════════════════════════════════════════════════════
    with dot.subgraph(name="cluster_staging") as s:
        s.attr(
            label="STAGING LAYER  (BTEQ → PySpark)",
            style="filled",
            color="#E8F5E9",
            fillcolor="#E8F5E9",
            fontcolor="#2E7D32",
            fontsize="12",
        )
        stg_style = {"fillcolor": "#C8E6C9", "color": "#2E7D32", "shape": "box"}

        s.node(
            "STG_CUST360",
            "stg_customer_360.py\n━━━━━━━━━━━━━━\nSTG_CUSTOMER_360\n(478 rows)\n"
            "━━━━━━━━━━━━━━\nJOINs, QUALIFY ROW_NUMBER\n"
            "age, tenure, credit util",
            **stg_style,
        )
        s.node(
            "STG_TXN",
            "stg_txn_summary.py\n━━━━━━━━━━━━━━\nSTG_TXN_SUMMARY\n(1,251 rows)\n"
            "━━━━━━━━━━━━━━\nSUM/AVG/COUNT, channel mix %\n"
            "merchant diversity, NULLIFZERO",
            **stg_style,
        )
        s.node(
            "STG_RISK",
            "stg_risk_factors.py\n━━━━━━━━━━━━━━\nSTG_RISK_FACTORS\n(478 rows)\n"
            "━━━━━━━━━━━━━━\nSTDDEV_POP, velocity,\n"
            "work tables, multi-pass joins",
            **stg_style,
        )

    # ══════════════════════════════════════════════════════════════════
    # Layer 3: SAS Analytics (DATA_PRODUCTS_DB) — Now PySpark+MLlib
    # ══════════════════════════════════════════════════════════════════
    with dot.subgraph(name="cluster_analytics") as s:
        s.attr(
            label="ANALYTICS LAYER  (SAS → PySpark MLlib)",
            style="filled",
            color="#FFF3E0",
            fillcolor="#FFF3E0",
            fontcolor="#E65100",
            fontsize="12",
        )
        sas_style = {"fillcolor": "#FFE0B2", "color": "#E65100"}

        s.node(
            "SEG",
            "customer_segments.py\n━━━━━━━━━━━━━━\nCUSTOMER_SEGMENTS\n(407 rows)\n"
            "━━━━━━━━━━━━━━\nPROC FASTCLUS → KMeans(k=5)\n"
            "PROC STDIZE → StandardScaler",
            **sas_style,
        )
        s.node(
            "TXN_ANAL",
            "txn_analytics.py\n━━━━━━━━━━━━━━\nTRANSACTION_ANALYTICS\n(500 rows)\n"
            "━━━━━━━━━━━━━━\nPROC RANK → percent_rank()\n"
            "PROC MEANS → approxQuantile()\n"
            "IQR anomaly detection",
            **sas_style,
        )
        s.node(
            "RISK_SCORE",
            "risk_scoring.py\n━━━━━━━━━━━━━━\nCUSTOMER_RISK_SCORES\n(407 rows)\n"
            "━━━━━━━━━━━━━━\nPROC LOGISTIC → LogisticRegr.\n"
            "composite scoring, tier classification",
            **sas_style,
        )

    # ══════════════════════════════════════════════════════════════════
    # Layer 4: Golden Record (DATA_PRODUCTS_DB)
    # ══════════════════════════════════════════════════════════════════
    with dot.subgraph(name="cluster_golden") as s:
        s.attr(
            label="GOLDEN RECORD  (4-Way Merge → PySpark Join)",
            style="filled",
            color="#F3E5F5",
            fillcolor="#F3E5F5",
            fontcolor="#6A1B9A",
            fontsize="12",
        )
        gold_style = {"fillcolor": "#E1BEE7", "color": "#6A1B9A"}

        s.node(
            "MASTER",
            "data_products.py\n━━━━━━━━━━━━━━━━\nCUSTOMER_MASTER_PROFILE\n(407 rows)\n"
            "━━━━━━━━━━━━━━━━\nDATA step MERGE → left join()\n"
            "coalesce() for defaults\n"
            "completeness report",
            **gold_style,
        )

    # ══════════════════════════════════════════════════════════════════
    # Edges: Source → Staging
    # ══════════════════════════════════════════════════════════════════
    # STG_CUSTOMER_360 sources
    dot.edge("CUSTOMERS", "STG_CUST360", label="  customer_id")
    dot.edge("ACCOUNTS", "STG_CUST360", label="  account aggs")
    dot.edge("ADDRESSES", "STG_CUST360", label="  primary addr")

    # STG_TXN_SUMMARY sources
    dot.edge("TRANSACTIONS", "STG_TXN", label="  txn details")
    dot.edge("TXN_TYPES", "STG_TXN", label="  type lookup")
    dot.edge("ACCOUNTS", "STG_TXN", label="  customer_id")

    # STG_RISK_FACTORS sources
    dot.edge("TRANSACTIONS", "STG_RISK", label="  daily balance")
    dot.edge("ACCOUNTS", "STG_RISK", label="  credit limit")
    dot.edge("CUSTOMERS", "STG_RISK", label="  status filter")
    dot.edge("BUREAU_SCORES", "STG_RISK", label="  credit score")

    # ══════════════════════════════════════════════════════════════════
    # Edges: Staging → Analytics
    # ══════════════════════════════════════════════════════════════════
    dot.edge("STG_CUST360", "SEG", label="  features", color="#2E7D32", penwidth="2")
    dot.edge("STG_TXN", "TXN_ANAL", label="  txn metrics", color="#2E7D32", penwidth="2")
    dot.edge("STG_RISK", "RISK_SCORE", label="  risk features", color="#2E7D32", penwidth="2")
    dot.edge("STG_CUST360", "RISK_SCORE", label="  tenure, accts", color="#2E7D32", style="dashed")

    # ══════════════════════════════════════════════════════════════════
    # Edges: Analytics → Golden Record
    # ══════════════════════════════════════════════════════════════════
    dot.edge("SEG", "MASTER", label="  segment", color="#E65100", penwidth="2")
    dot.edge("TXN_ANAL", "MASTER", label="  analytics", color="#E65100", penwidth="2")
    dot.edge("RISK_SCORE", "MASTER", label="  risk score", color="#E65100", penwidth="2")
    dot.edge("STG_CUST360", "MASTER", label="  base record", color="#2E7D32", style="dashed")

    # ══════════════════════════════════════════════════════════════════
    # Parallelization annotations
    # ══════════════════════════════════════════════════════════════════
    dot.node(
        "parallel_note",
        "Parallelization Opportunities:\n"
        "- Staging scripts (01, 02, 03) can run in parallel\n"
        "- customer_segments + txn_analytics can run in parallel\n"
        "- risk_scoring waits for stg_customer_360 + stg_risk_factors\n"
        "- data_products waits for ALL upstream",
        shape="note",
        fillcolor="#FFFDE7",
        color="#F9A825",
        fontsize="10",
    )

    # Render
    output_path = OUTPUT_DIR / "data_lineage"
    dot.render(str(output_path), cleanup=True)
    print(f"Data lineage chart saved to: {output_path}.png")
    return str(output_path) + ".png"


if __name__ == "__main__":
    create_lineage_chart()

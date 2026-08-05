"""Instrumentation for the end-to-end run.

Everything the HTML report shows is produced here, from the tables the run actually wrote:
per-job timings and statuses, per-table validation results, column profiles, row samples and the
business aggregates the legacy jobs printed to their listings. The lineage map is the only static
content — it records which legacy script produced each hop and the business rule it applied, so
the report can show source -> staging -> data product forward engineering.
"""

from __future__ import annotations

import json
import logging
import platform
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType, IntegralType, NumericType

from common import schemas
from common.config import PRODUCTION_MIN_ROWS, PipelineConfig
from common.io import DataIO
from common.schemas import TableSpec
from common.validation import validate_table
from orchestration.pipeline import PIPELINE, PipelineRun

LOGGER = logging.getLogger(__name__)

#: Tables profiled in the report, in pipeline order.
PROFILED_SPECS: tuple[TableSpec, ...] = (
    schemas.CUSTOMERS,
    schemas.ACCOUNTS,
    schemas.ADDRESSES,
    schemas.CUSTOMER_BUREAU_SCORES,
    schemas.TRANSACTIONS,
    schemas.TRANSACTION_TYPES,
    schemas.STG_CUSTOMER_360,
    schemas.STG_TXN_SUMMARY,
    schemas.STG_RISK_FACTORS,
    schemas.CUSTOMER_SEGMENTS,
    schemas.TRANSACTION_ANALYTICS,
    schemas.CUSTOMER_RISK_SCORES,
    schemas.CUSTOMER_MASTER_PROFILE,
)

#: The validation arguments each job passes to ``%validate_table``, re-run against the
#: DB-resident output so the report's checks are not the in-memory ones the job already did.
VALIDATION_ARGUMENTS: dict[str, dict[str, tuple[str, ...]]] = {
    "ETL_STAGING_DB.STG_CUSTOMER_360": {
        "key_cols": ("CUSTOMER_ID",),
        "not_null": ("CUSTOMER_ID",),
    },
    "ETL_STAGING_DB.STG_TXN_SUMMARY": {
        "key_cols": ("ACCOUNT_ID",),
        "not_null": ("ACCOUNT_ID", "CUSTOMER_ID"),
    },
    "ETL_STAGING_DB.STG_RISK_FACTORS": {
        "key_cols": ("CUSTOMER_ID",),
        "not_null": ("CUSTOMER_ID",),
    },
    "DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS": {
        "key_cols": ("CUSTOMER_ID",),
        "not_null": ("CUSTOMER_ID", "SEGMENT_NAME", "SEGMENT_ID"),
    },
    "DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS": {
        "key_cols": ("CUSTOMER_ID",),
        "not_null": ("CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS"),
    },
    "DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES": {
        "key_cols": ("CUSTOMER_ID",),
        "not_null": ("CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER"),
    },
    "DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE": {
        "key_cols": ("CUSTOMER_ID",),
        "not_null": ("CUSTOMER_ID", "FULL_NAME", "CUSTOMER_STATUS"),
    },
}


@dataclass(frozen=True)
class LineageEdge:
    """One forward-engineering hop: which job read what, wrote what, under which rule."""

    job: str
    legacy_source: str
    inputs: tuple[str, ...]
    output: str
    business_rule: str


LINEAGE: tuple[LineageEdge, ...] = (
    LineageEdge(
        job="01_stg_customer_360",
        legacy_source="bteq/01_stg_customer_360.bteq",
        inputs=(
            "CORE_BANKING_DB.CUSTOMERS",
            "CORE_BANKING_DB.ADDRESSES",
            "CORE_BANKING_DB.ACCOUNTS",
        ),
        output="ETL_STAGING_DB.STG_CUSTOMER_360",
        business_rule=(
            "Active and inactive customers only (status A/I). One primary HOME address per "
            "customer, latest unexpired by VALID_FROM. Account counts, balances and product "
            "flags aggregated from non-closed accounts; credit utilisation = card balance / "
            "card limit; age and tenure derived from the run date."
        ),
    ),
    LineageEdge(
        job="02_stg_txn_summary",
        legacy_source="bteq/02_stg_txn_summary.bteq",
        inputs=(
            "TXN_PROCESSING_DB.TRANSACTIONS",
            "TXN_PROCESSING_DB.TRANSACTION_TYPES",
            "CORE_BANKING_DB.ACCOUNTS",
        ),
        output="ETL_STAGING_DB.STG_TXN_SUMMARY",
        business_rule=(
            "Posted transactions over the LOOKBACK_MONTHS window, summarised per account: "
            "debit/credit counts and amounts, channel mix percentages, fees, and the top "
            "merchant category by spend (QUALIFY ROW_NUMBER = 1)."
        ),
    ),
    LineageEdge(
        job="03_stg_risk_factors",
        legacy_source="bteq/03_stg_risk_factors.bteq",
        inputs=(
            "CORE_BANKING_DB.CUSTOMERS",
            "CORE_BANKING_DB.ACCOUNTS",
            "CORE_BANKING_DB.CUSTOMER_BUREAU_SCORES",
            "TXN_PROCESSING_DB.TRANSACTIONS",
        ),
        output="ETL_STAGING_DB.STG_RISK_FACTORS",
        business_rule=(
            "Per-customer risk factors: 30/90-day average daily balances and volatility from a "
            "three-month daily balance work table, 24-month payment history, latest bureau "
            "score, and counts of large withdrawals, NSF events, high-risk merchant categories "
            "and new merchants (correlated NOT IN, ported as a left anti-join)."
        ),
    ),
    LineageEdge(
        job="01_sas_customer_segments",
        legacy_source="sas/01_sas_customer_segments.sas",
        inputs=("ETL_STAGING_DB.STG_CUSTOMER_360",),
        output="DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS",
        business_rule=(
            "Active customers standardised on six features and clustered into five segments "
            "(PROC FASTCLUS -> Spark ML KMeans), labelled by descending average balance; "
            "lifetime value, engagement, tenure/age/balance buckets and cross-sell, upsell and "
            "retention-risk flags."
        ),
    ),
    LineageEdge(
        job="02_sas_txn_analytics",
        legacy_source="sas/02_sas_txn_analytics.sas",
        inputs=("ETL_STAGING_DB.STG_TXN_SUMMARY",),
        output="DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS",
        business_rule=(
            "Account summaries rolled up per customer for the reporting period: spend "
            "percentile (PROC RANK groups=100 -> ntile), anomaly flag at median + 3*IQR, "
            "digital transaction share, revenue contribution (fees + 2% of debit volume)."
        ),
    ),
    LineageEdge(
        job="03_sas_risk_scoring",
        legacy_source="sas/03_sas_risk_scoring.sas",
        inputs=("ETL_STAGING_DB.STG_RISK_FACTORS", "ETL_STAGING_DB.STG_CUSTOMER_360"),
        output="DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES",
        business_rule=(
            "Composite risk score = 0.30*credit + 0.25*behaviour + 0.15*velocity + "
            "0.20*(100-bureau) + 0.10*(100-payment history), tiered LOW/MODERATE/ELEVATED/"
            "HIGH/CRITICAL at 20/40/60/80, with probability of default from a stepwise logistic "
            "regression and the top two risk drivers."
        ),
    ),
    LineageEdge(
        job="04_sas_data_products",
        legacy_source="sas/04_sas_data_products.sas",
        inputs=(
            "ETL_STAGING_DB.STG_CUSTOMER_360",
            "DATA_PRODUCTS_DB.CUSTOMER_SEGMENTS",
            "DATA_PRODUCTS_DB.TRANSACTION_ANALYTICS",
            "DATA_PRODUCTS_DB.CUSTOMER_RISK_SCORES",
        ),
        output="DATA_PRODUCTS_DB.CUSTOMER_MASTER_PROFILE",
        business_rule=(
            "Golden record: active customers from staging merged with all three data products, "
            "defaulting UNCLASSIFIED segment, zeroed transaction metrics and UNKNOWN risk tier "
            "where a component is missing."
        ),
    ),
)


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def profile_table(df: DataFrame, spec: TableSpec, *, sample_rows: int = 5) -> dict[str, object]:
    """Row count, per-column profile and a row sample, in one pass per statistic."""

    row_count = df.count()
    aggregates: list[object] = []
    for column in spec.columns:
        name = column.name
        aggregates.append(F.sum(F.col(name).isNull().cast("long")).alias(f"{name}__nulls"))
        aggregates.append(F.approx_count_distinct(F.col(name)).alias(f"{name}__distinct"))
        if isinstance(column.dtype, NumericType):
            aggregates.append(F.min(name).alias(f"{name}__min"))
            aggregates.append(F.max(name).alias(f"{name}__max"))
            aggregates.append(F.round(F.avg(F.col(name).cast("double")), 4).alias(f"{name}__mean"))

    stats = df.agg(*aggregates).collect()[0].asDict() if row_count else {}
    columns = []
    for column in spec.columns:
        name = column.name
        nulls = int(stats.get(f"{name}__nulls") or 0)
        columns.append(
            {
                "name": name,
                "type": column.dtype.simpleString(),
                "nullable": column.nullable,
                "default": column.default,
                "nulls": nulls,
                "null_pct": round(100.0 * nulls / row_count, 2) if row_count else 0.0,
                "distinct": int(stats.get(f"{name}__distinct") or 0),
                "min": _json_safe(stats.get(f"{name}__min")),
                "max": _json_safe(stats.get(f"{name}__max")),
                "mean": _json_safe(stats.get(f"{name}__mean")),
            }
        )

    sample = [
        {key: _json_safe(value) for key, value in row.asDict().items()}
        for row in df.limit(sample_rows).collect()
    ]
    return {
        "table": spec.qualified_name,
        "source_ddl": spec.source or "inferred",
        "inferred": spec.inferred,
        "primary_index": list(spec.primary_index),
        "partition_by": list(spec.partition_by),
        "row_count": row_count,
        "columns": columns,
        "sample": sample,
    }


def validate_persisted(df: DataFrame, spec: TableSpec, *, min_rows: int) -> dict[str, object]:
    """Re-run the legacy ``%validate_table`` checks against the DB-resident table."""

    arguments = VALIDATION_ARGUMENTS.get(spec.qualified_name, {})
    result = validate_table(
        df,
        table=spec.qualified_name,
        key_cols=arguments.get("key_cols", ()),
        not_null=arguments.get("not_null", ()),
        min_rows=min_rows,
    )
    return result.as_dict()


def _numeric_columns(spec: TableSpec) -> tuple[str, ...]:
    return tuple(
        column.name
        for column in spec.columns
        if isinstance(column.dtype, (DecimalType, DoubleType, IntegralType))
    )


def business_insights(io: DataIO) -> dict[str, object]:
    """The aggregates the legacy jobs printed to their listings, from the persisted outputs."""

    insights: dict[str, object] = {}

    if io.table_exists("DATA_PRODUCTS_DB", "CUSTOMER_SEGMENTS"):
        segments = io.read_table("DATA_PRODUCTS_DB", "CUSTOMER_SEGMENTS")
        insights["segment_distribution"] = [
            {key: _json_safe(value) for key, value in row.asDict().items()}
            for row in segments.groupBy("SEGMENT_NAME")
            .agg(
                F.count("*").alias("CUSTOMERS"),
                F.round(F.avg("LIFETIME_VALUE_SCORE"), 2).alias("AVG_LTV"),
                F.round(F.avg("ENGAGEMENT_SCORE"), 2).alias("AVG_ENGAGEMENT"),
                F.sum((F.col("CROSS_SELL_FLAG") == "Y").cast("long")).alias("CROSS_SELL"),
                F.sum((F.col("RETENTION_RISK_FLAG") == "Y").cast("long")).alias("RETENTION_RISK"),
            )
            .orderBy(F.col("CUSTOMERS").desc())
            .collect()
        ]

    if io.table_exists("DATA_PRODUCTS_DB", "CUSTOMER_RISK_SCORES"):
        risk = io.read_table("DATA_PRODUCTS_DB", "CUSTOMER_RISK_SCORES")
        insights["risk_tier_distribution"] = [
            {key: _json_safe(value) for key, value in row.asDict().items()}
            for row in risk.groupBy("RISK_TIER")
            .agg(
                F.count("*").alias("CUSTOMERS"),
                F.round(F.avg("COMPOSITE_RISK_SCORE"), 2).alias("AVG_SCORE"),
                F.sum((F.col("WATCH_LIST_FLAG") == "Y").cast("long")).alias("WATCH_LIST"),
                F.sum((F.col("REVIEW_REQUIRED_FLAG") == "Y").cast("long")).alias("REVIEW"),
            )
            .orderBy(F.col("AVG_SCORE").desc())
            .collect()
        ]
        insights["risk_drivers"] = [
            {key: _json_safe(value) for key, value in row.asDict().items()}
            for row in risk.groupBy("PRIMARY_RISK_DRIVER")
            .agg(F.count("*").alias("CUSTOMERS"))
            .orderBy(F.col("CUSTOMERS").desc())
            .collect()
        ]

    if io.table_exists("DATA_PRODUCTS_DB", "TRANSACTION_ANALYTICS"):
        txn = io.read_table("DATA_PRODUCTS_DB", "TRANSACTION_ANALYTICS")
        row = txn.agg(
            F.count("*").alias("CUSTOMERS"),
            F.sum("TOTAL_TRANSACTIONS").alias("TRANSACTIONS"),
            F.round(F.sum("TOTAL_DEBIT_AMT"), 2).alias("DEBIT_VOLUME"),
            F.round(F.sum("REVENUE_CONTRIBUTION"), 2).alias("REVENUE"),
            F.round(F.avg("DIGITAL_TXN_PCT"), 2).alias("AVG_DIGITAL_PCT"),
            F.sum((F.col("ANOMALY_FLAG") == "Y").cast("long")).alias("ANOMALIES"),
        ).collect()[0]
        insights["transaction_summary"] = {
            key: _json_safe(value) for key, value in row.asDict().items()
        }

    if io.table_exists("DATA_PRODUCTS_DB", "CUSTOMER_MASTER_PROFILE"):
        master = io.read_table("DATA_PRODUCTS_DB", "CUSTOMER_MASTER_PROFILE")
        row = master.agg(
            F.count("*").alias("TOTAL"),
            F.sum((F.col("SEGMENT_NAME") != "UNCLASSIFIED").cast("long")).alias("HAS_SEGMENT"),
            F.sum((F.col("MONTHLY_TRANSACTIONS") > 0).cast("long")).alias("HAS_TXN"),
            F.sum((F.col("RISK_TIER") != "UNKNOWN").cast("long")).alias("HAS_RISK_SCORE"),
            F.sum((F.col("CROSS_SELL_FLAG") == "Y").cast("long")).alias("CROSS_SELL_ELIGIBLE"),
            F.sum((F.col("UPSELL_FLAG") == "Y").cast("long")).alias("UPSELL_ELIGIBLE"),
            F.sum((F.col("RETENTION_RISK_FLAG") == "Y").cast("long")).alias("RETENTION_AT_RISK"),
            F.sum((F.col("WATCH_LIST_FLAG") == "Y").cast("long")).alias("ON_WATCH_LIST"),
        ).collect()[0]
        insights["completeness"] = {key: _json_safe(value) for key, value in row.asDict().items()}

    return insights


def audit_trail(io: DataIO, *, limit: int = 200) -> list[dict[str, object]]:
    """The persisted ``PIPELINE_AUDIT`` rows, read back out of the database."""

    if not io.table_exists("ETL_STAGING_DB", "PIPELINE_AUDIT"):
        return []
    audit = io.read_table("ETL_STAGING_DB", "PIPELINE_AUDIT").orderBy("LOG_TS").limit(limit)
    return [
        {key: _json_safe(value) for key, value in row.asDict().items()} for row in audit.collect()
    ]


def deployment_topology(
    spark: SparkSession, config: PipelineConfig, *, jdbc_url: str, schema_map: dict[str, str]
) -> dict[str, object]:
    return {
        "engine": "PySpark",
        "spark_version": spark.version,
        "python_version": platform.python_version(),
        "java_home": spark.conf.get("spark.driverEnv.JAVA_HOME", None) or "",
        "master": spark.sparkContext.master,
        "database": "PostgreSQL",
        "jdbc_url": jdbc_url,
        "schema_map": schema_map,
        "run_date": config.run_date_str,
        "run_timestamp": config.run_timestamp,
        "lookback_months": config.lookback_months,
        "min_rows": config.min_rows,
        "min_rows_production": PRODUCTION_MIN_ROWS,
        "risk_score_threshold": config.risk_score_threshold,
        "spark_conf": {
            key: spark.conf.get(key, "")
            for key in (
                "spark.sql.adaptive.enabled",
                "spark.sql.adaptive.skewJoin.enabled",
                "spark.sql.autoBroadcastJoinThreshold",
                "spark.sql.shuffle.partitions",
            )
        },
    }


def scale_recommendations(run: PipelineRun, row_counts: dict[str, int]) -> list[dict[str, object]]:
    """Extrapolations from *this* run's measurements to the 10M-customer target."""

    customers = row_counts.get("ETL_STAGING_DB.STG_CUSTOMER_360", 0)
    transactions = row_counts.get("TXN_PROCESSING_DB.TRANSACTIONS", 0)
    factor = 10_000_000 / customers if customers else 0.0
    recommendations = [
        {
            "topic": "Measured scale",
            "detail": (
                f"This run processed {customers:,} customers and {transactions:,} transactions "
                f"in {run.elapsed_seconds:.1f}s wall clock on a single local executor."
            ),
        },
        {
            "topic": "Linear extrapolation to 10M customers",
            "detail": (
                f"The target volume is {factor:,.0f}x this run. Wall clock does not scale "
                "linearly on a cluster, but the shuffle volume does: budget the transaction "
                f"table at roughly {int(transactions * factor):,} rows."
            ),
        },
        {
            "topic": "Partitioning",
            "detail": (
                "Read the transaction table by date slice (each job only needs its lookback "
                "window) and keep TRANSACTION_ANALYTICS partitioned by REPORTING_PERIOD as the "
                "DDL requires, so a period reload rewrites one partition."
            ),
        },
        {
            "topic": "Joins",
            "detail": (
                "TRANSACTION_TYPES (8 rows) and the other dimensions are broadcast; AQE skew "
                "join handling is enabled for the ACCOUNT_ID join on transactions, which is the "
                "only key with meaningful skew at target volume."
            ),
        },
        {
            "topic": "Legacy baseline",
            "detail": (
                "The legacy baselines are ~20 min for the BTEQ phase and ~40 min for the SAS "
                "phase at production volume; the performance tier asserts the ported jobs stay "
                "within those budgets on scaled synthetic data."
            ),
        },
    ]
    return recommendations


def collect_metrics(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    run: PipelineRun,
    *,
    jdbc_url: str = "",
    schema_map: dict[str, str] | None = None,
    specs: Sequence[TableSpec] = PROFILED_SPECS,
    sample_rows: int = 5,
) -> dict[str, object]:
    """Everything the report renders, gathered from the tables this run wrote."""

    tables: list[dict[str, object]] = []
    row_counts: dict[str, int] = {}
    validations: list[dict[str, object]] = []
    for spec in specs:
        if not io.table_exists(spec.database, spec.name):
            LOGGER.warning("%s is not materialised; skipping profile", spec.qualified_name)
            continue
        df = io.read_spec(spec).cache()
        profile = profile_table(df, spec, sample_rows=sample_rows)
        tables.append(profile)
        row_counts[spec.qualified_name] = int(profile["row_count"])
        if spec.qualified_name in VALIDATION_ARGUMENTS:
            validations.append(validate_persisted(df, spec, min_rows=config.min_rows))
        df.unpersist()

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "topology": deployment_topology(
            spark, config, jdbc_url=jdbc_url, schema_map=schema_map or {}
        ),
        "run": run.as_dict(),
        "jobs": [
            {
                **result.as_dict(),
                "legacy_source": next(
                    (node.legacy_source for node in PIPELINE if node.name == result.job_name), ""
                ),
            }
            for result in run.results
        ],
        "lineage": [asdict(edge) for edge in LINEAGE],
        "tables": tables,
        "row_counts": row_counts,
        "validations": validations,
        "audit_trail": audit_trail(io),
        "insights": business_insights(io),
        "scale_recommendations": scale_recommendations(run, row_counts),
    }


def write_metrics(metrics: dict[str, object], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(metrics, indent=2, default=str), encoding="utf-8")
    LOGGER.info("metrics written to %s", target)
    return target

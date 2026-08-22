"""Runtime configuration for the Synapse Spark risk scoring job (TICKET-08).

Every value that used to live in ``config/pipeline_config.cfg`` or as a SAS macro
variable in ``sas/03_sas_risk_scoring.sas`` is an injected parameter here: it can
be supplied as a Synapse pipeline argument (``--risk-score-threshold``) or as an
environment variable (``RISK_SCORE_THRESHOLD``). Nothing is hardcoded in the
scoring logic.
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from dataclasses import dataclass, field

MODEL_VERSION = "RISK_V4.0"

CANDIDATE_FEATURES: tuple[str, ...] = (
    "BUREAU_SCORE_NORM",
    "CREDIT_UTIL_RATIO",
    "PAYMENT_ONTIME_PCT",
    "BALANCE_VOLATILITY",
    "VELOCITY_RATIO",
    "ACCOUNT_OVERDRAFT_CNT",
    "LARGE_WITHDRAWAL_CNT",
    "HIGH_RISK_MERCHANT_CNT",
    "TENURE_MONTHS",
)


@dataclass(frozen=True)
class SnowflakeOptions:
    """Connection options for the Snowflake Spark connector.

    Database/schema/role naming follows the objects created by TICKET-01 and
    TICKET-02; this job never creates them.
    """

    url: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    staging_schema: str
    data_product_schema: str

    def reader_options(self, schema: str) -> dict[str, str]:
        return {
            "sfUrl": self.url,
            "sfUser": self.user,
            "sfPassword": self.password,
            "sfRole": self.role,
            "sfWarehouse": self.warehouse,
            "sfDatabase": self.database,
            "sfSchema": schema,
        }


@dataclass(frozen=True)
class RiskScoringConfig:
    """All parameters consumed by the risk scoring job."""

    snowflake: SnowflakeOptions
    risk_score_threshold: int
    model_version: str = MODEL_VERSION
    slentry: float = 0.10
    slstay: float = 0.05
    min_rows: int = 1000
    default_bureau_score: int = 680
    candidate_features: Sequence[str] = field(default=CANDIDATE_FEATURES)
    model_audit_path: str | None = None
    dry_run: bool = False


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise KeyError(f"Required environment variable {name} is not set")
    return value


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Synapse Spark port of sas/03_sas_risk_scoring.sas (TICKET-08)"
    )
    parser.add_argument("--risk-score-threshold", type=int, default=None)
    parser.add_argument("--model-version", default=MODEL_VERSION)
    parser.add_argument("--slentry", type=float, default=0.10)
    parser.add_argument("--slstay", type=float, default=0.05)
    parser.add_argument("--min-rows", type=int, default=1000)
    parser.add_argument("--default-bureau-score", type=int, default=680)
    parser.add_argument("--snowflake-database", default=None)
    parser.add_argument("--snowflake-staging-schema", default=None)
    parser.add_argument("--snowflake-data-product-schema", default=None)
    parser.add_argument(
        "--model-audit-path",
        default=None,
        help="ADLS/abfss location where model coefficients are persisted per run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run scoring and validation without publishing to Snowflake",
    )
    return parser


def config_from_args(argv: Sequence[str] | None = None) -> RiskScoringConfig:
    """Resolve configuration from CLI arguments, falling back to the environment."""

    args = build_arg_parser().parse_args(argv)

    threshold = args.risk_score_threshold
    if threshold is None:
        threshold = int(_env("RISK_SCORE_THRESHOLD"))

    snowflake = SnowflakeOptions(
        url=_env("SNOWFLAKE_URL"),
        user=_env("SNOWFLAKE_USER"),
        password=_env("SNOWFLAKE_PASSWORD"),
        role=_env("SNOWFLAKE_ROLE"),
        warehouse=_env("SNOWFLAKE_WAREHOUSE"),
        database=args.snowflake_database or _env("SNOWFLAKE_DATABASE"),
        staging_schema=args.snowflake_staging_schema
        or _env("SNOWFLAKE_STAGING_SCHEMA", "ETL_STAGING"),
        data_product_schema=args.snowflake_data_product_schema
        or _env("SNOWFLAKE_DATA_PRODUCT_SCHEMA", "DATA_PRODUCTS"),
    )

    return RiskScoringConfig(
        snowflake=snowflake,
        risk_score_threshold=threshold,
        model_version=args.model_version,
        slentry=args.slentry,
        slstay=args.slstay,
        min_rows=args.min_rows,
        default_bureau_score=args.default_bureau_score,
        model_audit_path=args.model_audit_path or os.environ.get("RISK_MODEL_AUDIT_PATH"),
        dry_run=args.dry_run,
    )

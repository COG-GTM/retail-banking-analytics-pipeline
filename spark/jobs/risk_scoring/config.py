"""Configuration for the risk scoring Synapse Spark job (migrated from sas/03_sas_risk_scoring.sas).

Every value that was a SAS macro variable or a hardcoded literal in the SAS program is
exposed here as an injectable parameter. Defaults reproduce the SAS behaviour exactly so
that a run without overrides reconciles with the legacy output.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, replace
from typing import Mapping, Sequence

DEFAULT_CANDIDATE_FEATURES: tuple[str, ...] = (
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

DEFAULT_COMPOSITE_WEIGHTS: Mapping[str, float] = {
    "CREDIT_RISK_COMPONENT": 0.30,
    "BEHAVIOUR_RISK_COMPONENT": 0.25,
    "VELOCITY_RISK_COMPONENT": 0.15,
    "INVERSE_BUREAU_SCORE_COMPONENT": 0.20,
    "INVERSE_PAYMENT_HISTORY_COMPONENT": 0.10,
}

# (exclusive upper bound of the composite score, tier label); the last tier is the catch-all.
DEFAULT_TIER_BOUNDARIES: tuple[tuple[float, str], ...] = (
    (20.0, "LOW"),
    (40.0, "MODERATE"),
    (60.0, "ELEVATED"),
    (80.0, "HIGH"),
)
DEFAULT_TOP_TIER = "CRITICAL"


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return default if raw is None or raw.strip() == "" else float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return default if raw is None or raw.strip() == "" else int(raw)


@dataclass(frozen=True)
class RiskScoringParams:
    """Business parameters of the risk scoring job."""

    model_version: str = "RISK_V4.0"

    # RISK_SCORE_THRESHOLD from config/pipeline_config.cfg (bureau score scale, currently 700).
    # In SAS it was read via %sysget into &RISK_THRESHOLD; it is injected here and reported as a
    # run-level monitoring metric (share of the cohort scoring below the bureau threshold).
    risk_score_threshold: float = 700.0

    # Bureau score normalisation and imputation (SAS STEP 2).
    bureau_score_floor: float = 300.0
    bureau_score_ceiling: float = 850.0
    bureau_score_imputed: float = 680.0

    # Binary target used to train the model: PAYMENT_LATE_CNT > default_flag_late_cnt.
    default_flag_late_cnt: int = 2

    # Stepwise selection significance levels (PROC LOGISTIC slentry / slstay).
    slentry: float = 0.10
    slstay: float = 0.05
    max_iter: int = 100
    seed: int = 42

    # When the cohort cannot support a fitted model (single-level target, or no candidate reaching
    # slentry) fall back to an intercept-only model instead of failing the run.
    allow_intercept_only_model: bool = False

    candidate_features: tuple[str, ...] = DEFAULT_CANDIDATE_FEATURES
    composite_weights: Mapping[str, float] = field(
        default_factory=lambda: dict(DEFAULT_COMPOSITE_WEIGHTS)
    )
    tier_boundaries: tuple[tuple[float, str], ...] = DEFAULT_TIER_BOUNDARIES
    top_tier: str = DEFAULT_TOP_TIER

    # Downstream flags (SAS STEP 4).
    watch_list_pod_threshold: float = 0.5
    review_required_min_score: float = 60.0
    review_required_velocity_ratio: float = 2.0

    # Publish validation (SAS %validate_table + PROC FREQ monitoring).
    min_rows: int = 1000
    max_null_pct: float = 0.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "RiskScoringParams":
        """Build parameters from the pipeline environment (config/pipeline_config.cfg)."""
        env = os.environ if env is None else env
        params = cls(
            model_version=env.get("RISK_MODEL_VERSION", cls.model_version),
            risk_score_threshold=_env_float("RISK_SCORE_THRESHOLD", cls.risk_score_threshold),
            slentry=_env_float("RISK_MODEL_SLENTRY", cls.slentry),
            slstay=_env_float("RISK_MODEL_SLSTAY", cls.slstay),
            seed=_env_int("RISK_MODEL_SEED", cls.seed),
            min_rows=_env_int("RISK_MIN_ROWS", cls.min_rows),
            allow_intercept_only_model=env.get("RISK_ALLOW_INTERCEPT_ONLY_MODEL", "")
            .strip()
            .lower()
            in {"1", "true", "yes"},
        )
        weights_json = env.get("RISK_COMPOSITE_WEIGHTS", "").strip()
        if weights_json:
            params = replace(params, composite_weights=json.loads(weights_json))
        tiers_json = env.get("RISK_TIER_BOUNDARIES", "").strip()
        if tiers_json:
            params = replace(
                params,
                tier_boundaries=tuple(
                    (float(bound), str(label)) for bound, label in json.loads(tiers_json)
                ),
            )
        features_csv = env.get("RISK_CANDIDATE_FEATURES", "").strip()
        if features_csv:
            params = replace(
                params,
                candidate_features=tuple(f.strip() for f in features_csv.split(",") if f.strip()),
            )
        return params

    def validate(self) -> None:
        missing = set(DEFAULT_COMPOSITE_WEIGHTS) - set(self.composite_weights)
        if missing:
            raise ValueError(f"composite_weights is missing components: {sorted(missing)}")
        if not self.candidate_features:
            raise ValueError("candidate_features must not be empty")
        bounds = [bound for bound, _ in self.tier_boundaries]
        if bounds != sorted(bounds):
            raise ValueError("tier_boundaries must be sorted by ascending upper bound")


@dataclass(frozen=True)
class SnowflakeOptions:
    """Connection options for the Snowflake Spark connector.

    Credentials are resolved at runtime (Azure Key Vault via Synapse linked services, per
    TICKET-02) and are never read from source control.
    """

    url: str
    user: str
    database: str
    staging_schema: str
    data_product_schema: str
    warehouse: str
    role: str
    private_key: str | None = None
    password: str | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "SnowflakeOptions":
        env = os.environ if env is None else env
        required = (
            "SNOWFLAKE_URL",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_STAGING_SCHEMA",
            "SNOWFLAKE_DATA_PRODUCT_SCHEMA",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_ROLE",
        )
        missing = [name for name in required if not env.get(name)]
        if missing:
            raise ValueError(f"Missing Snowflake environment variables: {missing}")
        return cls(
            url=env["SNOWFLAKE_URL"],
            user=env["SNOWFLAKE_USER"],
            database=env["SNOWFLAKE_DATABASE"],
            staging_schema=env["SNOWFLAKE_STAGING_SCHEMA"],
            data_product_schema=env["SNOWFLAKE_DATA_PRODUCT_SCHEMA"],
            warehouse=env["SNOWFLAKE_WAREHOUSE"],
            role=env["SNOWFLAKE_ROLE"],
            private_key=env.get("SNOWFLAKE_PRIVATE_KEY"),
            password=env.get("SNOWFLAKE_PASSWORD"),
        )

    def connector_options(self, schema: str) -> dict[str, str]:
        options = {
            "sfUrl": self.url,
            "sfUser": self.user,
            "sfDatabase": self.database,
            "sfSchema": schema,
            "sfWarehouse": self.warehouse,
            "sfRole": self.role,
        }
        if self.private_key:
            options["pem_private_key"] = self.private_key
        elif self.password:
            options["sfPassword"] = self.password
        else:
            raise ValueError("No Snowflake credential resolved (private key or password required)")
        return options


def feature_list(params: RiskScoringParams) -> Sequence[str]:
    return list(params.candidate_features)

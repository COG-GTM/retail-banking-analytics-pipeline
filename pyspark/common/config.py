"""Pipeline configuration loader.

Mirrors ``config/pipeline_config.cfg`` from the legacy BTEQ/SAS pipeline so that
every tunable the shell/BTEQ/SAS code read from the environment is available to
the PySpark port from a single, testable object.

The two business-critical parameters that drive transformation logic are:

* ``LOOKBACK_MONTHS`` (default ``12``) -- the transaction-summary window.
* ``RISK_SCORE_THRESHOLD`` (default ``700``) -- the bureau-score cutoff used by
  the risk-scoring job (``%sysget(RISK_SCORE_THRESHOLD)`` in the SAS code).

``run_date`` replaces Teradata/SAS ``CURRENT_DATE``/``today()``.  Making it an
explicit, config-driven value (rather than reading the wall clock deep inside a
transform) is what makes the port deterministic and reproducible (Rules R4).
"""

from __future__ import annotations

import datetime as _dt
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Mapping

# Defaults taken verbatim from config/pipeline_config.cfg
_DEFAULTS: dict[str, str] = {
    "TD_SERVER": "tdprod.corp.bankdemo.com",
    "TD_USERNAME": "svc_etl_pipeline",
    "TD_LOGMECH": "LDAP",
    "DB_CORE": "CORE_BANKING_DB",
    "DB_TXN": "TXN_PROCESSING_DB",
    "DB_STG": "ETL_STAGING_DB",
    "DB_DP": "DATA_PRODUCTS_DB",
    "LOOKBACK_MONTHS": "12",
    "RISK_SCORE_THRESHOLD": "700",
    "LOG_LEVEL": "INFO",
}

# Model-version tags carried over from the individual SAS programs so the data
# product output columns match the legacy contract exactly.
MODEL_VERSIONS: dict[str, str] = {
    "customer_segments": "SEG_V3.2",
    "transaction_analytics": "TXN_V2.1",
    "risk_scoring": "RISK_V4.0",
    "master_profile": "MASTER_V1.5",
}


def _parse_date(value: str | _dt.date | None) -> _dt.date:
    if value is None:
        return _dt.date.today()
    if isinstance(value, _dt.date):
        return value
    return _dt.date.fromisoformat(str(value).strip())


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for a single pipeline run."""

    td_server: str = _DEFAULTS["TD_SERVER"]
    td_username: str = _DEFAULTS["TD_USERNAME"]
    td_logmech: str = _DEFAULTS["TD_LOGMECH"]

    db_core: str = _DEFAULTS["DB_CORE"]
    db_txn: str = _DEFAULTS["DB_TXN"]
    db_stg: str = _DEFAULTS["DB_STG"]
    db_dp: str = _DEFAULTS["DB_DP"]

    lookback_months: int = int(_DEFAULTS["LOOKBACK_MONTHS"])
    risk_score_threshold: int = int(_DEFAULTS["RISK_SCORE_THRESHOLD"])
    log_level: str = _DEFAULTS["LOG_LEVEL"]

    # run_date replaces CURRENT_DATE / today(); defaults to today when unset.
    run_date: _dt.date = field(default_factory=_dt.date.today)

    # Layout of the local/lake data used by the file-based readers/writers.
    # Production overrides these with JDBC/catalog references.
    data_root: Path | None = None

    def with_overrides(self, **kwargs) -> "PipelineConfig":
        """Return a copy with the given fields replaced."""
        return replace(self, **kwargs)

    # -- Derived date literals ------------------------------------------------
    @property
    def lookback_start(self) -> _dt.date:
        """PERIOD_START = ADD_MONTHS(CURRENT_DATE, -LOOKBACK_MONTHS)."""
        return add_months(self.run_date, -self.lookback_months)

    @property
    def run_date_str(self) -> str:
        return self.run_date.isoformat()

    @property
    def reporting_period(self) -> str:
        """YYYY-MM for the run month (SAS REPORTING_PERIOD)."""
        return self.run_date.strftime("%Y-%m")

    # -- Factories ------------------------------------------------------------
    @classmethod
    def from_mapping(cls, mapping: Mapping[str, str]) -> "PipelineConfig":
        get = lambda k: mapping.get(k, _DEFAULTS.get(k))  # noqa: E731
        data_root = mapping.get("DATA_ROOT")
        return cls(
            td_server=get("TD_SERVER"),
            td_username=get("TD_USERNAME"),
            td_logmech=get("TD_LOGMECH"),
            db_core=get("DB_CORE"),
            db_txn=get("DB_TXN"),
            db_stg=get("DB_STG"),
            db_dp=get("DB_DP"),
            lookback_months=int(get("LOOKBACK_MONTHS")),
            risk_score_threshold=int(get("RISK_SCORE_THRESHOLD")),
            log_level=get("LOG_LEVEL"),
            run_date=_parse_date(mapping.get("RUN_DATE")),
            data_root=Path(data_root) if data_root else None,
        )

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "PipelineConfig":
        return cls.from_mapping(dict(environ if environ is not None else os.environ))

    @classmethod
    def from_cfg_file(cls, path: str | Path, environ: Mapping[str, str] | None = None) -> "PipelineConfig":
        """Parse the shell ``pipeline_config.cfg`` without sourcing it.

        Reads ``export KEY="value"`` lines, expands ``${VAR:-default}`` style
        references against ``environ``, and layers any real environment values
        on top (env wins, matching shell ``${VAR:-default}`` semantics).
        """
        environ = dict(environ if environ is not None else os.environ)
        parsed = _parse_cfg(Path(path), environ)
        merged = {**parsed, **{k: v for k, v in environ.items() if k in _DEFAULTS or k in ("RUN_DATE", "DATA_ROOT")}}
        return cls.from_mapping(merged)


_EXPORT_RE = re.compile(r'^\s*export\s+([A-Z0-9_]+)=(.*)$')
_VAR_DEFAULT_RE = re.compile(r'\$\{([A-Z0-9_]+):-([^}]*)\}')
_VAR_RE = re.compile(r'\$\{?([A-Z0-9_]+)\}?')


def _parse_cfg(path: Path, environ: Mapping[str, str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _EXPORT_RE.match(line)
        if not m:
            continue
        key, rhs = m.group(1), m.group(2).strip()
        # Strip trailing inline comments outside of quotes and surrounding quotes.
        rhs = _strip_inline_comment(rhs)
        rhs = rhs.strip().strip('"').strip("'")
        # Skip dynamic shell expressions like $(date ...).
        if rhs.startswith("$(") or "`" in rhs:
            continue
        rhs = _VAR_DEFAULT_RE.sub(lambda mm: environ.get(mm.group(1), mm.group(2)), rhs)
        rhs = _VAR_RE.sub(lambda mm: environ.get(mm.group(1), values.get(mm.group(1), "")), rhs)
        values[key] = rhs
    return values


def _strip_inline_comment(rhs: str) -> str:
    in_single = in_double = False
    for i, ch in enumerate(rhs):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            return rhs[:i]
    return rhs


def add_months(base: _dt.date, months: int) -> _dt.date:
    """Teradata ``ADD_MONTHS`` semantics with month-end clamping."""
    month_index = base.month - 1 + months
    year = base.year + month_index // 12
    month = month_index % 12 + 1
    # Clamp day to the last valid day of the target month.
    if month == 12:
        next_month_first = _dt.date(year + 1, 1, 1)
    else:
        next_month_first = _dt.date(year, month + 1, 1)
    last_day = (next_month_first - _dt.timedelta(days=1)).day
    return _dt.date(year, month, min(base.day, last_day))

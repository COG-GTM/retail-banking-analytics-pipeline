"""Pipeline configuration.

Port of ``config/pipeline_config.cfg``. The legacy shell config is parsed rather than
re-typed so that a change to the ``.cfg`` file flows through to the PySpark jobs. Jobs must
never hardcode a value that lives here.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

_EXPORT_RE = re.compile(r"^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$")
_DEFAULT_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_DATE_CMD_RE = re.compile(r"\$\(date\s+\+([^)]+)\)")

_STRFTIME_TRANSLATION = {
    "%Y%m%d_%H%M%S": "%Y%m%d_%H%M%S",
    "%Y%m%d": "%Y%m%d",
}

DEFAULT_CFG_PATH = Path(__file__).resolve().parents[2] / "config" / "pipeline_config.cfg"


class ConfigError(RuntimeError):
    """Raised when the legacy configuration file cannot be interpreted."""


@dataclass(frozen=True)
class RiskScoringConstants:
    """Constants embedded in ``sas/03_sas_risk_scoring.sas``.

    They are not part of ``pipeline_config.cfg`` but are exposed here so that no job carries a
    magic number and so that a model recalibration is a configuration change.
    """

    credit_risk_weight: float = 0.30
    behaviour_risk_weight: float = 0.25
    velocity_risk_weight: float = 0.15
    bureau_score_weight: float = 0.20
    payment_history_weight: float = 0.10

    tier_low_max: float = 20.0
    tier_moderate_max: float = 40.0
    tier_elevated_max: float = 60.0
    tier_high_max: float = 80.0

    bureau_score_min: int = 300
    bureau_score_max: int = 850
    bureau_score_impute: int = 680

    default_flag_late_payment_threshold: int = 2
    watch_list_pd_threshold: float = 0.5
    review_required_score_threshold: float = 60.0
    review_required_velocity_threshold: float = 2.0

    stepwise_slentry: float = 0.10
    stepwise_slstay: float = 0.05


@dataclass(frozen=True)
class PipelineConfig:
    """Typed view over ``config/pipeline_config.cfg``."""

    td_server: str
    td_username: str
    td_logmech: str
    db_core: str
    db_txn: str
    db_stg: str
    db_dp: str
    pipeline_home: str
    bteq_dir: str
    sas_dir: str
    log_dir: str
    archive_dir: str
    run_date: date
    run_timestamp: str
    lookback_months: int
    risk_score_threshold: int
    log_level: str
    min_rows: int = 1000
    risk: RiskScoringConstants = field(default_factory=RiskScoringConstants)
    raw: dict[str, str] = field(default_factory=dict)

    @property
    def run_date_str(self) -> str:
        return self.run_date.isoformat()

    @classmethod
    def from_cfg_file(
        cls,
        path: str | Path | None = None,
        *,
        run_date: date | None = None,
        min_rows: int | None = None,
        environ: dict[str, str] | None = None,
    ) -> PipelineConfig:
        cfg_path = Path(path) if path is not None else DEFAULT_CFG_PATH
        values = parse_cfg_file(cfg_path, run_date=run_date, environ=environ)
        return cls.from_mapping(values, run_date=run_date, min_rows=min_rows)

    @classmethod
    def from_mapping(
        cls,
        values: dict[str, str],
        *,
        run_date: date | None = None,
        min_rows: int | None = None,
    ) -> PipelineConfig:
        missing = [
            key
            for key in (
                "DB_CORE",
                "DB_TXN",
                "DB_STG",
                "DB_DP",
                "LOOKBACK_MONTHS",
                "RISK_SCORE_THRESHOLD",
            )
            if key not in values
        ]
        if missing:
            raise ConfigError(f"pipeline config is missing required keys: {', '.join(missing)}")

        effective_run_date = run_date or _parse_run_date(values.get("RUN_DATE"))
        return cls(
            td_server=values.get("TD_SERVER", ""),
            td_username=values.get("TD_USERNAME", ""),
            td_logmech=values.get("TD_LOGMECH", "LDAP"),
            db_core=values["DB_CORE"],
            db_txn=values["DB_TXN"],
            db_stg=values["DB_STG"],
            db_dp=values["DB_DP"],
            pipeline_home=values.get("PIPELINE_HOME", ""),
            bteq_dir=values.get("BTEQ_DIR", ""),
            sas_dir=values.get("SAS_DIR", ""),
            log_dir=values.get("LOG_DIR", ""),
            archive_dir=values.get("ARCHIVE_DIR", ""),
            run_date=effective_run_date,
            run_timestamp=values.get("RUN_TIMESTAMP", effective_run_date.strftime("%Y%m%d_%H%M%S")),
            lookback_months=int(values["LOOKBACK_MONTHS"]),
            risk_score_threshold=int(values["RISK_SCORE_THRESHOLD"]),
            log_level=values.get("LOG_LEVEL", "INFO"),
            min_rows=1000 if min_rows is None else min_rows,
            raw=dict(values),
        )


def parse_cfg_file(
    path: str | Path,
    *,
    run_date: date | None = None,
    environ: dict[str, str] | None = None,
) -> dict[str, str]:
    """Parse the ``export KEY=VALUE`` lines of the legacy bash config.

    ``${VAR:-default}`` is resolved against the process environment and previously parsed keys.
    ``$(date +FMT)`` is resolved against ``run_date`` so that a run is reproducible.
    """

    cfg_path = Path(path)
    if not cfg_path.is_file():
        raise ConfigError(f"pipeline config not found: {cfg_path}")

    env = dict(os.environ if environ is None else environ)
    stamp = datetime.combine(run_date, datetime.min.time()) if run_date else datetime.now()
    values: dict[str, str] = {}

    for line in cfg_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _EXPORT_RE.match(line)
        if match is None:
            continue
        key, raw_value = match.group(1), match.group(2)
        raw_value = _strip_inline_comment(raw_value)
        values[key] = _expand(raw_value, values, env, stamp)

    return values


def _strip_inline_comment(value: str) -> str:
    if value.startswith('"'):
        end = value.find('"', 1)
        if end != -1:
            return value[1:end]
    if value.startswith("'"):
        end = value.find("'", 1)
        if end != -1:
            return value[1:end]
    return value.split("#", 1)[0].strip()


def _expand(value: str, values: dict[str, str], env: dict[str, str], stamp: datetime) -> str:
    def _date_sub(match: re.Match[str]) -> str:
        fmt = match.group(1)
        if fmt not in _STRFTIME_TRANSLATION:
            raise ConfigError(f"unsupported date format in pipeline config: {fmt}")
        return stamp.strftime(_STRFTIME_TRANSLATION[fmt])

    def _var_sub(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in env:
            return env[name]
        if name in values:
            return values[name]
        return default if default is not None else ""

    return _DEFAULT_RE.sub(_var_sub, _DATE_CMD_RE.sub(_date_sub, value)).strip('"')


def _parse_run_date(raw: str | None) -> date:
    if not raw:
        return date.today()
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError as exc:  # pragma: no cover - defensive
        raise ConfigError(f"RUN_DATE is not YYYYMMDD: {raw}") from exc

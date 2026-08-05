"""Job contract shared by every ported job.

Each job module exposes pure ``transform_*`` functions over DataFrames plus a thin
``run(spark, io, config, audit) -> JobResult`` and a ``main()`` CLI. The orchestration driver
only knows about :class:`JobResult`.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from common.audit import AuditLog
from common.config import PipelineConfig
from common.io import DataIO, JdbcDataIO, LocalDataIO
from common.spark import build_spark_session
from common.validation import ValidationResult

LOGGER = logging.getLogger(__name__)

STATUS_SUCCESS = "SUCCESS"
STATUS_WARNING = "WARNING"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"


@dataclass
class JobResult:
    """Outcome of a single job, mirroring the legacy exit-code semantics.

    ``SAS`` return codes: 0 clean, 1 warnings (tolerated), >=2 error (aborts the phase).
    ``BTEQ``: any non-zero return code aborts the phase.
    """

    job_name: str
    status: str = STATUS_SUCCESS
    row_count: int = 0
    start_ts: datetime = field(default_factory=datetime.now)
    end_ts: datetime | None = None
    target_table: str = ""
    validation: ValidationResult | None = None
    error: str = ""

    @property
    def elapsed_seconds(self) -> float:
        end = self.end_ts or datetime.now()
        return (end - self.start_ts).total_seconds()

    @property
    def return_code(self) -> int:
        if self.status == STATUS_FAILED:
            return 2
        if self.status == STATUS_WARNING:
            return 1
        return 0

    def as_dict(self) -> dict[str, object]:
        return {
            "job_name": self.job_name,
            "status": self.status,
            "row_count": self.row_count,
            "target_table": self.target_table,
            "start_ts": self.start_ts.isoformat(),
            "end_ts": (self.end_ts or datetime.now()).isoformat(),
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "return_code": self.return_code,
            "error": self.error,
            "validation": self.validation.as_dict() if self.validation else None,
        }


JobRunner = Callable[[SparkSession, DataIO, PipelineConfig, AuditLog], JobResult]


def build_arg_parser(description: str) -> argparse.ArgumentParser:
    """Standard CLI shared by every job and by the DAG driver."""

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", default=None, help="path to config/pipeline_config.cfg")
    parser.add_argument("--run-date", default=None, help="pinned run date (YYYY-MM-DD)")
    parser.add_argument(
        "--io",
        choices=("local", "jdbc"),
        default="local",
        help="data access layer to use",
    )
    parser.add_argument("--data-path", default="warehouse", help="root path for the local IO layer")
    parser.add_argument("--format", default="parquet", help="file format for the local IO layer")
    parser.add_argument("--jdbc-url", default=None, help="JDBC URL when --io jdbc")
    parser.add_argument("--jdbc-user", default=None, help="JDBC user when --io jdbc")
    parser.add_argument("--jdbc-password", default=None, help="JDBC password when --io jdbc")
    parser.add_argument(
        "--min-rows", type=int, default=None, help="override %%validate_table min_rows"
    )
    parser.add_argument("--master", default=None, help="Spark master override")
    return parser


def config_from_args(args: argparse.Namespace) -> PipelineConfig:
    run_date = date.fromisoformat(args.run_date) if args.run_date else None
    return PipelineConfig.from_cfg_file(args.config, run_date=run_date, min_rows=args.min_rows)


def io_from_args(args: argparse.Namespace, spark: SparkSession, config: PipelineConfig) -> DataIO:
    if args.io == "jdbc":
        if not args.jdbc_url:
            raise SystemExit("--jdbc-url is required with --io jdbc")
        return JdbcDataIO(
            spark=spark,
            url=args.jdbc_url,
            user=args.jdbc_user or "",
            password=args.jdbc_password or "",
            schema_map=default_schema_map(config),
        )
    return LocalDataIO(spark=spark, base_path=Path(args.data_path), fmt=args.format)


def default_schema_map(config: PipelineConfig) -> dict[str, str]:
    """Teradata database -> physical schema in the target database."""

    return {
        config.db_core: config.db_core.lower(),
        config.db_txn: config.db_txn.lower(),
        config.db_stg: config.db_stg.lower(),
        config.db_dp: config.db_dp.lower(),
    }


def run_job_cli(runner: JobRunner, description: str, argv: list[str] | None = None) -> int:
    """Shared ``main()`` body: build config, session, IO and audit, then run one job."""

    args = build_arg_parser(description).parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s"
    )
    config = config_from_args(args)
    spark = build_spark_session(description, master=args.master)
    audit = AuditLog(run_timestamp=config.run_timestamp)
    try:
        io = io_from_args(args, spark, config)
        result = runner(spark, io, config, audit)
        LOGGER.info("%s finished: %s", result.job_name, result.as_dict())
        return result.return_code
    finally:
        spark.stop()


def job_entry_point(runner: JobRunner, description: str) -> Callable[[], None]:
    """Build a ``main()`` for a job module."""

    def main() -> None:
        sys.exit(run_job_cli(runner, description))

    return main

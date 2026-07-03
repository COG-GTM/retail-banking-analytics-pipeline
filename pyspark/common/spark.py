"""SparkSession factory with the scalability knobs the migration requires.

Adaptive Query Execution + skew handling are enabled here (rather than sprinkled
through the jobs) so every transform benefits from dynamic partition coalescing
and skew-join splitting -- the mechanism we rely on for the CUSTOMER_ID /
ACCOUNT_ID joins on a multi-million customer database.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

_SCALE_CONF = {
    # Adaptive Query Execution: coalesce shuffle partitions + optimise joins.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    # Auto-broadcast small dimensions (TRANSACTION_TYPES) up to 32MB.
    "spark.sql.autoBroadcastJoinThreshold": str(32 * 1024 * 1024),
    # Keep decimal arithmetic exact rather than silently returning NULL on overflow.
    "spark.sql.decimalOperations.allowPrecisionLoss": "false",
    # Deterministic, timezone-stable date/time handling for reproducibility.
    "spark.sql.session.timeZone": "UTC",
}


def build_spark(
    app_name: str = "retail-banking-analytics",
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Return a configured SparkSession (reuses the active one if present)."""
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    for key, value in _SCALE_CONF.items():
        builder = builder.config(key, value)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def build_local_spark(app_name: str = "rbap-tests", shuffle_partitions: int = 4) -> SparkSession:
    """Small local SparkSession for tests -- few shuffle partitions for speed."""
    return build_spark(
        app_name=app_name,
        master="local[2]",
        extra_conf={
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            "spark.ui.enabled": "false",
            "spark.sql.adaptive.enabled": "false",
        },
    )

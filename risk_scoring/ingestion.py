"""STEP 1 and STEP 2 of ``sas/03_sas_risk_scoring.sas``.

* :func:`read_risk_raw` ports the ``PROC SQL`` that builds ``WORK.RISK_RAW``.
* :func:`build_risk_features` ports the ``DATA`` step that builds
  ``WORK.RISK_FEATURES``.

Both are pure: DataFrame in, DataFrame out, no side effects, deterministic on
re-run.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import schemas
from .connections import Connections

#: Bureau score imputed when the upstream join missed (SAS: population median
#: placeholder). ``03_stg_risk_factors.bteq`` COALESCEs a missing bureau row to
#: ``0``, so a zero and a NULL score are both imputed here.
BUREAU_SCORE_IMPUTE = 680

#: Bureau score range used to normalise to 0-100. SAS writes the divisor as
#: ``(850 - 300)``, i.e. 550.
BUREAU_SCORE_MIN = 300.0
BUREAU_SCORE_MAX = 850.0

#: SAS annualises the 7-day debit velocity onto a 30-day window: ``30/7``.
VELOCITY_ANNUALISATION = 30.0 / 7.0

#: ``DEFAULT_FLAG = (PAYMENT_LATE_CNT > 2)`` — the SAS default proxy.
DEFAULT_LATE_PAYMENT_THRESHOLD = 2


def read_risk_raw(connections: Connections) -> DataFrame:
    """Build ``WORK.RISK_RAW`` — STEP 1.

    .. code-block:: sas

        proc sql;
            create table WORK.RISK_RAW as
            select r.*, c.TENURE_MONTHS, c.NUM_ACTIVE_ACCOUNTS,
                   c.TOTAL_BALANCE, c.CUSTOMER_STATUS
            from STGDB.STG_RISK_FACTORS r
            inner join STGDB.STG_CUSTOMER_360 c
                on r.CUSTOMER_ID = c.CUSTOMER_ID
            where c.CUSTOMER_STATUS = 'A';
        quit;

    ``STGDB`` is ``config.databases.staging``, reached through ``connections``.
    The risk factors are read with :func:`schemas.analytic_schema` so the
    DECIMAL measures arrive as ``double``, matching SAS's 64-bit float numerics.
    """
    risk_factors = connections.read_staging(
        schemas.STG_RISK_FACTORS,
        schemas.analytic_schema(schemas.STG_RISK_FACTORS_SCHEMA),
    )
    customer_360 = connections.read_staging(schemas.STG_CUSTOMER_360).select(
        *schemas.STG_CUSTOMER_360_RISK_COLUMNS
    )

    # Join on the column name so the output carries a single CUSTOMER_ID; the
    # remaining STG_CUSTOMER_360 columns are appended after r.* , as in the
    # SELECT list above.
    return (
        risk_factors.join(customer_360, on="CUSTOMER_ID", how="inner")
        .where(F.col("CUSTOMER_STATUS") == F.lit("A"))
        .select(
            *risk_factors.columns,
            *(
                c
                for c in schemas.STG_CUSTOMER_360_RISK_COLUMNS
                if c != "CUSTOMER_ID"
            ),
        )
    )


def build_risk_features(risk_raw: DataFrame) -> DataFrame:
    """Build ``WORK.RISK_FEATURES`` — STEP 2.

    .. code-block:: sas

        if EXTERNAL_CREDIT_SCORE <= 0 or EXTERNAL_CREDIT_SCORE = . then
            EXTERNAL_CREDIT_SCORE = 680;
        BUREAU_SCORE_NORM = (EXTERNAL_CREDIT_SCORE - 300) / (850 - 300) * 100;
        if AVG_DAILY_BALANCE_90D > 0 then
            BALANCE_TREND_RATIO = AVG_DAILY_BALANCE_30D / AVG_DAILY_BALANCE_90D;
        else BALANCE_TREND_RATIO = 1;
        if DEBIT_VELOCITY_30D > 0 then
            VELOCITY_RATIO = (DEBIT_VELOCITY_7D * (30/7)) / DEBIT_VELOCITY_30D;
        else VELOCITY_RATIO = 1;
        DEFAULT_FLAG = (PAYMENT_LATE_CNT > 2);

    ``EXTERNAL_CREDIT_SCORE`` is overwritten in place and ``BUREAU_SCORE_NORM``
    derives from the imputed value. Every input column is preserved in its
    original position.

    Both ratios keep SAS's ``> 0`` guard rather than relying on Spark returning
    NULL for a division by zero: a NULL denominator is not ``> 0``, so — as in
    SAS, where a missing value also falls into the ``else`` branch — it yields
    ``1.0``.
    """
    external_credit_score = (
        F.when(
            F.col("EXTERNAL_CREDIT_SCORE").isNull()
            | (F.col("EXTERNAL_CREDIT_SCORE") <= 0),
            F.lit(BUREAU_SCORE_IMPUTE),
        )
        .otherwise(F.col("EXTERNAL_CREDIT_SCORE"))
        .alias("EXTERNAL_CREDIT_SCORE")
    )

    imputed = risk_raw.select(*[
        external_credit_score if c == "EXTERNAL_CREDIT_SCORE" else F.col(c)
        for c in risk_raw.columns
    ])

    bureau_score_norm = (
        (F.col("EXTERNAL_CREDIT_SCORE") - F.lit(BUREAU_SCORE_MIN))
        / F.lit(BUREAU_SCORE_MAX - BUREAU_SCORE_MIN)
        * F.lit(100.0)
    )

    # Derived by the SAS DATA step but never consumed downstream (it is not a
    # MODEL predictor and not a CUSTOMER_RISK_SCORES column); ported for parity.
    balance_trend_ratio = F.when(
        F.col("AVG_DAILY_BALANCE_90D") > 0,
        F.col("AVG_DAILY_BALANCE_30D") / F.col("AVG_DAILY_BALANCE_90D"),
    ).otherwise(F.lit(1.0))

    velocity_ratio = F.when(
        F.col("DEBIT_VELOCITY_30D") > 0,
        (F.col("DEBIT_VELOCITY_7D") * F.lit(VELOCITY_ANNUALISATION))
        / F.col("DEBIT_VELOCITY_30D"),
    ).otherwise(F.lit(1.0))

    # SAS evaluates a comparison to numeric 0/1, and a missing PAYMENT_LATE_CNT
    # is not > 2, so NULL must yield 0 rather than NULL.
    default_flag = (
        F.coalesce(F.col("PAYMENT_LATE_CNT"), F.lit(0))
        > F.lit(DEFAULT_LATE_PAYMENT_THRESHOLD)
    ).cast("int")

    return imputed.withColumns({
        "BUREAU_SCORE_NORM": bureau_score_norm.cast("double"),
        "BALANCE_TREND_RATIO": balance_trend_ratio.cast("double"),
        "VELOCITY_RATIO": velocity_ratio.cast("double"),
        "DEFAULT_FLAG": default_flag,
    })

-- =========================================================================
-- TICKET-08 / MBA-2209 - Risk model audit tables (Snowflake)
--
-- Written by spark/jobs/03_risk_scoring.py on every scoring run so the fitted
-- coefficients and the stepwise-selected variable set stay auditable. The SAS
-- job kept these only in WORK.RISK_MODEL, which was discarded at session end.
--
-- Teradata -> Snowflake type mapping used across the migration:
--   VARCHAR(n) LATIN      -> VARCHAR(n)
--   DECIMAL(p,s)          -> NUMBER(p,s)
--   FLOAT                 -> FLOAT
--   BYTEINT / SMALLINT    -> NUMBER(3,0) / NUMBER(5,0)
--   TIMESTAMP(6)          -> TIMESTAMP_NTZ(6)
-- =========================================================================

CREATE TABLE IF NOT EXISTS RISK_MODEL_RUNS (
    RUN_ID              VARCHAR(64)     NOT NULL,
    MODEL_VERSION       VARCHAR(20)     NOT NULL,
    SELECTED_FEATURES   VARCHAR(1000),
    SELECTION_LOG       VARCHAR(4000),
    N_OBSERVATIONS      NUMBER(18,0),
    N_EVENTS            NUMBER(18,0),
    LOG_LIKELIHOOD      FLOAT,
    CONVERGED           BOOLEAN,
    LOAD_TS             TIMESTAMP_NTZ(6) NOT NULL
);

CREATE TABLE IF NOT EXISTS RISK_MODEL_COEFFICIENTS (
    RUN_ID              VARCHAR(64)     NOT NULL,
    MODEL_VERSION       VARCHAR(20)     NOT NULL,
    TERM                VARCHAR(64)     NOT NULL,
    COEFFICIENT         FLOAT,
    STD_ERROR           FLOAT,
    P_VALUE             FLOAT,
    SELECTED            BOOLEAN,
    LOAD_TS             TIMESTAMP_NTZ(6) NOT NULL
);

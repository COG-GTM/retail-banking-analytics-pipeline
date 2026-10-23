# synapse/tests

Each ticket writes tests **only** under its own folder, `synapse/tests/ticket_NN/`
(e.g. `ticket_04/`), including its own `conftest.py`. There is deliberately no shared
`conftest.py` here, to avoid merge conflicts between parallel sessions.

Local validation conventions (no live Azure resources are required):

- **T-SQL** (T2–T6, T9 helper procs): run against a `mcr.microsoft.com/mssql/server:2022-latest`
  container. Dedicated-pool-only syntax (`WITH (DISTRIBUTION = ..., CLUSTERED COLUMNSTORE INDEX)`,
  CTAS) must be shimmed in the test harness (e.g. strip the `WITH (...)` table options, rewrite
  `CREATE TABLE t WITH (...) AS <select>` to `SELECT * INTO t FROM (<select>) AS src`).
  Each ticket uses its **own host port and container name** and removes only the container it
  started: T2 `14332`, T3 `14333`, T4 `14334`, T5 `14335`, T6 `14336`, T9 `14339`.
- **PySpark** (T7–T9): local PySpark 3.4.1 under a Python 3.10 virtualenv created outside the repo;
  Java 17 needs the usual `--add-opens` JVM flags.
- **Parity data:** `data/01_source_tables/`, `data/02_bteq_staging/`, `data/03_sas_data_products/`
  (generated as of 2026-04-10 by a DuckDB/scikit-learn re-implementation, not by real
  Teradata/SAS — compare with documented tolerances, not exact equality).
- Ticket-specific extra Python deps go in `synapse/tests/ticket_NN/requirements.txt`.

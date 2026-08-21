# Azure Synapse Spark artifacts

Migrated counterparts of the SAS analytics steps. The legacy SAS programs under
`sas/` stay in place as the reference implementation.

| Legacy | Migrated |
| --- | --- |
| `sas/01_sas_customer_segments.sas` | `synapse/spark/jobs/01_customer_segments.py` (TICKET-06 / MBA-2207) |

## Layout

```
synapse/spark/jobs/            PySpark entry points submitted to a Synapse Spark pool
synapse/spark/jobdefinitions/  Synapse Spark job definition (workspace artifact)
synapse/spark/reconciliation/  SAS-vs-Spark output reconciliation tooling
```

## 01_customer_segments

Reads `ETL_STAGING.STG_CUSTOMER_360` from Snowflake (active customers only),
rebuilds the SAS feature set, standardizes six clustering features with
`StandardScaler(withMean=True, withStd=True)` (PROC STDIZE METHOD=STD), fits
Spark MLlib `KMeans` with `k=5`, labels clusters by descending mean
`LOG_BALANCE`, scores LTV/engagement/breadth, derives the cross-sell, upsell and
retention-risk flags, validates row count / key uniqueness / null rates and
truncate-loads `DATA_PRODUCTS.CUSTOMER_SEGMENTS` through the Snowflake Spark
connector.

Snowflake credentials come from Synapse pipeline parameters plus an Azure Key
Vault secret resolved with the workspace managed identity; nothing is stored in
the repository.

Reproducibility: the job fits `--n-init` (default 20) k-means models from the
fixed seed sequence `seed .. seed + n_init - 1` and keeps the lowest-cost fit,
so the same input and seed always yield identical assignments.

### Offline run and reconciliation

```bash
python synapse/spark/jobs/01_customer_segments.py \
    --input-csv data/02_bteq_staging/stg_customer_360.csv \
    --output-csv /tmp/customer_segments_spark \
    --min-rows 100

python synapse/spark/reconciliation/reconcile_customer_segments.py \
    --baseline data/03_sas_data_products/customer_segments.csv \
    --candidate /tmp/customer_segments_spark \
    --min-cluster-agreement 0.85 \
    --min-column-agreement 0.94
```

Agreed cluster-agreement threshold with the SAS baseline: **85%** (current run:
90.91%, with no cluster-label permutation needed). The scores and flags
reconcile exactly; `TENURE_GROUP`, `AGE_GROUP` and the two `UPSELL_FLAG` rows
that depend on `TENURE_GROUP` differ only on boundary values (`TENURE_MONTHS`
12/36, `AGE` 25/41/57/76), because the checked-in demo extract classifies those
boundaries inclusively while the SAS source uses strict `<` comparisons. The
Spark job follows the SAS source, hence `--min-column-agreement 0.94` when
reconciling against that extract.

### Tests

```bash
python -m pytest tests/synapse -q
```

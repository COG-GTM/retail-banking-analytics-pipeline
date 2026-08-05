# Risk Scoring Parity Report

* PySpark output: `/home/ubuntu/repos/retail-banking-analytics-pipeline/output/customer_risk_scores_csv` — 407 rows
* Oracle: `/home/ubuntu/repos/retail-banking-analytics-pipeline/data/03_sas_data_products/customer_risk_scores.csv` — 407 rows
* Joined on `CUSTOMER_ID`: 407 common, 0 only in PySpark, 0 only in oracle

## Exact parity (deterministic fields)

| Column | Compared | Mismatches | Match rate | Max abs diff |
| --- | --- | --- | --- | --- |
| COMPOSITE_RISK_SCORE | 407 | 0 | 100.0000% | 0 |
| CREDIT_RISK_COMPONENT | 407 | 0 | 100.0000% | 0.00454545 |
| BEHAVIOUR_RISK_COMPONENT | 407 | 0 | 100.0000% | 0 |
| VELOCITY_RISK_COMPONENT | 407 | 0 | 100.0000% | 0.00499506 |
| BUREAU_SCORE_COMPONENT | 407 | 0 | 100.0000% | 0.00454545 |
| PAYMENT_HISTORY_COMPONENT | 407 | 0 | 100.0000% | 0 |
| SCORE_DELTA_30D | 407 | 0 | 100.0000% | 0 |
| RISK_TIER | 407 | 0 | 100.0000% | - |
| REVIEW_REQUIRED_FLAG | 407 | 0 | 100.0000% | - |

## Known oracle divergence: risk-driver tie-breaking

`CREDIT_RISK_COMPONENT` and `(100 - BUREAU_SCORE_COMPONENT)` are the same
quantity whenever the clamps are inactive, so array index 0
(`CREDIT_UTILIZATION`) and index 3 (`BUREAU_SCORE`) tie on nearly every row.
The SAS source resolves the tie with a strict `>` comparison walked in array
order, which keeps the **first** index; the oracle extract keeps `BUREAU_SCORE`.
This port follows the SAS source, so these two columns are expected to differ
from the oracle and are excluded from the verdict above. No simple tie rule
reproduces the oracle either (last-index-wins still leaves 9 primary and 16
secondary mismatches), so the oracle's ordering is not a documented rule to
port.

| Column | Compared | Mismatches | Agreement with oracle |
| --- | --- | --- | --- |
| PRIMARY_RISK_DRIVER | 407 | 294 | 27.76% |
| SECONDARY_RISK_DRIVER | 407 | 407 | 0.00% |


## Distribution parity (model-dependent fields)

### PROBABILITY_OF_DEFAULT

| Statistic | PySpark | Oracle | Delta |
| --- | --- | --- | --- |
| n | 407.000000 | 407.000000 | +0.000000 |
| mean | 0.000000 | 0.050000 | -0.050000 |
| stdev | 0.000000 | 0.000000 | +0.000000 |
| min | 0.000000 | 0.050000 | -0.050000 |
| p25 | 0.000000 | 0.050000 | -0.050000 |
| p50 | 0.000000 | 0.050000 | -0.050000 |
| p75 | 0.000000 | 0.050000 | -0.050000 |
| max | 0.000000 | 0.050000 | -0.050000 |

Two-sample KS statistic: `1.0000`

### WATCH_LIST_FLAG

| Value | PySpark | Oracle | Delta |
| --- | --- | --- | --- |
| N | 407 | 407 | +0 |

Row-level agreement: 407/407 (100.00%)

## Risk tier distribution (replaces `PROC FREQ`)

| Risk tier | PySpark | PySpark % | Oracle | Oracle % | Delta |
| --- | --- | --- | --- | --- | --- |
| LOW | 290 | 71.25% | 290 | 71.25% | +0 |
| MODERATE | 113 | 27.76% | 113 | 27.76% | +0 |
| ELEVATED | 4 | 0.98% | 4 | 0.98% | +0 |

## Verdict

Exact parity on deterministic fields: **PASS**

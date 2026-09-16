import pyspark.sql.functions as F

NUMERIC_COLS = {
    "stg_customer_360": ["age", "tenure_months", "num_accounts",
                         "num_active_accounts", "total_balance",
                         "total_credit_limit", "credit_utilization_pct"],
    "stg_txn_summary": ["txn_count_total", "txn_count_debit",
                        "txn_count_credit", "txn_count_fee",
                        "amt_total_debit", "amt_total_credit",
                        "amt_total_fees", "amt_avg_debit", "amt_avg_credit",
                        "amt_max_single_debit", "amt_max_single_credit",
                        "distinct_merchants", "pct_atm", "pct_pos",
                        "pct_web", "pct_mobile", "days_since_last_txn"],
    "stg_risk_factors": ["account_overdraft_cnt", "nsf_fee_total",
                         "large_withdrawal_cnt", "large_withdrawal_amt",
                         "avg_daily_balance_30d", "avg_daily_balance_90d",
                         "balance_volatility", "credit_util_ratio",
                         "payment_ontime_pct", "payment_late_cnt",
                         "months_since_last_late", "external_credit_score",
                         "debit_velocity_7d", "debit_velocity_30d",
                         "new_merchant_cnt_30d", "international_txn_cnt",
                         "high_risk_merchant_cnt"],
}
STRING_COLS = {
    "stg_customer_360": ["has_checking", "has_savings", "has_credit",
                         "has_loan"],
    "stg_txn_summary": ["top_merchant_category"],
    "stg_risk_factors": [],
}
KEYS = {"stg_customer_360": ["customer_id"],
        "stg_txn_summary": ["customer_id", "account_id"],
        "stg_risk_factors": ["customer_id"]}


def test_row_counts(silver_actual, silver_expected):
    for t in NUMERIC_COLS:
        a, e = silver_actual[t].count(), silver_expected[t].count()
        assert a == e, f"{t}: actual={a} expected={e}"


def test_key_sets(silver_actual, silver_expected):
    for t, keys in KEYS.items():
        a = silver_actual[t].select(*keys)
        e = silver_expected[t].select(*keys)
        diff = a.subtract(e).union(e.subtract(a)).count()
        assert diff == 0, f"{t}: {diff} keys differ"


def test_numeric_columns(silver_actual, silver_expected):
    for t, cols in NUMERIC_COLS.items():
        keys = KEYS[t]
        a = silver_actual[t]
        e = silver_expected[t]
        joined = a.join(e, keys)
        for c in cols:
            row = joined.agg(
                F.max(F.abs(a[c].cast("double")
                            - e[c].cast("double"))).alias("d")
            ).first()
            d = row["d"]
            assert d is not None and d <= 0.01, \
                f"{t}.{c}: max abs diff {d}"


def test_string_columns(silver_actual, silver_expected):
    for t, cols in STRING_COLS.items():
        keys = KEYS[t]
        a = silver_actual[t]
        e = silver_expected[t]
        joined = a.join(e, keys)
        for c in cols:
            mismatches = joined.filter(
                ~a[c].eqNullSafe(e[c])).count()
            assert mismatches == 0, f"{t}.{c}: {mismatches} mismatches"

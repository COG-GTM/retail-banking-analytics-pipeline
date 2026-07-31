{#-
    In-database replacement for SAS PROC LOGISTIC (sas/03_sas_risk_scoring.sas).

    dbt models cannot fit a logistic regression, so probability-of-default
    scoring lives outside the DAG in this run-operation. It fits a binomial
    TD_GLM on int_customer_risk_features using the same predictors and the same
    DEFAULT_FLAG target as the SAS program, scores every customer, and
    overwrites the customer_default_probabilities hand-off table that
    customer_risk_scores joins back to.

    Usage:
        dbt run --select int_customer_risk_features
        dbt run-operation run_td_glm_default_scores
        dbt run --select customer_risk_scores

    Teams that score outside Teradata skip this macro and load the hand-off
    table with `dbt seed` instead.

    Note: TD_GLM fits the full specification. SAS used stepwise selection
    (slentry=0.10, slstay=0.05), so coefficients - and therefore the
    probabilities - will not match the legacy model exactly.
-#}

{% macro run_td_glm_default_scores(max_iter=100, model_version=none) %}

    {%- set version = model_version or var('risk_model_version') -%}
    {%- set features = ref('int_customer_risk_features') -%}
    {%- set scores = ref('customer_default_probabilities') -%}
    {%- set model_table = api.Relation.create(
            database=scores.database,
            schema=scores.schema,
            identifier='TD_GLM_DEFAULT_MODEL') -%}
    {%- set predictors = [
            'BUREAU_SCORE_NORM',
            'CREDIT_UTIL_RATIO',
            'PAYMENT_ONTIME_PCT',
            'BALANCE_VOLATILITY',
            'VELOCITY_RATIO',
            'ACCOUNT_OVERDRAFT_CNT',
            'LARGE_WITHDRAWAL_CNT',
            'HIGH_RISK_MERCHANT_CNT',
            'TENURE_MONTHS'
        ] -%}
    {%- set quoted_predictors = predictors | join("', '") -%}

    {% do log("Fitting TD_GLM probability-of-default model on " ~ features, info=true) %}

    {%- set existing_model = load_relation(model_table) -%}
    {% if existing_model is not none %}
        {% do adapter.drop_relation(existing_model) %}
    {% endif %}

    {% call statement('train_glm', fetch_result=false) %}
        CREATE TABLE {{ model_table }} AS (
            SELECT * FROM TD_GLM (
                ON (
                    SELECT
                        CUSTOMER_ID,
                        {{ predictors | join(',\n                        ') }},
                        DEFAULT_FLAG
                    FROM {{ features }}
                ) AS InputTable
                USING
                    InputColumns('{{ quoted_predictors }}')
                    ResponseColumn('DEFAULT_FLAG')
                    Family('BINOMIAL')
                    MaxIterNum({{ max_iter }})
                    Tolerance(0.001)
            ) AS dt
        ) WITH DATA
    {% endcall %}

    {% call statement('truncate_scores', fetch_result=false) %}
        DELETE FROM {{ scores }} ALL
    {% endcall %}

    {% call statement('load_scores', fetch_result=false) %}
        INSERT INTO {{ scores }}
        (CUSTOMER_ID, PROBABILITY_OF_DEFAULT, MODEL_VERSION, SCORED_AT)
        SELECT
            p.CUSTOMER_ID,
            CAST(p.prediction AS DECIMAL(7,6)),
            '{{ version }}',
            CURRENT_TIMESTAMP(6)
        FROM TD_GLMPredict (
            ON (
                SELECT
                    CUSTOMER_ID,
                    {{ predictors | join(',\n                    ') }}
                FROM {{ features }}
            ) AS InputTable
            ON {{ model_table }} AS ModelTable DIMENSION
            USING
                IDColumn('CUSTOMER_ID')
                Accumulate('CUSTOMER_ID')
                OutputProb('true')
        ) AS p
    {% endcall %}

    {% do log("Default probabilities refreshed in " ~ scores, info=true) %}

{% endmacro %}

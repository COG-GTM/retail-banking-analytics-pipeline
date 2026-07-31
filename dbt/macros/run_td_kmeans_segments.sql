{#-
    In-database replacement for SAS PROC FASTCLUS (sas/01_sas_customer_segments.sas).

    dbt models cannot run k-means, so clustering lives outside the DAG in this
    run-operation. It trains TD_KMeans on the standardized features produced by
    int_customer_segment_features, predicts a cluster per customer, labels the
    clusters by descending average balance (the ordering the SAS program used),
    and overwrites the customer_segment_assignments hand-off table that
    customer_segments joins back to.

    Usage:
        dbt run --select int_customer_segment_features
        dbt run-operation run_td_kmeans_segments
        dbt run --select customer_segments

    Teams that score outside Teradata skip this macro entirely and load the
    hand-off table with `dbt seed` instead.
-#}

{% macro run_td_kmeans_segments(num_clusters=5, max_iter=50, model_version=none) %}

    {%- set version = model_version or var('segment_model_version') -%}
    {%- set features = ref('int_customer_segment_features') -%}
    {%- set assignments = ref('customer_segment_assignments') -%}
    {%- set model_table = api.Relation.create(
            database=assignments.database,
            schema=assignments.schema,
            identifier='TD_KMEANS_SEGMENT_MODEL') -%}

    {% do log("Training TD_KMeans (k=" ~ num_clusters ~ ") on " ~ features, info=true) %}

    {%- set existing_model = load_relation(model_table) -%}
    {% if existing_model is not none %}
        {% do adapter.drop_relation(existing_model) %}
    {% endif %}

    {% call statement('train_kmeans', fetch_result=false) %}
        CREATE TABLE {{ model_table }} AS (
            SELECT * FROM TD_KMeans (
                ON (
                    SELECT
                        CUSTOMER_ID,
                        STD_LOG_BALANCE,
                        STD_TENURE_MONTHS,
                        STD_CREDIT_UTILIZATION_PCT,
                        STD_PRODUCT_BREADTH,
                        STD_ACCT_RATIO,
                        STD_AGE
                    FROM {{ features }}
                ) AS InputTable
                USING
                    IdColumn('CUSTOMER_ID')
                    TargetColumns(
                        'STD_LOG_BALANCE',
                        'STD_TENURE_MONTHS',
                        'STD_CREDIT_UTILIZATION_PCT',
                        'STD_PRODUCT_BREADTH',
                        'STD_ACCT_RATIO',
                        'STD_AGE'
                    )
                    NumClusters({{ num_clusters }})
                    StopThreshold(0.001)
                    MaxIterNum({{ max_iter }})
                    Seed(42)
            ) AS dt
        ) WITH DATA
    {% endcall %}

    {% call statement('score_and_label', fetch_result=false) %}
        DELETE FROM {{ assignments }} ALL
    {% endcall %}

    {% call statement('load_assignments', fetch_result=false) %}
        INSERT INTO {{ assignments }}
        (CUSTOMER_ID, SEGMENT_ID, SEGMENT_NAME, SUBSEGMENT_ID, MODEL_VERSION, SCORED_AT)
        WITH scored AS (
            SELECT
                p.CUSTOMER_ID,
                p.td_clusterid_kmeans AS CLUSTER_ID
            FROM TD_KMeansPredict (
                ON (
                    SELECT
                        CUSTOMER_ID,
                        STD_LOG_BALANCE,
                        STD_TENURE_MONTHS,
                        STD_CREDIT_UTILIZATION_PCT,
                        STD_PRODUCT_BREADTH,
                        STD_ACCT_RATIO,
                        STD_AGE
                    FROM {{ features }}
                ) AS InputTable
                ON {{ model_table }} AS ModelTable DIMENSION
                USING
                    Accumulate('CUSTOMER_ID')
            ) AS p
        ),
        cluster_profile AS (
            -- SAS labelled clusters by descending average balance
            SELECT
                s.CLUSTER_ID,
                row_number() OVER (ORDER BY avg(f.LOG_BALANCE) DESC) AS LABEL_RANK
            FROM scored s
            INNER JOIN {{ features }} f
                ON s.CUSTOMER_ID = f.CUSTOMER_ID
            GROUP BY s.CLUSTER_ID
        )
        SELECT
            s.CUSTOMER_ID,
            CAST(s.CLUSTER_ID AS SMALLINT),
            CASE cp.LABEL_RANK
                WHEN 1 THEN 'PREMIUM_WEALTH'
                WHEN 2 THEN 'ENGAGED_MAINSTREAM'
                WHEN 3 THEN 'GROWING_DIGITAL'
                WHEN 4 THEN 'CREDIT_DEPENDENT'
                ELSE 'VALUE_BASIC'
            END,
            CAST(0 AS SMALLINT),
            '{{ version }}',
            CURRENT_TIMESTAMP(6)
        FROM scored s
        INNER JOIN cluster_profile cp
            ON s.CLUSTER_ID = cp.CLUSTER_ID
    {% endcall %}

    {% do log("Segment assignments refreshed in " ~ assignments, info=true) %}

{% endmacro %}

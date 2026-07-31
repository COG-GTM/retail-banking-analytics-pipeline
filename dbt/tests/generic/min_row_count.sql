{#-
    Row-count floor, replacing the min_rows check in sas/macros/validate_table.sas.
    Fails when the relation holds fewer than min_rows rows.
-#}

{% test min_row_count(model, min_rows=1) %}

select count(*) as ROW_CNT
from {{ model }}
having count(*) < {{ min_rows }}

{% endtest %}

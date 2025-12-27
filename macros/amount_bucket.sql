{% macro get_amount_bucket(column_name) %}
    CASE
        WHEN {{ column_name }} <= 10 THEN 'Small' -- 0.25 quantile
        WHEN {{ column_name }} > 10 AND {{ column_name }} <= 47 THEN 'Medium-Small' -- 0.5 quantile
        WHEN {{ column_name }} > 47 AND {{ column_name }} <= 83 THEN 'Medium-Large' -- 0.75 quantile
        ELSE 'Large' -- 1.0 quantile
    END
{% endmacro %}

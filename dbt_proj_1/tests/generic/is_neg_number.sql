{% test is_neg_number(model, column_name) %}

    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0 and {{ column_name }} is not null

{% endtest %}
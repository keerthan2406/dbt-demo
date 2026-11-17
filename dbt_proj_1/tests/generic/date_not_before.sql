{% test date_not_before(model, column_name, compare_column) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} < {{ compare_column }}

{% endtest %}

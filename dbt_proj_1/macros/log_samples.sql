{% macro log_samples(results, n=5) %}
  {% if execute and var('log_samples', false) %}
    {% for r in results if r.node.resource_type == 'model'
         and r.status == 'success' and r.node.config.materialized != 'ephemeral' %}
      {% set t = run_query("select * from " ~ r.node.relation_name ~ " limit " ~ n) %}
      {{ log("---- " ~ r.node.name ~ " ----", info=True) }}
      {% do t.print_table(max_rows=n, max_columns=12) %}
    {% endfor %}
  {% endif %}
{% endmacro %}

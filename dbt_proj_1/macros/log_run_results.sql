{# on-run-end: write one control_load_history row per model / seed / test result. #}
{% macro log_run_results(results) %}
  {% if not execute %}{{ return('') }}{% endif %}

  {% set load_date = var('load_date', var('load_id', run_started_at.strftime('%Y-%m-%d'))) %}
  {# batch_id: latest BRONZE batch for load_date from the control table (non-raising) #}
  {% set batch_id = 'manual_run' %}
  {% set bq = run_query("SELECT batch_id FROM control.control_load_history WHERE load_date = CAST('" ~ load_date ~ "' AS DATE) AND UPPER(layer) = 'BRONZE' ORDER BY ended_at DESC LIMIT 1") %}
  {% if bq and bq.rows | length > 0 %}{% set batch_id = bq.rows[0][0] %}{% endif %}

  {% set status_map = {'success':'SUCCESS','pass':'SUCCESS','fail':'FAILED','error':'FAILED','warn':'WARN','skipped':'SKIPPED'} %}
  {% set rows = [] %}

  {% for r in results %}
    {% set n = r.node %}
    {% if n.resource_type in ['model', 'seed', 'test'] %}
      {% if n.resource_type == 'test' %}
        {% set layer = 'TEST' %}{% set seq = 7 %}
        {% set attached = n.attached_node or (n.depends_on.nodes[0] if n.depends_on.nodes else n.name) %}
        {% set base = attached.split('.')[-1] %}
        {% set load_type = 'test' %}
        {% set target = 'NULL' %}
      {% else %}
        {% set fqn = n.fqn | join('.') | lower %}
        {% if 'gold' in fqn or 'mart' in fqn %}
          {% set layer = 'GOLD' %}{% set seq = 6 %}
        {% elif 'staging' in fqn %}
          {% set layer = 'STAGING' %}{% set seq = 4 %}
        {% else %}
          {% set layer = 'SILVER' %}{% set seq = 5 %}
        {% endif %}
        {% set base = n.alias %}
        {% set load_type = n.config.materialized %}
        {% set target = "'" ~ (n.relation_name | replace("'", "''")) ~ "'" %}
      {% endif %}
      {% set entity = base | replace('stg_', '') | replace('silver_', '') | replace('dim_', '') | replace('fct_', '') %}
      {% set status = status_map.get(r.status | string | lower, 'FAILED') %}
      {% set msg = (r.message or '') | string | replace('\\', '\\\\') | replace("'", "''") %}
      {% set rows_read = r.failures if (n.resource_type == 'test' and r.failures is not none) else none %}

      {% set rows_written = 'NULL' %}
      {% if n.resource_type == 'model' and load_type == 'table' and r.status == 'success' %}
        {% set h = run_query("SELECT CAST(operationMetrics['numOutputRows'] AS BIGINT) FROM (DESCRIBE HISTORY " ~ n.relation_name ~ " LIMIT 1)") %}
        {% if h and h.rows | length > 0 and h.rows[0][0] is not none %}{% set rows_written = h.rows[0][0] %}{% endif %}
      {% endif %}

      {% set row %}(
        '{{ batch_id }}', '{{ invocation_id }}', CAST('{{ load_date }}' AS DATE),
        '{{ layer }}', {{ seq }}, '{{ entity }}', '{{ n.unique_id }}', NULL,
        {{ target }}, '{{ load_type }}', '{{ status }}',
        {{ rows_read if rows_read is not none else 'NULL' }}, {{ rows_written }}, NULL,
        {{ "'" ~ msg ~ "'" if msg else 'NULL' }},
        CAST('{{ run_started_at }}' AS TIMESTAMP), current_timestamp(),
        ROUND({{ r.execution_time or 0 }}, 2), current_user(), 'dbt'
      ){% endset %}
      {% do rows.append(row) %}
    {% endif %}
  {% endfor %}

  {% if rows | length > 0 %}
    {% set ins %}
      INSERT INTO control.control_load_history (
        batch_id, dbt_invocation_id, load_date, layer, layer_seq, entity_name, object_name, source_path,
        target_table, load_type, status, rows_read, rows_written, rows_deleted, error_message,
        started_at, ended_at, duration_sec, run_by, run_source
      ) VALUES {{ rows | join(',\n') }}
    {% endset %}
    {% do run_query(ins) %}
  {% endif %}
{% endmacro %}

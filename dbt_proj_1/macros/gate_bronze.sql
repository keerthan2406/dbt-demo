{# on-run-start: stop the dbt run unless the latest BRONZE batch for the load_date is fully SUCCESS. #}
{% macro resolve_batch_id() %}
  {% if not execute %}{{ return('') }}{% endif %}
  {% set load_date = var('load_date', var('load_id', run_started_at.strftime('%Y-%m-%d'))) %}
  {% set q %}
    SELECT batch_id,
           MAX(CASE WHEN UPPER(status) <> 'SUCCESS' THEN 1 ELSE 0 END) AS has_bad,
           MAX(ended_at) AS last_end
    FROM control.control_load_history
    WHERE load_date = CAST('{{ load_date }}' AS DATE)
      AND UPPER(layer) = 'BRONZE'
    GROUP BY batch_id
    ORDER BY last_end DESC
    LIMIT 1
  {% endset %}
  {% set res = run_query(q) %}
  {% if res is none or res.rows | length == 0 %}
    {{ exceptions.raise_compiler_error("GATE FAILED: no BRONZE load found for load_date=" ~ load_date) }}
  {% elif res.rows[0][1] | int != 0 %}
    {{ exceptions.raise_compiler_error("GATE FAILED: BRONZE batch " ~ res.rows[0][0] ~ " has non-SUCCESS entities for load_date=" ~ load_date) }}
  {% endif %}
  {{ return(res.rows[0][0]) }}
{% endmacro %}

{% macro gate_bronze() %}
  {% if execute %}
    {% set b = resolve_batch_id() %}
    {{ log("Bronze gate passed, batch_id=" ~ b, info=True) }}

    {# Latest-date check: bronze is truncated and reloaded, so it only holds the newest file.
       load_date must be the newest BRONZE load_date in the control table AND match _file_date in every bronze table. #}
    {% set gate_load_date = var('load_date', var('load_id', run_started_at.strftime('%Y-%m-%d'))) %}
    {% set lq = run_query("SELECT CAST(MAX(load_date) AS STRING) FROM control.control_load_history WHERE UPPER(layer) = 'BRONZE' AND UPPER(status) = 'SUCCESS'") %}
    {% set latest_ctrl = lq.rows[0][0] if lq and lq.rows | length > 0 else none %}
    {% if latest_ctrl is none or latest_ctrl | string != gate_load_date | string %}
      {{ exceptions.raise_compiler_error("GATE FAILED: load_date=" ~ gate_load_date ~ " is not the latest BRONZE load_date in the control table (latest=" ~ latest_ctrl ~ "). Bronze is truncated and reloaded, so only the latest date can be processed.") }}
    {% endif %}
    {% for src in graph.sources.values() if src.source_name == 'bronze' %}
      {% set fq = run_query("SELECT CAST(MIN(_file_date) AS STRING), CAST(MAX(_file_date) AS STRING) FROM " ~ source(src.source_name, src.name)) %}
      {% if fq and fq.rows | length > 0 and (fq.rows[0][0] | string != gate_load_date | string or fq.rows[0][1] | string != gate_load_date | string) %}
        {{ exceptions.raise_compiler_error("GATE FAILED: bronze." ~ src.name ~ " holds _file_date " ~ fq.rows[0][0] ~ " .. " ~ fq.rows[0][1] ~ ", expected only " ~ gate_load_date) }}
      {% endif %}
    {% endfor %}

    {# Already-complete check: every BRONZE entity in this batch already has a GOLD SUCCESS
       logged after the bronze load finished -> nothing left to do. #}
    {% if true %}
      {% set load_date = var('load_date', var('load_id', run_started_at.strftime('%Y-%m-%d'))) %}
      {% set q %}
        WITH br AS (
          SELECT LOWER(entity_name) AS entity_name, MAX(ended_at) AS bronze_end
          FROM control.control_load_history
          WHERE batch_id = '{{ b }}' AND UPPER(layer) = 'BRONZE'
          GROUP BY LOWER(entity_name)
        )
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN EXISTS (
                SELECT 1 FROM control.control_load_history g
                WHERE g.load_date = CAST('{{ load_date }}' AS DATE)
                  AND UPPER(g.layer) = 'GOLD'
                  AND UPPER(g.status) = 'SUCCESS'
                  AND LOWER(g.entity_name) = br.entity_name
                  AND g.ended_at >= br.bronze_end
              ) THEN 1 ELSE 0 END) AS gold_done
        FROM br
      {% endset %}
      {% set res = run_query(q) %}
      {% if res and res.rows | length > 0 and res.rows[0][0] | int > 0
            and res.rows[0][0] | int == (res.rows[0][1] or 0) | int %}
        {{ exceptions.raise_compiler_error("GATE STOP: all " ~ res.rows[0][0] ~ " entities for load_date=" ~ load_date ~ " (batch " ~ b ~ ") already completed through GOLD.") }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}

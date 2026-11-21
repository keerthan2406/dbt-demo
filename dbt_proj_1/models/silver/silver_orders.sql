{{ config(
    materialized='incremental',
    incremental_strategy='replace_where',
    incremental_predicates=["_file_date = date'" ~ var('load_date') ~ "'"]
) }}
select * from {{ ref('stg_orders') }}
{% if is_incremental() %}
where _file_date = date'{{ var("as_of_date") }}'
{% endif %}

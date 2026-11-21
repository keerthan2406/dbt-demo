{{ config(
    materialized='incremental',
    incremental_strategy='replace_where',
    incremental_predicates=["_file_date = date'" ~ var('load_date') ~ "'"]
) }}
select * from {{ ref('stg_order_items') }}
{% if is_incremental() %}
where _file_date = (select max(_file_date) from {{ ref('stg_order_items') }})
{% endif %}

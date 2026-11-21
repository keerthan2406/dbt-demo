{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_id',
    merge_update_columns=['product_name','category','unit_price','unit_cost','updated_at'],
    on_schema_change='append_new_columns'
) }}
select * from {{ ref('stg_products') }}
{% if is_incremental() %}
where updated_at >= (select max(updated_at) from {{ this }})
{% endif %}

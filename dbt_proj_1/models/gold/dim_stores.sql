{# dim_stores: one row per store, from silver_stores. #}
SELECT
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} AS store_key,
    store_id,
    store_name,
    city,
    country_code,
    region,
    currency,
    TRY_CAST(open_date AS DATE) AS open_date,
    manager_name
FROM {{ ref('silver_stores') }}

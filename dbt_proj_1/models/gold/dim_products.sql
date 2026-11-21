{# dim_products: one row per product (SCD Type 1 - latest values), from silver_products. #}
SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    product_id,
    product_name,
    category,
    unit_price,
    unit_cost,
    ROUND(margin_pct, 2) AS margin_pct,
    updated_at
FROM {{ ref('silver_products') }}

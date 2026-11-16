WITH PRODUCTS AS (
    SELECT 
        * 
    FROM {{ source('bronze','products')}}
),
CLEANED AS (
    SELECT 
    TRIM(UPPER(product_id)) as product_id,
    TRIM(INITCAP(product_name)) as product_name,
    TRIM(INITCAP(category)) as category,
    {{dbt.cast("unit_price","decimal(18,0)")}} as unit_price,
    {{dbt.cast("unit_cost","decimal(18,0)")}} as unit_cost,
    updated_at,
     ({{ dbt.cast("unit_price", "decimal(18,2)") }} - {{ dbt.cast("unit_cost", "decimal(18,2)") }}) 
            / NULLIF({{ dbt.cast("unit_price", "decimal(18,2)") }}, 0) * 100 AS margin_pct
    FROM PRODUCTS

)
SELECT * FROM CLEANED



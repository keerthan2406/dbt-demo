{# obt_sales: one big table = fct_order_items + customer / product / store attributes. #}

SELECT
    f.*,
    c.full_name  AS customer_name,
    c.segment    AS customer_segment,
    c.city       AS customer_city,
    c.region     AS customer_region,
    p.product_name,
    p.category   AS product_category,
    s.store_name,
    s.region     AS store_region
FROM {{ ref('fct_order_items') }} f
LEFT JOIN {{ ref('dim_customers') }} c ON c.customer_key = f.customer_key
LEFT JOIN {{ ref('dim_products') }} p  ON p.product_key  = f.product_key
LEFT JOIN {{ ref('dim_stores') }} s    ON s.store_key    = f.store_key

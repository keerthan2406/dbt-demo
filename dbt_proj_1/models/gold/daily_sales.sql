{# daily_sales: grain = order_date x store region. Revenue orders only (status seed is_revenue = true). #}

SELECT
    f.order_date,
    COALESCE(s.region, 'Unknown')                          AS region,
    COUNT(DISTINCT f.order_id)                             AS order_count,
    COUNT(*)                                               AS item_count,
    SUM(f.quantity)                                        AS units_sold,
    SUM(f.gross_amount)                                    AS gross_amount,
    SUM(f.discount_amount)                                 AS discount_amount,
    SUM(f.net_amount)                                      AS net_amount,
    SUM(f.cost_amount)                                     AS cost_amount,
    SUM(f.gross_profit)                                    AS gross_profit,
    SUM(f.net_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0) AS avg_order_value
FROM {{ ref('fct_order_items') }} f
JOIN {{ ref('fct_orders') }} o      ON o.order_id = f.order_id AND o.is_revenue
LEFT JOIN {{ ref('dim_stores') }} s ON s.store_key = f.store_key
GROUP BY f.order_date, COALESCE(s.region, 'Unknown')

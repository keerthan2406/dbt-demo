{# fct_order_items: one row per order line. Amounts come from silver_order_items (unit_price already falls back to product price). #}

SELECT
    i.order_item_id,
    i.order_id,
    p.product_key,
    o.customer_key,
    o.store_key,
    o.order_date,
    i.quantity,
    i.unit_price,
    i.gross_amount,
    i.discount_amount,
    i.net_amount,
    i.cost_amount,
    i.gross_profit,
    ROUND(i.gross_profit / NULLIF(i.net_amount, 0) * 100, 2) AS margin_pct,
    i._file_date
FROM {{ ref('silver_order_items') }} i
JOIN {{ ref('fct_orders') }} o        ON o.order_id = i.order_id
LEFT JOIN {{ ref('dim_products') }} p ON p.product_id = i.product_id

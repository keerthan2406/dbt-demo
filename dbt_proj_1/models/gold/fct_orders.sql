{# fct_orders: one row per order. Keys resolved to dims; measures rolled up from silver_order_items. #}

WITH orders AS (
    SELECT * FROM {{ ref('silver_orders') }}
),

items AS (
    SELECT
        order_id,
        COUNT(*)             AS item_count,
        SUM(gross_amount)    AS gross_amount,
        SUM(discount_amount) AS discount_amount,
        SUM(net_amount)      AS net_amount
    FROM {{ ref('silver_order_items') }}
    GROUP BY order_id
),

-- Point-in-time customer version: latest version valid on order_date;
-- if the order predates every version, fall back to the earliest one.
customer_version AS (
    SELECT
        o.order_id,
        c.customer_key
    FROM orders o
    JOIN {{ ref('dim_customers') }} c
      ON c.customer_id = o.customer_id
     AND NOT c.is_deleted
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.order_id
        ORDER BY
            CASE WHEN o.order_date >= CAST(c.valid_from AS DATE) THEN 0 ELSE 1 END,
            CASE WHEN o.order_date >= CAST(c.valid_from AS DATE) THEN c.valid_from END DESC,
            c.valid_from ASC
    ) = 1
)

SELECT
    o.order_id,
    cv.customer_key,
    s.store_key,
    o.order_ts,
    o.order_date,
    o.ship_date,
    o.days_to_ship,
    o.status,
    o.is_revenue,
    o.payment_method,
    o.discount_pct,
    COALESCE(i.item_count, 0)        AS item_count,
    COALESCE(i.gross_amount, 0)      AS gross_amount,
    COALESCE(i.discount_amount, 0)   AS discount_amount,
    COALESCE(i.net_amount, 0)        AS net_amount,
    o._file_date
FROM orders o
LEFT JOIN items i            ON i.order_id = o.order_id
LEFT JOIN customer_version cv ON cv.order_id = o.order_id
LEFT JOIN {{ ref('dim_stores') }} s ON s.store_id = o.store_id

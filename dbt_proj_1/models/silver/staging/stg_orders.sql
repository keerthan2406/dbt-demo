WITH ORDER AS (
    SELECT
        *
    FROM {{ source('bronze', 'orders') }}
),
CLEANED AS(
    SELECT 
        TRIM(UPPER(order_id)) AS order_id,
        TRIM(UPPER(customer_id)) AS customer_id,
        store_id,
        TO_DATE(order_ts) as order_date,
        TRIM(LOWER(status)) as status,
        {{dbt.cast("discount_pct","decimal(18, 2)")}} as discount_pct,
        TRIM(LOWER(payment_method)) as payment_method,
        TO_DATE(ship_date) as ship_date,
        datediff(ship_date,order_date) as days_to_ship,
        _file_date,
        _source_file,
        _ingested_at
    FROM ORDER
)
SELECT * FROM CLEANED
QUALIFY ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY _ingested_at DESC) = 1

{# tg_orders (main.raw.orders)

Duplicates: the same order appears twice in every file. Dedupe with qualify row_number() over (partition by order_id order by _ingested_at desc) = 1.
order_id, customer_id: TRIM and UPPER.
status: TRIM and LOWER, then map through the order_status_map seed. This fixes canceled/cancelled, SHIPPED and Pending.
payment_method: TRIM and standardize case, and leave NULLs as NULL.
store_id: leave NULLs as NULL.
order_date, ship_date: cast to DATE, and leave a NULL ship_date as NULL.
days_to_ship: calculate as datediff(ship_date, order_date), which is NULL when ship_date is NULL.
discount_pct: cast to DECIMAL, and leave NULL as NULL.
Do not filter the orphan C999. The relationships test only warns about it.
Keep _file_date, _source_file and _ingested_at. The date-append load uses _file_date. #}



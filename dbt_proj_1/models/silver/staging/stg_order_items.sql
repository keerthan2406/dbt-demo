WITH ORDER_ITEMS AS(
    SELECT * FROM {{ source('bronze', 'order_items') }}
),
CLEANED AS (
    SELECT
    order_item_id,
    order_id,
    product_id,
    {{dbt.cast("quantity","decimal(18,0)")}} AS quantity,
    {{dbt.cast("unit_price","decimal(18,2)")}} AS unit_price,
    _source_file,
    _file_date,
    _ingested_at

    FROM ORDER_ITEMS
),
DEDUP AS(
    SELECT * FROM CLEANED
    QUALIFY ROW_NUMBER() OVER(PARTITION BY order_item_id,order_id,product_id ORDER BY order_id)=1
    ),
PRODUCTS AS(
    SELECT 
    *
    FROM {{ ref('stg_products') }}
),
ORDER AS(
    SELECT 
    *
    FROM {{ ref('stg_orders') }}
),
ORDER_UNIT_PRICE AS(
    SELECT 
    D.order_item_id AS order_item_id,
    D.order_id AS order_id, 
    D.product_id AS product_id,
    D.quantity AS quantity,
    coalesce(D.unit_price,P.unit_price)  AS unit_price,
    P.unit_cost AS unit_cost,
    D._source_file AS _source_file,
    D._file_date AS _file_date,
    D._ingested_at AS _ingested_at

    FROM DEDUP D JOIN PRODUCTS P ON D.product_id=P.product_id
)
SELECT 
O.order_item_id AS order_item_id,
O.order_id AS order_id,
O.product_id AS product_id,
O.quantity AS quantity,
O.unit_price AS unit_price,
O.unit_cost AS unit_cost,
R.discount_pct AS discount_pct,
unit_price*IFNULL(quantity, 1) AS gross_amount,
gross_amount*IFNULL(discount_pct, 0)/100 AS discount_amount,
gross_amount - discount_amount AS net_amount,
IFNULL(O.quantity, 1)*IFNULL(O.unit_cost, 0) AS cost_amount,
net_amount - cost_amount AS gross_profit,
O._source_file AS _source_file,
O._file_date AS _file_date,
O._ingested_at AS _ingested_at
FROM ORDER_UNIT_PRICE O JOIN ORDER R ON O.order_id=R.order_id
WHERE quantity IS NOT NULL AND unit_price IS NOT NULL AND quantity>0 AND unit_price>=0


{# dim_customers: one row per customer version (SCD Type 2), built from the customers_scd2 snapshot. #}
{% set as_of = var('load_date', var('load_id', run_started_at.strftime('%Y-%m-%d'))) %}

WITH customers AS (
    SELECT * FROM {{ ref('customers_scd2') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} AS customer_key,
    customer_id,
    CONCAT_WS(' ', first_name, last_name)                                     AS full_name,
    email,
    phone,
    city,
    country_code,
    region,
    segment,
    signup_date,
    DATEDIFF(TO_DATE('{{ as_of }}'), signup_date)                             AS customer_age_days,
    dbt_valid_from                                                            AS valid_from,
    COALESCE(dbt_valid_to, CAST('9999-12-31' AS TIMESTAMP))                   AS valid_to,
    (dbt_valid_to IS NULL)                                                    AS is_current,
    (CAST(dbt_is_deleted AS STRING) = 'True')                                 AS is_deleted
FROM customers

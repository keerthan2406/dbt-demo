WITH CUSTOMERS AS (
    SELECT 
        *
    FROM {{ source('bronze', 'customers') }}
),
CLEANED AS (
    SELECT
    TRIM(UPPER(customer_id)) AS customer_id,
    TRIM(INITCAP(first_name)) AS first_name,
    TRIM(INITCAP(last_name)) AS last_name,
    CASE 
        WHEN LOWER(TRIM(email)) IN ('', 'n/a', 'na', 'null') THEN NULL
        ELSE LOWER(TRIM(email))
    END AS email,
    phone,
    city,
    TRIM(UPPER(country_code)) AS country_code,
    TRIM(INITCAP(segment)) AS segment,
    signup_date,
    updated_at,
    _source_file,
    _file_date,
    _ingested_at
    FROM CUSTOMERS

),
COUNTRY_REGION AS (
    SELECT 
        TRIM(UPPER(country_code)) AS country_code,
        country_name,
        region
    FROM {{ ref('country_region_map') }}
)

SELECT 
    C.*,
    CR.country_name,
    CR.region
FROM  CLEANED C JOIN COUNTRY_REGION CR ON C.country_code = CR.country_code





{# stg_customers (main.raw.customers)

customer_id: TRIM and UPPER.
Name columns: TRIM and initcap.
email: TRIM and LOWER, and set N/A and empty string to NULL.
phone: TRIM, and set empty string to NULL.
segment: TRIM and standardize case, and leave NULLs as NULL.
country_code: TRIM and UPPER, then map through the country_region_map seed.
customer_age_days: calculate from the customer's created date (or the as-of date). #}
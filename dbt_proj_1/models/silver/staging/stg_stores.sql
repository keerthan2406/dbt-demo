WITH STORES AS (
    SELECT
        *
    FROM {{ source('bronze', 'stores') }}
),
CLEANED AS(
    SELECT 
        TRIM(UPPER(store_id)) AS store_id,
        TRIM(UPPER(store_name)) AS store_name,
        TRIM(UPPER(manager_name)) AS manager_name,
        TRIM(UPPER(country_code)) AS country_code,
        _file_date,
        _source_file,
        _ingested_at
    FROM STORES
),
COUNTRY_REGION AS (
    SELECT 
        TRIM(UPPER(country_code)) AS country_code,
        country_name,
        region
    FROM {{ ref('country_region_map') }}
),
JOINED AS (
    SELECT 
        C.store_id,
        C.store_name,
        C.manager_name,
        C.country_code,
        CR.country_name,
        CR.region,
        C._file_date,
        C._source_file,
        C._ingested_at
    FROM CLEANED  C
    LEFT JOIN COUNTRY_REGION  CR
    ON C.country_code = CR.country_code
)
SELECT * FROM JOINED;



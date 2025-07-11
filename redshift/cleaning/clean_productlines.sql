WITH source AS (
    SELECT
        *
    FROM raw_zone.productlines
    WHERE productline IS NOT NULL
),

deduplicate AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY productline) AS row_num
    FROM source
)

SELECT productLine, textDescription
FROM deduplicate
WHERE row_num = 1
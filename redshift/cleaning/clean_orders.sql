WITH source AS (
    SELECT
        *
    FROM raw_zone.orders
    WHERE ordernumber IS NOT NULL
),

deduplicate AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY ordernumber) AS row_num
    FROM source
)

SELECT 
    orderNumber,
    orderDate,
    requiredDate,
    shippedDate,
    status,
    comments,
    customerNumber
FROM deduplicate
WHERE row_num = 1
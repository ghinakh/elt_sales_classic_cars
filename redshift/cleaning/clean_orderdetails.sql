WITH source AS (
    SELECT
        *
    FROM raw_zone.orderdetails
    WHERE ordernumber IS NOT NULL AND productCode IS NOT NULL
),

deduplicate AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY ordernumber, productCode) AS row_num
    FROM source
)

SELECT 
    orderNumber,
    productCode,
    orderLineNumber,
    quantityOrdered,
    priceEach,
    (priceEach*quantityOrdered) AS totalPrices
FROM deduplicate
WHERE row_num = 1
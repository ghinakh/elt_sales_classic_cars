WITH source AS (
    SELECT
        *
    FROM raw_zone.products
    WHERE productcode IS NOT NULL
),

deduplicate AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY productcode) AS row_num
    FROM source
)

SELECT productCode, productName, productLine, productScale, 
       productVendor, productDescription, quantityInStock,
       buyPrice, MSRP
FROM deduplicate
WHERE row_num = 1
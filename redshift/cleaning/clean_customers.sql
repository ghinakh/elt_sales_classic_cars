WITH source AS (
    SELECT
        *
    FROM raw_zone.customers
    WHERE customernumber IS NOT NULL
),

deduplicate AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY customernumber) AS row_num
    FROM source
)

SELECT 
    customerNumber,
    customerName,
    contactLastName,
    contactFirstName,
    phone,
    addressLine1,
    addressLine2,
    city,
    state,
    postalCode,
    country,
    creditLimit
FROM deduplicate
WHERE row_num = 1
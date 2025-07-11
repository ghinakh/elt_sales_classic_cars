MERGE INTO raw_zone.customers
USING raw_zone.tmp_customers AS source
ON raw_zone.customers.customerNumber = source.customerNumber
WHEN MATCHED THEN 
    UPDATE SET
        customerName = source.customerName,
        contactLastName = source.contactLastName,
        contactFirstName = source.contactFirstName,
        phone = source.phone,
        addressLine1 = source.addressLine1,
        addressLine2 = source.addressLine2,
        city = source.city,
        state = source.state,
        postalCode = source.postalCode,
        country = source.country,
        creditLimit = source.creditLimit
WHEN NOT MATCHED THEN
    INSERT (customerNumber, customerName, contactLastName, contactFirstName, phone, 
    addressLine1, addressLine2, city, state, postalCode, country, salesRepEmployeeNumber, creditLimit)
    VALUES (source.customerNumber, source.customerName, source.contactLastName, source.contactFirstName, source.phone, 
    source.addressLine1, source.addressLine2, source.city, source.state, source.postalCode, source.country, source.salesRepEmployeeNumber, source.creditLimit)
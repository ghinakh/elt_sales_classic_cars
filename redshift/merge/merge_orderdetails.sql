MERGE INTO raw_zone.orderdetails
USING raw_zone.tmp_orderdetails AS source
ON raw_zone.orderdetails.orderNumber = source.orderNumber
   AND raw_zone.orderdetails.productCode = source.productCode
WHEN MATCHED THEN 
    UPDATE SET
        quantityOrdered = source.quantityOrdered,
        priceEach = source.priceEach,
        orderLineNumber = source.orderLineNumber
WHEN NOT MATCHED THEN
    INSERT (orderNumber, productCode, quantityOrdered, priceEach, orderLineNumber, orderDate)
    VALUES (source.orderNumber, source.productCode, source.quantityOrdered, source.priceEach, source.orderLineNumber, source.orderDate)
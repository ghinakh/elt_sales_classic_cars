MERGE INTO processed_zone.fact_sales
USING tmp_fact_sales AS source
ON processed_zone.fact_sales.orderNumber = source.orderNumber AND processed_zone.fact_sales.productCode = source.productCode
WHEN MATCHED THEN 
    UPDATE SET
        orderNumber = source.orderNumber,
        productCode = source.productCode,
        orderLineNumber = source.orderLineNumber,
        customerNumber = source.customerNumber,
        quantityOrdered = source.quantityOrdered,
        priceEach = source.priceEach,
        totalPrices = source.totalPrices,
        orderDate = source.orderDate,
        requiredDate = source.requiredDate,
        shippedDate = source.shippedDate,
        status = source.status,
        comments = source.comments
WHEN NOT MATCHED THEN
    INSERT (orderNumber, productCode, orderLineNumber, customerNumber, quantityOrdered, priceEach, totalPrices, orderDate, requiredDate, shippedDate, status, comments)
    VALUES (source.orderNumber, source.productCode, source.orderLineNumber, source.customerNumber, source.quantityOrdered, source.priceEach, source.totalPrices, source.orderDate, source.requiredDate, source.shippedDate, source.status, source.comments)
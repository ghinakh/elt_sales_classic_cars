MERGE INTO raw_zone.orders
USING raw_zone.tmp_orders AS source
ON raw_zone.orders.orderNumber = source.orderNumber
WHEN MATCHED THEN 
    UPDATE SET
        requiredDate = source.requiredDate,
        shippedDate = source.shippedDate,
        status = source.status,
        comments = source.comments
WHEN NOT MATCHED THEN
    INSERT (orderNumber, orderDate, requiredDate, shippedDate, status, comments, customerNumber)
    VALUES (source.orderNumber, source.orderDate, source.requiredDate, source.shippedDate, 
    source.status, source.comments, source.customerNumber)
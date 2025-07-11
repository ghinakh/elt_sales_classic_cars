MERGE INTO raw_zone.products
USING raw_zone.tmp_products AS source
ON raw_zone.products.productCode = source.productCode
WHEN MATCHED THEN 
    UPDATE SET
        productName = source.productName,
        productLine = source.productLine,
        productScale = source.productScale,
        productVendor = source.productVendor,
        productDescription = source.productDescription,
        quantityInStock = source.quantityInStock,
        buyPrice = source.buyPrice,
        MSRP = source.MSRP
WHEN NOT MATCHED THEN
    INSERT (productCode, productName, productLine, productScale, productVendor, productDescription, quantityInStock, buyPrice, MSRP)
    VALUES (source.productCode, source.productName, source.productLine, source.productScale, source.productVendor, 
    source.productDescription, source.quantityInStock, source.buyPrice, source.MSRP)
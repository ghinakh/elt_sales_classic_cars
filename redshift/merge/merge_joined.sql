MERGE INTO processed_zone.dim_products
USING tmp_dim_products AS source
ON processed_zone.dim_products.productCode = source.productCode
WHEN MATCHED THEN 
    UPDATE SET
        productName = source.productName,
        productLine = source.productLine,
        productScale = source.productScale,
        productVendor = source.productVendor,
        productDescription = source.productDescription,
        quantityInStock = source.quantityInStock,
        buyPrice = source.buyPrice,
        MSRP = source.MSRP,
        textDescriptionProductLine = source.textDescriptionProductLine
WHEN NOT MATCHED THEN
    INSERT (productCode, productName, productLine, productScale, productVendor, productDescription, quantityInStock, buyPrice, MSRP, textDescriptionProductLine)
    VALUES (source.productCode, source.productName, source.productLine, source.productScale, source.productVendor, 
    source.productDescription, source.quantityInStock, source.buyPrice, source.MSRP, source.textDescriptionProductLine)
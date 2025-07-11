MERGE INTO raw_zone.productlines
USING raw_zone.tmp_productlines AS source
ON raw_zone.productlines.productLine = source.productLine
WHEN MATCHED THEN 
    UPDATE SET
        textDescription = source.textDescription,
        htmlDescription = source.htmlDescription,
        image = source.image
WHEN NOT MATCHED THEN
    INSERT (productLine, textDescription, htmlDescription, image)
    VALUES (source.productLine, source.textDescription, source.htmlDescription, source.image)
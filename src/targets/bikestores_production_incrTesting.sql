-- COPY INTO RAW

    COPY INTO bikestores_RAW.production.incrtesting
        FROM @bikestores_TGT.PUBLIC.MY_S3_STAGE/bikestores/production/incrtesting/incremental/
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE
    FORCE = FALSE;
    

-- MERGE INTO TARGET

    MERGE INTO bikestores_TGT.production.incrtesting t
    USING (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY product_id
                       ORDER BY LOAD_TS DESC
                   ) rn
            FROM bikestores_RAW.production.incrtesting
        )
        WHERE rn = 1
    ) r
    ON t.product_id=r.product_id

    WHEN MATCHED AND r.SOFT_DELETE = 1 THEN
        UPDATE SET 
            t.IS_DELETED = 1,
            t.DELETE_TS = CURRENT_TIMESTAMP,
            t.UPDATE_TS = CURRENT_TIMESTAMP

    WHEN MATCHED AND r.SOFT_DELETE = 0 THEN
        UPDATE SET
            t.PRODUCT_ID=r.PRODUCT_ID, t.PRODUCT_NAME=r.PRODUCT_NAME, t.BRAND_ID=r.BRAND_ID, t.LIST_PRICE=r.LIST_PRICE, t.LASTMODIFIEDDATE=r.LASTMODIFIEDDATE,
            t.UPDATE_TS = CURRENT_TIMESTAMP,
            t.IS_DELETED = 0

    WHEN NOT MATCHED AND r.SOFT_DELETE = 0 THEN
        INSERT (PRODUCT_ID, PRODUCT_NAME, BRAND_ID, LIST_PRICE, LASTMODIFIEDDATE, INSERT_TS)
        VALUES (r.PRODUCT_ID, r.PRODUCT_NAME, r.BRAND_ID, r.LIST_PRICE, r.LASTMODIFIEDDATE, CURRENT_TIMESTAMP);
    
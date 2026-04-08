-- COPY INTO RAW

    COPY INTO bikestores_RAW.production.timestampTest
        FROM @bikestores_TGT.PUBLIC.MY_S3_STAGE/bikestores/production/timestamptest/incremental/
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE
    FORCE = FALSE;
    

-- MERGE INTO TARGET

    MERGE INTO bikestores_TGT.production.timestampTest t
    USING (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY product_id
                       ORDER BY LOAD_TS DESC
                   ) rn
            FROM bikestores_RAW.production.timestampTest
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
            ,
            t.UPDATE_TS = CURRENT_TIMESTAMP,
            t.IS_DELETED = 0

    WHEN NOT MATCHED AND r.SOFT_DELETE = 0 THEN
        INSERT (INSERT_TS)
        VALUES (CURRENT_TIMESTAMP);
    
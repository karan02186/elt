-- COPY INTO RAW

COPY INTO bikestores_RAW.production.table_booking
FROM @bikestores_TGT.EXT_STAGE_SCHEMA.external_aws_stage/data/bikestores/production/table_booking/incremental/
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[.]parquet'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FORCE = TRUE;


-- MERGE INTO TARGET

MERGE INTO bikestores_TGT.production.table_booking t
USING (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY booking_id
                   ORDER BY __load_ts DESC
               ) rn
        FROM bikestores_RAW.production.table_booking
    )
    WHERE rn = 1
) r
ON t.booking_id = r.booking_id

WHEN MATCHED AND r.soft_delete = 1 THEN
    UPDATE SET
        t.soft_delete = 1,
        t.__delete_timestamp  = CURRENT_TIMESTAMP,
        t.__update_timestamp  = CURRENT_TIMESTAMP

WHEN MATCHED AND r.soft_delete = 0 THEN
    UPDATE SET
        t.CUSTOMER_NAME = r.CUSTOMER_NAME,
        t.PHONE_NUMBER = r.PHONE_NUMBER,
        t.EMAIL_ADDRESS = r.EMAIL_ADDRESS,
        t.BOOKING_DATE = r.BOOKING_DATE,
        t.GUEST_COUNT = r.GUEST_COUNT,
        t.TABLE_NUMBER = r.TABLE_NUMBER,
        t.STATUS = r.STATUS,
        t.SPECIAL_REQUESTS = r.SPECIAL_REQUESTS,
        t.SOFT_DELETE = r.SOFT_DELETE,
        t.BOOKING_TIME = r.BOOKING_TIME,
        t.__update_timestamp  = CURRENT_TIMESTAMP

WHEN NOT MATCHED AND r.soft_delete = 0 THEN
    INSERT (BOOKING_ID, CUSTOMER_NAME, PHONE_NUMBER, EMAIL_ADDRESS, BOOKING_DATE, GUEST_COUNT, TABLE_NUMBER, STATUS, SPECIAL_REQUESTS, SOFT_DELETE, BOOKING_TIME, __insert_timestamp, __update_timestamp)
    VALUES (r.BOOKING_ID, r.CUSTOMER_NAME, r.PHONE_NUMBER, r.EMAIL_ADDRESS, r.BOOKING_DATE, r.GUEST_COUNT, r.TABLE_NUMBER, r.STATUS, r.SPECIAL_REQUESTS, r.SOFT_DELETE, r.BOOKING_TIME, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

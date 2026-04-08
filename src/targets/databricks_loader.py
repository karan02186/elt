from databricks import sql
import logging
import configparser
import os

logger = logging.getLogger("data_accelerator")
script_dir = os.path.dirname(os.path.abspath(__file__))

def read_config():
    config = configparser.ConfigParser()

    # config_path = os.path.join(script_dir, "sapsqlserver_aws_snowflake.cfg")
    config_path = r"D:\elt\config\sqlserver-aws-databricks.cfg"
    files = config.read(config_path)

    if not files:
        raise Exception(f"Config file not found: {config_path}")

    print("Reading config from:", config_path)
    return config

def write_sql_to_file(script_dir,database, schema, table, copy_sql, merge_sql):
    file_name = f"{database}_{schema}_{table}.sql"
    file_path = os.path.join(script_dir, file_name)


    with open(file_path, "w") as f:
        f.write("-- COPY INTO RAW\n")
        f.write(copy_sql)
        f.write("\n\n")
        f.write("-- MERGE INTO TARGET\n")
        f.write(merge_sql)

    print(f"SQL file generated: {file_name}")

def get_connection(cfg):
    try:
        conn = sql.connect(
            server_hostname=cfg["databricks"]["server_hostname"],
            http_path=cfg["databricks"]["http_path"],
            access_token=cfg["databricks"]["access_token"]
        )
        logger.info("Databricks SQL connection established.")
        return conn

    except Exception as e:
        logger.error(f"Error connecting to Databricks: {e}")
        raise

def build_stage_path(bucket_path, database, schema, table):
    return f"{bucket_path}/data/{database}/{schema}/{table}/incremental/"


def generate_copy(database, schema, table, s3_path):
    raw_table = f"{database}_raw.{schema}.{table}"

    copy_sql = f"""
    COPY INTO {raw_table}
    FROM (
        SELECT
            * EXCEPT (hash_val),
            current_timestamp() AS load_ts
        FROM '{s3_path}'
    )
    FILEFORMAT = PARQUET
    FORMAT_OPTIONS ('recursiveFileLookup' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true','force' = 'true');
    """

    return copy_sql

def generate_merge(database, schema, table, primary_keys, columns):
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]

    raw_table = f"{database}_raw.{schema}.{table}"
    tgt_table = f"{database}_tgt.{schema}.{table}"

    update_cols = [c for c in columns if c not in primary_keys]

    update_set = ", ".join([f"t.{c}=r.{c}" for c in update_cols])

    insert_cols = ", ".join(columns + ['IS_DELETED','INSERT_TS','UPDATE_TS','DELETE_TS'])
    insert_vals = ", ".join([f"r.{c}" for c in columns] + 
                            ['0','current_timestamp()','current_timestamp()','NULL'])

    pk_condition = " AND ".join([f"t.{pk}=r.{pk}" for pk in primary_keys])
    partition_by = ", ".join(primary_keys)

    merge_sql = f"""
    MERGE INTO {tgt_table} t
    USING (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {partition_by}
                       ORDER BY load_ts DESC
                   ) rn
            FROM {raw_table}
        ) x
        WHERE rn = 1
    ) r
    ON {pk_condition}

    WHEN MATCHED AND r.soft_delete = 1 THEN
        UPDATE SET
            t.IS_DELETED = 1,
            t.DELETE_TS = current_timestamp(),
            t.UPDATE_TS = current_timestamp()

    WHEN MATCHED AND r.soft_delete = 0 THEN
        UPDATE SET
            {update_set},
            t.IS_DELETED = 0,
            t.DELETE_TS = NULL,
            t.UPDATE_TS = current_timestamp()

    WHEN NOT MATCHED AND r.soft_delete = 0 THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals});
    """

    return merge_sql

def load_to_databricks(cfg, database, schema, table, primary_keys, s3_path):
    conn =  get_connection(cfg)
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]

    cursor = conn.cursor()
    # bucket_path = build_stage_path(s3_path, database, schema, table)
    bucket_path = s3_path
    # COPY
    copy_sql = generate_copy(database, schema, table, bucket_path)
    logger.info(f"[Databricks] Running COPY INTO for {database}.{schema}.{table}")
    cursor.execute(copy_sql)

    # Ensure copy loaded data before merge.
    # COPY INTO returns metrics/result set in Databricks SQL.
    try:
        copy_rows = cursor.fetchall()
    except Exception:
        copy_rows = []
    loaded_rows = 0
    for row in copy_rows:
        row_text = " ".join([str(x) for x in row]).lower()
        if "num_inserted_rows" in row_text or "rows_copied" in row_text:
            for val in row:
                if isinstance(val, int):
                    loaded_rows += val

    if loaded_rows == 0:
        logger.warning(
            f"[Databricks] COPY returned 0 loaded rows for {database}.{schema}.{table}. "
            "Skipping MERGE."
        )
        cursor.close()
        conn.close()
        return

    # Get columns from bronze
    cursor.execute(f"DESCRIBE {database}_raw.{schema}.{table}")
    cols = [row[0] for row in cursor.fetchall()]
    cols = [c for c in cols if c not in ['soft_delete','load_ts', 'hash_val']]

    # MERGE
    merge_sql = generate_merge(database, schema, table, primary_keys, cols)
    logger.info(f"[Databricks] Running MERGE for {database}.{schema}.{table}")
    # write_sql_to_file(script_dir,database, schema, table, copy_sql, merge_sql)
    cursor.execute(merge_sql)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"[Databricks] Load completed for {database}.{schema}.{table}")






# cfg = read_config()

# load_to_databricks(
#     cfg,
#     database="bikestores",
#     schema="production",
#     table="table_booking",
#     primary_keys=["booking_id"],
#     s3_path="s3://bikestores-data-pipeline/"
# )

from databricks import sql
import logging
import configparser
import os

logger = logging.getLogger(__name__)
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
    # cfg = parse_config(config_path, configfile=configfile, target_type="databricks")
    print(f'{cfg} in connection')

    try:
        conn = sql.connect(
            server_hostname=cfg["databricks"]["server_hostname"],
            http_path=cfg["databricks"]["http_path"],
            access_token=cfg["databricks"]["access_token"]
        )

        return conn

    except Exception as e:
        logger.error(f"Error connecting to Databricks: {e}")
        return None

def build_stage_path(bucket_path, database, schema, table):
    return f"{bucket_path}/data/{database.lower()}/{schema.lower()}/{table.lower()}/incremental/"


def generate_copy(database, schema, table, s3_path):

    bronze = f"{database}.bronze.{table}"

    copy_sql = f"""
    COPY INTO {bronze}
    FROM (
        SELECT
            * EXCEPT (row_hash),
            current_timestamp() AS load_ts
        FROM '{s3_path}'
    )
    FILEFORMAT = PARQUET
    FORMAT_OPTIONS ('recursiveFileLookup' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true','force' = 'true');
    """

    return copy_sql

def generate_merge(database, table, primary_keys, columns):
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]

    bronze = f"{database}.bronze.{table}"
    silver = f"{database}.silver.{table}"

    update_cols = [c for c in columns if c not in primary_keys]

    update_set = ", ".join([f"t.{c}=r.{c}" for c in update_cols])

    insert_cols = ", ".join(columns + ['IS_DELETED','INSERT_TS','UPDATE_TS','DELETE_TS'])
    insert_vals = ", ".join([f"r.{c}" for c in columns] + 
                            ['0','current_timestamp()','current_timestamp()','NULL'])

    pk_condition = " AND ".join([f"t.{pk}=r.{pk}" for pk in primary_keys])
    partition_by = ", ".join(primary_keys)

    merge_sql = f"""
    MERGE INTO {silver} t
    USING (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {partition_by}
                       ORDER BY load_ts DESC
                   ) rn
            FROM {bronze}
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
    print(cfg)

    conn =  get_connection(cfg)
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]

    cursor = conn.cursor()
    # bucket_path = build_stage_path(s3_path, database, schema, table)
    bucket_path = s3_path
    # COPY
    copy_sql = generate_copy(database, schema, table, bucket_path)
    print(copy_sql)
    print("Running COPY INTO...")
    cursor.execute(copy_sql)

    # Get columns from bronze
    cursor.execute(f"DESCRIBE {database}.bronze.{table}")
    cols = [row[0] for row in cursor.fetchall()]
    cols = [c for c in cols if c not in ['soft_delete','load_ts']]

    # MERGE
    merge_sql = generate_merge(database, table, primary_keys, cols)
    print(merge_sql)
    print("Running MERGE...")
    # write_sql_to_file(script_dir,database, schema, table, copy_sql, merge_sql)
    cursor.execute(merge_sql)

    conn.commit()
    cursor.close()
    conn.close()

    print("Databricks Load Completed")






# cfg = read_config()

# load_to_databricks(
#     cfg,
#     database="bikestores",
#     schema="production",
#     table="table_booking",
#     primary_keys=["booking_id"],
#     s3_path="s3://bikestores-data-pipeline/"
# )
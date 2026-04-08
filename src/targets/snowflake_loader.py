import snowflake.connector
import pandas as pd
import configparser
from datetime import datetime
import os

script_dir = os.path.dirname(os.path.abspath(__file__))


# ─────────────────────────────────────────────
# CONFIG / CONNECTION
# ─────────────────────────────────────────────

def get_audit_columns(cfg):
    try:
        return (
            cfg['soft_delete'],
            cfg['load_ts'],
            cfg['insert_ts'],
            cfg['update_ts'],
            cfg['delete_ts'],
            cfg['is_deleted']
        )
    except Exception as e:
        raise


def read_config():
    config = configparser.ConfigParser()
    config_path = r"D:\elt\config\sqlserver-aws-snowflake.cfg"
    files = config.read(config_path)
    if not files:
        raise Exception(f"Config file not found: {config_path}")
    print("Reading config from:", config_path)
    return config


def get_connection(cfg):
    conn = snowflake.connector.connect(
        user=cfg["snowflake"]["user"],
        password=cfg["snowflake"]["password"],
        account=cfg["snowflake"]["account"],
        warehouse=cfg["snowflake"]["warehouse"],
        role=cfg["snowflake"]["role"]
    )
    return conn


# ─────────────────────────────────────────────
# METADATA
# ─────────────────────────────────────────────

def read_metadata(file):
    df = pd.read_csv(file)
    tables = []
    for _, row in df.iterrows():
        keys = [k.strip() for k in row["primary_key"].split(",")]
        tables.append({
            "database": row["database"],
            "schema":   row["schema"],
            "table":    row["table_name"],
            "primary_keys": keys
        })
    return tables


# ─────────────────────────────────────────────
# COLUMN RETRIEVAL
# ─────────────────────────────────────────────

# Framework cols that live in RAW but must be excluded from MERGE business logic
METADATA_COLS = {'__LOAD_TS', '__FILE_NAME', '__SOFT_DELETE'}


def get_columns(conn, database, schema, table):
    """
    Returns all column names from the RAW table.
    Used to derive business columns for MERGE (METADATA_COLS stripped by caller).
    """
    raw_db = f"{database}_RAW"

    query = f"""
    SELECT COLUMN_NAME
    FROM {raw_db}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema.upper()}'
      AND TABLE_NAME   = '{table.upper()}'
    ORDER BY ORDINAL_POSITION
    """

    cur = conn.cursor()
    cur.execute(query)
    columns = [row[0] for row in cur.fetchall()]

    print(f"Columns from RAW ({len(columns)}): {columns}")
    return columns


# ─────────────────────────────────────────────
# COPY INTO  — simple, no transformation
# ─────────────────────────────────────────────

def build_stage_path(stage, database, schema, table):
    stage_full = f"{database}_TGT.EXT_STAGE_SCHEMA.{stage}"
    return f"{stage_full}/data/{database}/{schema}/{table}/incremental/"


def generate_copy(database, schema, table, stage):
    """
    Simple COPY INTO using MATCH_BY_COLUMN_NAME.
    Snowflake maps parquet columns to table columns by name — no transformation.
    HASH_VAL or any extra parquet columns not in the RAW table are ignored automatically.
    FORCE = TRUE reloads files even if already loaded (idempotent reruns).
    """
    raw_db     = f"{database}_RAW"
    full_table = f"{raw_db}.{schema}.{table}"
    stage_path = build_stage_path(stage, database, schema, table)

    copy_sql = f"""
COPY INTO {full_table}
FROM @{stage_path}
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[.]parquet'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FORCE = TRUE;
"""
    return copy_sql


# ─────────────────────────────────────────────
# COPY RESULT VALIDATION
# ─────────────────────────────────────────────

class CopyIntoError(Exception):
    """Raised when COPY INTO reports 0 rows loaded or any per-file error."""
    pass


def validate_copy_result(cur):
    """
    Parses Snowflake's COPY INTO result rows and raises CopyIntoError on failure.

    Result row layout:
        0  file name
        1  status         ('LOADED' | 'LOAD_FAILED' | 'PARTIALLY_LOADED')
        2  rows_parsed
        3  rows_loaded
        4  error_limit
        5  errors_seen
        6  first_error
        7  first_error_line
        8  first_error_character
        9  first_error_column_name
    """
    rows = cur.fetchall()

    if not rows:
        raise CopyIntoError(
            "COPY INTO returned no result rows — "
            "stage may be empty or pattern matched nothing."
        )

    total_loaded = 0
    errors       = []

    for row in rows:
        file_name   = row[0]
        status      = row[1]
        rows_loaded = row[3]
        first_error = row[6] if len(row) > 6 else None

        if status == "LOADED":
            total_loaded += rows_loaded

        elif status in ("LOAD_FAILED", "PARTIALLY_LOADED"):
            errors.append(
                f"File '{file_name}' → status={status}, "
                f"rows_loaded={rows_loaded}, error='{first_error}'"
            )

    if errors:
        raise CopyIntoError("COPY INTO failed for one or more files:\n" + "\n".join(errors))

    if total_loaded == 0:
        raise CopyIntoError(
            "COPY INTO completed but 0 rows were loaded. "
            "Check that parquet files exist at the stage path and the pattern matches."
        )

    print(f"✅ COPY INTO — {total_loaded} rows loaded across {len(rows)} file(s).")


# ─────────────────────────────────────────────
# MERGE
# ─────────────────────────────────────────────

def generate_merge(database, schema, table, primary_keys, columns, Audit_Columns):
    """
    columns — ALL RAW columns (including metadata).
              METADATA_COLS and audit cols are stripped internally.
    """
    soft_delete, load_ts, insert_ts, update_ts, delete_ts, is_deleted = \
        get_audit_columns(Audit_Columns)

    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]

    # Normalize PKs to uppercase for safe comparison
    primary_keys_upper = [pk.upper() for pk in primary_keys]

    raw_db = f"{database}_RAW"
    tgt_db = f"{database}_TGT"
    tgt    = f"{tgt_db}.{schema}.{table}"
    raw    = f"{raw_db}.{schema}.{table}"

    audit_col_set = {
        soft_delete, load_ts, insert_ts,
        update_ts, delete_ts, is_deleted
    }

    # Business cols — strip metadata and audit cols, keep only data cols
    source_cols = [
        col for col in columns
        if col not in METADATA_COLS
        and col not in audit_col_set
    ]

    # Update cols — data cols only, no PKs
    update_cols = [
        col for col in source_cols
        if col.upper() not in primary_keys_upper
    ]

    # update_set   = ", ".join([f"t.{col} = r.{col}" for col in update_cols])
    # update_set = ",\n        ".join([f"t.{col} = r.{col}" for col in update_cols])
    update_set = ",\n        ".join([
                                        f"t.{col} = r.{col}" for col in update_cols
                                    ] )

    insert_cols  = ", ".join(source_cols + [insert_ts, update_ts])
    insert_vals  = ", ".join([f"r.{col}" for col in source_cols] +
                             ['CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP'])
    pk_condition = " AND ".join([f"t.{pk} = r.{pk}" for pk in primary_keys])
    partition_by = ", ".join(primary_keys)

    print(f"  PKs:         {primary_keys}")
    print(f"  source_cols: {source_cols}")
    print(f"  update_cols: {update_cols}")
    print(f"  insert_cols: {insert_cols}")

    return f"""
MERGE INTO {tgt} t
USING (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY {partition_by}
                   ORDER BY {load_ts} DESC
               ) rn
        FROM {raw}
    )
    WHERE rn = 1
) r
ON {pk_condition}

WHEN MATCHED AND r.{soft_delete} = 1 THEN
    UPDATE SET
        t.{is_deleted} = 1,
        t.{delete_ts}  = CURRENT_TIMESTAMP,
        t.{update_ts}  = CURRENT_TIMESTAMP

WHEN MATCHED AND r.{soft_delete} = 0 THEN
    UPDATE SET
        {update_set},
        t.{update_ts}  = CURRENT_TIMESTAMP

WHEN NOT MATCHED AND r.{soft_delete} = 0 THEN
    INSERT ({insert_cols})
    VALUES ({insert_vals});
"""

def truncate_raw(database, schema, table):
    return f"TRUNCATE TABLE {database}_RAW.{schema}.{table};"


def create_logger(conn, database):
    tgt_db = f"{database}_TGT"
    sql = f"""
    CREATE TABLE IF NOT EXISTS {tgt_db}.PUBLIC.PIPELINE_LOG (
        table_name  STRING,
        start_time  TIMESTAMP,
        end_time    TIMESTAMP,
        status      STRING,
        message     STRING
    );
    """
    conn.cursor().execute(sql)


def write_sql_to_file(script_dir, database, schema, table, copy_sql, merge_sql):
    file_name = f"{database}_{schema}_{table}.sql"
    file_path = os.path.join(script_dir, file_name)
    with open(file_path, "w") as f:
        f.write("-- COPY INTO RAW\n")
        f.write(copy_sql)
        f.write("\n\n-- MERGE INTO TARGET\n")
        f.write(merge_sql)
    print(f"SQL file generated: {file_name}")


# ─────────────────────────────────────────────
# MAIN LOAD FUNCTION
# ─────────────────────────────────────────────

def load_to_snowflake(cfg, db, schema, tbl, pk):
    print(f"Loading: {db}.{schema}.{tbl} | pk={pk}")
    Audit_cols = {
        "soft_delete": "soft_delete",
        "is_deleted": "soft_delete",
        "load_ts": "__load_ts",
        "insert_ts": "__insert_timestamp",
        "update_ts": "__update_timestamp",
        "delete_ts": "__delete_timestamp"
    }
    if isinstance(pk, str):
        pk = [pk]

    stage = cfg["snowflake"]["stage_name"]
    conn  = get_connection(cfg)
    cur   = conn.cursor()

    try:
        # ── 1. Get all RAW table columns (for MERGE col filtering)
        all_columns = get_columns(conn, db, schema, tbl)

        # ── 2. Generate SQL
        copy_sql  = generate_copy(db, schema, tbl, stage)          # no col_defs needed
        merge_sql = generate_merge(db, schema, tbl, pk, all_columns, Audit_cols)
        trunc_sql = truncate_raw(db, schema, tbl)

        # ── 3. Write SQL to file for audit / replay
        write_sql_to_file(script_dir, db, schema, tbl, copy_sql, merge_sql)

        # ── 4. COPY INTO RAW
        print("▶ Running COPY INTO...")
        cur.execute(copy_sql)
        validate_copy_result(cur)

        # ── 5. MERGE into TGT
        print("▶ Running MERGE...")
        cur.execute(merge_sql)
        print("✅ MERGE completed.")

        # ── 6. Truncate RAW staging table
        print("▶ Truncating RAW...")
        cur.execute(trunc_sql)
        print("✅ RAW truncated.")

        conn.commit()

    except CopyIntoError as e:
        conn.rollback()
        print(f"❌ COPY INTO failed — rolling back.\n{e}")
        raise

    except snowflake.connector.errors.ProgrammingError as e:
        conn.rollback()
        print(f"❌ Snowflake SQL error — rolling back.\n{e}")
        raise

    except Exception as e:
        conn.rollback()
        print(f"❌ Unexpected error — rolling back.\n{e}")
        raise

    finally:
        cur.close()
        conn.close()



# import snowflake.connector
# import pandas as pd
# import configparser
# from datetime import datetime
# import os
#
# script_dir = os.path.dirname(os.path.abspath(__file__))
#
#
# def get_audit_columns(cfg):
#     """
#     Extract audit column names from configuration.
#     """
#
#     try:
#         return (
#             cfg['soft_delete'],
#             cfg['load_ts'],
#             cfg['insert_ts'],
#             cfg['update_ts'],
#             cfg['delete_ts'],
#             cfg['is_deleted']
#         )
#
#     except Exception as e:
#         raise
# # use parse congig file to get connection details and stage name
# def read_config():
#     config = configparser.ConfigParser()
#
#     # config_path = os.path.join(script_dir, "sapsqlserver_aws_snowflake.cfg")
#     config_path = r"D:\elt\config\sqlserver-aws-snowflake.cfg"
#     files = config.read(config_path)
#
#     if not files:
#         raise Exception(f"Config file not found: {config_path}")
#
#     print("Reading config from:", config_path)
#     return config
#
# # use snowflake connector to connect to snowflake using details from config file
# def get_connection(cfg):
#     conn = snowflake.connector.connect(
#             user=cfg["snowflake"]["user"],
#         password=cfg["snowflake"]["password"],
#         account=cfg["snowflake"]["account"],
#         warehouse=cfg["snowflake"]["warehouse"],
#         role=cfg["snowflake"]["role"]
#     )
#     return conn
#
#
# # call this itteration inside the main function and proceed with the rest of the steps like get columns, generate merge sql, write to file etc.
# def read_metadata(file):
#     df = pd.read_csv(file)
#     tables = []
#
#     for _, row in df.iterrows():
#         keys = [k.strip() for k in row["primary_key"].split(",")]
#
#         tables.append({
#             "database": row["database"],
#             "schema": row["schema"],
#             "table": row["table_name"],
#             "primary_keys": keys
#         })
#
#     return tables
#
#
# def build_stage_path(stage, database, schema, table):
#     stage_full = f"{database}_TGT.EXT_STAGE_SCHEMA.{stage}"
#     return f"@{stage_full}/data/{database}/{schema}/{table}/incremental/"
#
#
# def generate_copy(database, schema, table, stage):
#
#     raw_db = f"{database}_RAW"
#     raw_table = f"{raw_db}.{schema}.{table}"
#
#     stage_path = build_stage_path(stage, database, schema, table)
#
#     copy_sql = f"""
#     COPY INTO {raw_table}
# FROM {stage_path}
# FILE_FORMAT = (TYPE = PARQUET)
# PATTERN = '.*[.]parquet'
# MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
# ON_ERROR = 'SKIP_FILE'
# FORCE = FALSE;
#     """
#
#     return copy_sql
#
#
# def get_columns(conn, database, schema, table):
#     raw_db = f"{database}_RAW"
#
#     query = f"""
#     SELECT COLUMN_NAME
#     FROM {raw_db}.INFORMATION_SCHEMA.COLUMNS
#     WHERE TABLE_SCHEMA = '{schema.upper()}'
#     AND TABLE_NAME = '{table.upper()}'
#     ORDER BY ORDINAL_POSITION
#     """
#
#     print("Executing column retrieval query:\n", query)
#
#     cur = conn.cursor()
#     cur.execute(query)
#     cols = [row[0] for row in cur.fetchall()]
#
#     # Remove metadata columns
#     cols = [c for c in cols if c not in ['__LOAD_TS','__FILE_NAME','__SOFT_DELETE']]
#
#     print("Columns Found:", cols)
#     return cols
#
# def generate_merge(database, schema, table, primary_keys, columns, Audit_Columns):
#     soft_delete, load_ts, insert_ts, update_ts, delete_ts, is_deleted = get_audit_columns(Audit_Columns)
#
#     # Ensure primary_keys is always a list
#     if isinstance(primary_keys, str):
#         primary_keys = [primary_keys]
#
#     raw_db = f"{database}_RAW"
#     tgt_db = f"{database}_TGT"
#
#     tgt = f"{tgt_db}.{schema}.{table}"
#     raw = f"{raw_db}.{schema}.{table}"
#
#     # Columns to exclude
#     exclude_cols = ['__SOFT_DELETE', '__LOAD_TS', '__FILE_NAME']
#     target_columns = [col for col in columns if col not in exclude_cols]
#
#     # Update columns (exclude PK)
#     audit_cols = {insert_ts, update_ts, delete_ts, is_deleted}
#     update_cols = [col for col in target_columns if col not in primary_keys and col not in audit_cols]
#     update_set = ", ".join([f"t.{col}=r.{col}" for col in update_cols])
#
#     # Insert columns
#     insert_cols = ", ".join(target_columns + [insert_ts, update_ts])
#     insert_vals = ", ".join([f"r.{col}" for col in target_columns] + ['CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP'])
#
#     # PK condition (supports composite keys)
#     pk_condition = " AND ".join([f"t.{pk}=r.{pk}" for pk in primary_keys])
#
#     # Partition by PK for latest record logic
#     partition_by = ", ".join(primary_keys)
#
#     merge_sql = f"""
#     MERGE INTO {tgt} t
#     USING (
#         SELECT *
#         FROM (
#             SELECT *,
#                    ROW_NUMBER() OVER (
#                        PARTITION BY {partition_by}
#                        ORDER BY {load_ts} DESC
#                    ) rn
#             FROM {raw}
#         )
#         WHERE rn = 1
#     ) r
#     ON {pk_condition}
#
#     WHEN MATCHED AND r.{soft_delete} = 1 THEN
#         UPDATE SET
#             t.{is_deleted} = 1,
#             t.{delete_ts} = CURRENT_TIMESTAMP,
#             t.{update_ts} = CURRENT_TIMESTAMP
#
#     WHEN MATCHED AND r.{soft_delete} = 0 THEN
#         UPDATE SET
#             {update_set},
#             t.{update_ts} = CURRENT_TIMESTAMP,
#             t.{is_deleted} = 0
#
#     WHEN NOT MATCHED AND r.{soft_delete} = 0 THEN
#         INSERT ({insert_cols})
#         VALUES ({insert_vals});
#     """
#
#     return merge_sql
#
# def truncate_raw(database, schema, table):
#     query = f"""
#         TRUNCATE TABLE {database}_RAW.{schema}.{table};
# """
#     return query
#
# def create_logger(conn, database):
#     tgt_db = f"{database}_TGT"
#
#     sql = f"""
#     CREATE TABLE IF NOT EXISTS {tgt_db}.PUBLIC.PIPELINE_LOG (
#         table_name STRING,
#         start_time TIMESTAMP,
#         end_time TIMESTAMP,
#         status STRING,
#         message STRING
#     );
#     """
#     conn.cursor().execute(sql)
#
# def write_sql_to_file(script_dir,database, schema, table, copy_sql, merge_sql):
#     file_name = f"{database}_{schema}_{table}.sql"
#     file_path = os.path.join(script_dir, file_name)
#
#
#     with open(file_path, "w") as f:
#         f.write("-- COPY INTO RAW\n")
#         f.write(copy_sql)
#         f.write("\n\n")
#         f.write("-- MERGE INTO TARGET\n")
#         f.write(merge_sql)
#
#     print(f"SQL file generated: {file_name}")
#
#
# def load_to_snowflake(cfg,db,schema,tbl,pk,Audit_cols ):
#     print(db,schema,tbl,pk,Audit_cols)
#     print(f"pk in load snoflake {pk}")
#
#     # 🔥 Ensure PK is always a list
#     if isinstance(pk, str):
#         pk = [pk]
#     stage = cfg["snowflake"]["stage_name"]
#     conn = get_connection(cfg)
#
#     cur = conn.cursor()
#
#
#     copy_sql = generate_copy(db, schema, tbl, stage)
#
#     # GET COLUMNS
#     columns = get_columns(conn, db, schema, tbl)
#
#     # MERGE SQL
#     merge_sql = generate_merge(db, schema, tbl, pk, columns, Audit_cols)
#
#     truncate_sql = truncate_raw(db,schema,tbl)
#
#     # WRITE TO FILE INSTEAD OF EXECUTE
#     write_sql_to_file(script_dir, db, schema, tbl, copy_sql, merge_sql)
#     status = "SUCCESS"
#     msg = "SQL generated successfully. Please execute the SQL file to run the pipeline."
#
#     cur.execute(copy_sql)
#
#     print("✅ COPY INTO completed successfully")
#
#     cur.execute(merge_sql)
#     print('Merge successfull')
#     cur.execute(truncate_sql)
#     print("Truncate Raw stage table success")
#
#     conn.commit()
#     cur.close()
#     conn.close()
# # cfg = read_config()
#
# # load_to_snowflake(
# #     cfg,
# #     db="BIKESTORES",
# #     schema="PRODUCTION",
# #     tbl="TABLE_BOOKING2",
# #     pk=["BOOKING_ID"],
# #     stage="MY_S3_STAGE"
# # )
# =============================================================================
# File        : control_table.py
# Project     : Data Accelerator – ELT Framework
# Description : Pipeline control table management — creation, run tracking,
#               and incremental watermark management across Snowflake,
#               Databricks, and BigQuery targets.
# Author      : Data Engineering Team
# Created     : 2024-01-01
# Modified    : 2025-01-01
# Version     : 1.2.0
# =============================================================================

import snowflake.connector
from datetime import datetime
from typing import Any as any

from pyspark.sql import SparkSession  # type: ignore

from elt.src.parse_config import parse_config
from elt.src.connections import snow_conn, data_conn, bigquery_conn

import logging

logger = logging.getLogger("data_accelerator")


# =============================================================================
# INTERNAL HELPERS
# =============================================================================

def get_target_type(config_path: str, configfile: str) -> str:
    """
    Detects and returns the target type from the config file by attempting
    to parse known target sections in priority order: Databricks → Snowflake → BigQuery.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        str: Detected target type ('databricks', 'snowflake', or 'bigquery').

    Raises:
        ValueError: If target type cannot be determined from the config.
    """
    # Attempt explicit target detection in priority order
    for target in ("databricks", "snowflake", "bigquery"):
        try:
            parsed = parse_config(config_path, configfile=configfile,
                                  src_db_type=None, cloud_type=None, target_type=target)
            if target in parsed:
                logger.debug(f"Detected target type: '{target}'")
                return target
        except Exception:
            pass

    # Final fallback — parse without target hint and inspect keys
    try:
        parsed = parse_config(config_path, configfile=configfile)
        for target in ("databricks", "snowflake", "bigquery"):
            if target in parsed:
                logger.debug(f"Fallback detected target type: '{target}'")
                return target
    except Exception:
        pass

    raise ValueError(f"Unable to determine target type from config file: '{configfile}'")


def _get_target_db_schema(
    config_path: str,
    configfile: str,
    src_db_type: str,
    target_type: str
):
    """
    Extracts target and staging database/schema names from the config.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.
        src_db_type (str): Source database type.
        target_type (str): Target system ('snowflake', 'databricks', or 'bigquery').

    Returns:
        Tuple[str, str, str, str]: trg_database, trg_schema, stg_database, stg_schema.

    Raises:
        KeyError   : If required config keys are missing.
        ValueError : If target_type is unsupported.
    """
    try:
        parsed_config = parse_config(config_path, configfile=configfile,
                                     src_db_type=src_db_type, cloud_type=None, target_type=target_type)
    except Exception as err:
        logger.exception(f"Failed to parse config for target schema resolution: {err}")
        raise

    try:
        if target_type == 'snowflake':
            database = parsed_config['snowflake']['database']
            trg_database = f'{database}_tgt'
            trg_schema   = "CONTROL_SCHEMA"
            stg_database = parsed_config['snowflake'].get('stg_database', trg_database)
            stg_schema   = parsed_config['snowflake'].get('stg_schema', "CONTROL_SCHEMA")
            return trg_database, trg_schema, stg_database, stg_schema

        elif target_type == 'databricks':
            base_database = parsed_config['databricks']['database']
            trg_database = f"{base_database}_tgt"
            trg_schema   = "CONTROL_SCHEMA"
            if not trg_database:
                raise ValueError("Databricks 'database' (catalog) key must be specified in config.")
            return trg_database, trg_schema, trg_database, trg_schema

        elif target_type == 'bigquery':
            trg_database = parsed_config['bigquery']['project']
            base_dataset = parsed_config['bigquery'].get('dataset', 'default_dataset')
            trg_schema   = f"{base_dataset}_tgt"
            return trg_database, trg_schema, trg_database, trg_schema

        else:
            raise ValueError(f"Unsupported target type: '{target_type}'")

    except KeyError as key_err:
        logger.error(f"Missing config key for target '{target_type}': {key_err}. Check '{configfile}'.")
        raise


def _get_target_connection(target_type: str, config_path: str, configfile: str):
    """
    Returns a database connection object for the specified target type.

    Params:
        target_type (str): Target system ('snowflake', 'databricks', or 'bigquery').
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        Connection object for the specified target.

    Raises:
        ValueError : If target_type is unsupported.
        Exception  : If the connection attempt fails.
    """
    target = target_type.lower()
    if target == "snowflake":
        return snow_conn(config_path, configfile)
    elif target == "databricks":
        return data_conn(config_path, configfile)
    elif target == "bigquery":
        return bigquery_conn(config_path, configfile)
    else:
        raise ValueError(f"Unsupported target type: '{target_type}'")


# =============================================================================
# FUNCTION : control_tbl
# =============================================================================
def control_tbl(
    conn,
    src_db_type: str,
    target_type: str,
    config_path: str,
    configfile: str
) -> None:
    """
    Creates PIPELINE_CONTROL_TABLE in the target system if it does not exist.
    Also ensures the required database/schema objects exist before table creation.

    Params:
        conn        : Active target database connection.
        src_db_type (str): Source database type.
        target_type (str): Target system ('snowflake' or 'databricks').
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        None

    Raises:
        ValueError : If target_type is unsupported.
        Exception  : If DDL execution fails.
    """
    trg_database, trg_schema, stg_database, stg_schema = _get_target_db_schema(
        config_path, configfile, src_db_type, target_type
    )
    cursor = conn.cursor()

    try:
        if target_type == 'databricks':
            cursor.execute(f"CREATE CATALOG IF NOT EXISTS {trg_database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {trg_database}.{trg_schema}")
            logger.info(f"Databricks catalog/schema ensured: '{trg_database}.{trg_schema}'")

            cursor.execute(f"SHOW TABLES IN {trg_database}.{trg_schema} LIKE 'PIPELINE_CONTROL_TABLE'")
            table_exists = len(cursor.fetchall())

        elif target_type == 'snowflake':
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {trg_database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {trg_database}.{trg_schema}")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {stg_database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {stg_database}.{stg_schema}")
            logger.info("Snowflake database/schema ensured for target and staging.")

            cursor.execute(f"""
                SELECT COUNT(*) FROM {trg_database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{trg_schema.upper()}'
                  AND TABLE_NAME = 'PIPELINE_CONTROL_TABLE'
            """)
            table_exists = cursor.fetchone()[0]

        else:
            raise ValueError(f"Unsupported target type: '{target_type}'")

        if table_exists == 0:
            if target_type == 'databricks':
                create_sql = f"""
                    CREATE TABLE {trg_database}.{trg_schema}.PIPELINE_CONTROL_TABLE (
                        RUN_ID BIGINT GENERATED ALWAYS AS IDENTITY,
                        RUN_START TIMESTAMP, RUN_END TIMESTAMP, DURATION_IN_SEC DOUBLE,
                        SOURCE_DATABASE_NAME STRING, SOURCE_SCHEMA_NAME STRING,
                        SOURCE_OBJECT_NAME STRING, TARGET_DATABASE_NAME STRING,
                        TARGET_SCHEMA_NAME STRING, TARGET_TABLE_NAME STRING,
                        TRIGGERED_BY STRING, STATUS STRING, ERROR_MESSAGE STRING,
                        ROWS_INSERTED BIGINT, ROWS_UPDATED BIGINT,
                        MAX_TIMESTAMP TIMESTAMP, CREATED_TIMESTAMP TIMESTAMP, CREATED_BY STRING
                    )
                """
            else:
                create_sql = f"""
                    CREATE TABLE {trg_database}.{trg_schema}.PIPELINE_CONTROL_TABLE (
                        RUN_ID NUMBER NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER,
                        RUN_START TIMESTAMP_NTZ, RUN_END TIMESTAMP_NTZ, DURATION_IN_SEC FLOAT,
                        SOURCE_DATABASE_NAME VARCHAR, SOURCE_SCHEMA_NAME VARCHAR,
                        SOURCE_OBJECT_NAME VARCHAR, TARGET_DATABASE_NAME VARCHAR,
                        TARGET_SCHEMA_NAME VARCHAR, TARGET_TABLE_NAME VARCHAR,
                        TRIGGERED_BY VARCHAR, STATUS VARCHAR, ERROR_MESSAGE VARCHAR,
                        ROWS_INSERTED NUMBER, ROWS_UPDATED NUMBER,
                        MAX_TIMESTAMP TIMESTAMP_NTZ, CREATED_TIMESTAMP TIMESTAMP_NTZ, CREATED_BY VARCHAR
                    )
                """
            cursor.execute(create_sql)
            logger.info("PIPELINE_CONTROL_TABLE created successfully.")
        else:
            logger.info("PIPELINE_CONTROL_TABLE already exists.")

    except Exception as err:
        logger.exception(f"Failed to create PIPELINE_CONTROL_TABLE: {err}")
        raise
    finally:
        cursor.close()


# =============================================================================
# FUNCTION : insrt_into_ctrl_tbl
# =============================================================================
def insrt_into_ctrl_tbl(
    target_conn,
    target_type: str,
    run_start: datetime,
    run_end: datetime,
    src_db_type: str,
    src_database_name: str,
    src_schema_name: str,
    src_tbl_name: str,
    trg_tbl_name: str,
    rows_upserted: list,
    error: str,
    status: str,
    max_timestamp: any,
    config_path: str,
    configfile: str
) -> None:
    """
    Inserts a pipeline run record into PIPELINE_CONTROL_TABLE in the target.

    Params:
        target_conn        : Active target database connection.
        target_type   (str): Target system ('snowflake' or 'databricks').
        run_start  (datetime): Pipeline run start time.
        run_end    (datetime): Pipeline run end time.
        src_db_type   (str): Source database type.
        src_database_name (str): Source database name.
        src_schema_name   (str): Source schema name.
        src_tbl_name      (str): Source table name.
        trg_tbl_name      (str): Target table name.
        rows_upserted  (list): [inserted_count, updated_count].
        error         (str): Error message if any, else None.
        status        (str): Run status ('SUCCESS', 'FAILED', etc.).
        max_timestamp      : Max timestamp from the processed data.
        config_path   (str): Path to the configuration directory.
        configfile    (str): Name of the configuration file.

    Returns:
        None

    Raises:
        KeyError   : If required config keys are missing.
        ValueError : If target_type is unsupported.
        Exception  : If the INSERT execution fails.
    """
    try:
        parsed_config = parse_config(config_path, configfile, src_db_type,
                                     cloud_type=None, target_type=target_type)
        if target_type == 'snowflake':
            trg_database = parsed_config['snowflake']['database']
            trg_schema   = parsed_config['snowflake']['schema']
        elif target_type == 'databricks':
            trg_database = parsed_config['databricks']['database']
            trg_schema   = parsed_config['databricks']['schema']
        else:
            raise ValueError(f"Unsupported target type: '{target_type}'")
    except KeyError as key_err:
        logger.error(f"Missing config key for control table insert: {key_err}")
        raise

    cursor        = target_conn.cursor()
    duration_secs = (run_end - run_start).total_seconds()

    rows_inserted = 0
    rows_updated  = 0
    if rows_upserted and len(rows_upserted[0]) > 0:
        rows_inserted = rows_upserted[0][0]
        if len(rows_upserted[0]) > 1:
            rows_updated = rows_upserted[0][1]

    error_message = str(error).replace("'", "''") if error else ""
    max_ts_val    = f"'{max_timestamp}'" if max_timestamp is not None else "NULL"

    try:
        if target_type == "snowflake":
            insert_sql = f"""
                INSERT INTO {trg_database}.{trg_schema}.PIPELINE_CONTROL_TABLE
                (RUN_START, RUN_END, DURATION_IN_SEC, SOURCE_DATABASE_NAME, SOURCE_SCHEMA_NAME,
                 SOURCE_OBJECT_NAME, TARGET_DATABASE_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME,
                 TRIGGERED_BY, STATUS, ERROR_MESSAGE, ROWS_INSERTED, ROWS_UPDATED,
                 MAX_TIMESTAMP, CREATED_TIMESTAMP, CREATED_BY)
                VALUES ('{run_start}', '{run_end}', {duration_secs}, '{src_database_name}',
                 '{src_schema_name}', '{src_tbl_name}', '{trg_database.upper()}', '{trg_schema.upper()}',
                 '{trg_tbl_name.upper()}', CURRENT_ROLE(), '{status}', '{error_message}',
                 {rows_inserted}, {rows_updated}, {max_ts_val}, CURRENT_TIMESTAMP, CURRENT_USER())
            """
        else:
            insert_sql = f"""
                INSERT INTO {trg_database}.{trg_schema}.PIPELINE_CONTROL_TABLE
                (RUN_START, RUN_END, DURATION_IN_SEC, SOURCE_DATABASE_NAME, SOURCE_SCHEMA_NAME,
                 SOURCE_OBJECT_NAME, TARGET_DATABASE_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME,
                 TRIGGERED_BY, STATUS, ERROR_MESSAGE, ROWS_INSERTED, ROWS_UPDATED,
                 MAX_TIMESTAMP, CREATED_TIMESTAMP, CREATED_BY)
                VALUES ('{run_start}', '{run_end}', {duration_secs}, '{src_database_name}',
                 '{src_schema_name}', '{src_tbl_name}', '{trg_database}', '{trg_schema}',
                 '{trg_tbl_name}', CURRENT_USER(), '{status}', '{error_message}',
                 {rows_inserted}, {rows_updated},
                 {f"TIMESTAMP('{max_timestamp}')" if max_timestamp else 'NULL'},
                 CURRENT_TIMESTAMP, CURRENT_USER())
            """
        cursor.execute(insert_sql)
        logger.info(f"Inserted run record into PIPELINE_CONTROL_TABLE | table='{src_tbl_name}' | status='{status}'")

    except Exception as err:
        logger.exception(f"Failed to insert into PIPELINE_CONTROL_TABLE: {err}")
        raise
    finally:
        cursor.close()


# =============================================================================
# BACKWARD-COMPATIBLE NO-OPS
# =============================================================================

def src_control_tbl(spark: SparkSession, src_db_type: str, target_type: str,
                    config_path: str, configfile: str):
    """No-op. Control tables now live in the target system, not the source."""
    logger.info("src_control_tbl called — skipped. Control tables are maintained in the target.")


def insrt_into_src_ctrl_tbl(spark, src_db_type, run_id, run_start, run_end,
                             duration_in_sec, source_database_name, source_schema_name,
                             source_object_name, target_database_name, target_collection_name,
                             status, error_message, rows_inserted, config_path, configfile):
    """No-op. Source control table is no longer used."""
    logger.info("insrt_into_src_ctrl_tbl called — skipped. Source control table is deprecated.")


# =============================================================================
# FUNCTION : check_table_exists_target
# =============================================================================
def check_table_exists_target(
    table_name: str,
    config_path: str,
    configfile: str,
    target_type: str = None
) -> bool:
    """
    Checks whether a table exists in the target system.
    Handles BigQuery separately as it uses a client API instead of a cursor.

    Params:
        table_name  (str): Name of the table to check.
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.
        target_type (str): Target system type. Auto-detected if not provided.

    Returns:
        bool: True if the table exists, False otherwise.

    Raises:
        ValueError : If target_type is unsupported.
        Exception  : If the existence check fails.
    """
    target_type = target_type or get_target_type(config_path, configfile)
    conn        = _get_target_connection(target_type, config_path, configfile)

    try:
        trg_database, trg_schema, _, _ = _get_target_db_schema(
            config_path, configfile, "sqlserver", target_type
        )

        # BigQuery uses a client API — no cursor required
        if target_type.lower() == "bigquery":
            from google.cloud.exceptions import NotFound
            table_id = f"{trg_database}.{trg_schema}.{table_name}"
            try:
                conn.get_table(table_id)
                logger.info(f"BigQuery table '{table_id}' exists.")
                return True
            except NotFound:
                logger.info(f"BigQuery table '{table_id}' does not exist.")
                return False

        # Snowflake and Databricks use cursor-based queries
        cur = conn.cursor()
        try:
            if target_type.lower() == "snowflake":
                cur.execute(f"""
                    SELECT 1 FROM {trg_database}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{trg_schema.upper()}'
                      AND TABLE_NAME = '{table_name.upper()}'
                """)
                exists = cur.fetchone() is not None

            elif target_type.lower() == "databricks":
                cur.execute(f"SHOW TABLES IN {trg_database}.{trg_schema} LIKE '{table_name}'")
                exists = len(cur.fetchall()) > 0

            else:
                raise ValueError(f"Unsupported target type: '{target_type}'")

            logger.info(f"Table '{table_name}' exists in '{target_type}': {exists}")
            return exists

        except Exception as err:
            logger.exception(f"Failed to check table existence for '{table_name}': {err}")
            raise
        finally:
            cur.close()

    finally:
        # BigQuery client does not have a close() method — skip for BigQuery
        if target_type.lower() != "bigquery":
            conn.close()


# =============================================================================
# FUNCTION : create_pipeline_run_control_table
# =============================================================================
def create_pipeline_run_control_table(
    config_path: str,
    configfile: str,
    target_type: str = None
) -> None:
    """
    Creates the PipelineRunControl table in the target system if it does not exist.
    Handles BigQuery separately using the client API instead of a cursor.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.
        target_type (str): Target system type. Auto-detected if not provided.

    Returns:
        None

    Raises:
        ValueError : If target_type is unsupported.
        Exception  : If DDL execution fails.
    """
    target_type = target_type or get_target_type(config_path, configfile)
    conn        = _get_target_connection(target_type, config_path, configfile)

    try:
        trg_database, trg_schema, _, _ = _get_target_db_schema(
            config_path, configfile, "sqlserver", target_type
        )

        # BigQuery uses client API — dataset creation + DDL via query job
        if target_type.lower() == "bigquery":
            from google.cloud import bigquery as bq
            try:
                # Ensure the dataset exists before creating the table
                dataset_ref = conn.dataset(trg_schema)
                conn.create_dataset(dataset_ref, exists_ok=True)
                logger.info(f"BigQuery dataset '{trg_database}.{trg_schema}' ensured.")

                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS `{trg_database}.{trg_schema}.PipelineRunControl` (
                        RunId INT64,
                        PipelineRunId STRING, PipelineId STRING, PipelineName STRING,
                        SourceType STRING, SourceDatabase STRING, SourceSchema STRING,
                        SourceTable STRING, LoadType STRING, Status STRING,
                        StartTime TIMESTAMP, EndTime TIMESTAMP, DurationSeconds INT64,
                        RowsProcessed INT64, LastRunTimestamp TIMESTAMP,
                        Cloud STRING, CloudLocation STRING, ErrorMessage STRING,
                        ExtractStatus STRING, RawLoadStatus STRING, MergeStatus STRING,
                        TableRunStatus STRING, PipelineRunStatus STRING,
                        RowsExtracted INT64, RowsWrittenToCloud INT64, RowsMerged INT64, RowsDeleted INT64
                    )
                """
                conn.query(create_sql).result()
                logger.info("PipelineRunControl ensured in BigQuery.")
            except Exception as err:
                logger.exception(f"Failed to create PipelineRunControl in BigQuery: {err}")
                raise
            return  # BigQuery path complete — no cursor to close

        # Snowflake and Databricks use cursor-based DDL
        cur = conn.cursor()
        try:
            if target_type.lower() == "snowflake":
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {trg_database}")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {trg_database}.{trg_schema}")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {trg_database}.{trg_schema}.PipelineRunControl (
                        RunId BIGINT AUTOINCREMENT START 1 INCREMENT 1,
                        PipelineRunId VARCHAR(100), PipelineId VARCHAR(100), PipelineName VARCHAR(200),
                        SourceType VARCHAR(50), SourceDatabase VARCHAR(100), SourceSchema VARCHAR(100),
                        SourceTable VARCHAR(100), LoadType VARCHAR(30), Status VARCHAR(30),
                        StartTime TIMESTAMP_NTZ, EndTime TIMESTAMP_NTZ, DurationSeconds INT,
                        RowsProcessed BIGINT, LastRunTimestamp TIMESTAMP_NTZ,
                        Cloud VARCHAR(50), CloudLocation VARCHAR(500), ErrorMessage VARCHAR,
                        ExtractStatus VARCHAR(30), RawLoadStatus VARCHAR(30), MergeStatus VARCHAR(30),
                        TableRunStatus VARCHAR(30), PipelineRunStatus VARCHAR(30),
                        RowsExtracted BIGINT, RowsWrittenToCloud BIGINT, RowsMerged BIGINT, RowsDeleted BIGINT
                    )
                """)

            elif target_type.lower() == "databricks":
                cur.execute(f"CREATE CATALOG IF NOT EXISTS {trg_database}")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {trg_database}.{trg_schema}")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {trg_database}.{trg_schema}.PipelineRunControl (
                        RunId BIGINT GENERATED ALWAYS AS IDENTITY,
                        PipelineRunId STRING, PipelineId STRING, PipelineName STRING,
                        SourceType STRING, SourceDatabase STRING, SourceSchema STRING,
                        SourceTable STRING, LoadType STRING, Status STRING,
                        StartTime TIMESTAMP, EndTime TIMESTAMP, DurationSeconds INT,
                        RowsProcessed BIGINT, LastRunTimestamp TIMESTAMP,
                        Cloud STRING, CloudLocation STRING, ErrorMessage STRING,
                        ExtractStatus STRING, RawLoadStatus STRING, MergeStatus STRING,
                        TableRunStatus STRING, PipelineRunStatus STRING,
                        RowsExtracted BIGINT, RowsWrittenToCloud BIGINT, RowsMerged BIGINT, RowsDeleted BIGINT
                    )
                """)

            else:
                raise ValueError(f"Unsupported target type: '{target_type}'")

            logger.info(f"PipelineRunControl table ensured in '{target_type}' target.")

        except Exception as err:
            logger.exception(f"Failed to create PipelineRunControl table: {err}")
            raise
        finally:
            cur.close()

    finally:
        if target_type.lower() != "bigquery":
            conn.close()


# =============================================================================
# FUNCTION : insert_pipeline_run_start
# =============================================================================
def insert_pipeline_run_start(
    PipelineRunId: str,
    pipeline_id: str,
    source_type: str,
    database: str,
    schema: str,
    table: str,
    load_type: str,
    cloud: str,
    cloud_location: str,
    pipeline_name: str = None,
    config_path: str = None,
    configfile: str = None,
    target_type: str = None
) -> int:
    """
    Inserts a run-start record into PipelineRunControl and returns the RunId.

    Params:
        PipelineRunId  (str): Unique identifier for this pipeline run.
        pipeline_id    (str): Pipeline identifier.
        source_type    (str): Source database type.
        database       (str): Source database name.
        schema         (str): Source schema name.
        table          (str): Source table name.
        load_type      (str): Load strategy ('TIMESTAMP' or 'SNAPSHOT').
        cloud          (str): Cloud provider.
        cloud_location (str): Cloud storage path for this table.
        pipeline_name  (str): Human-readable pipeline name. Defaults to None.
        config_path    (str): Path to the configuration directory.
        configfile     (str): Name of the configuration file.
        target_type    (str): Target system type. Auto-detected if not provided.

    Returns:
        int: The RunId assigned to this run. Returns 0 if retrieval fails.

    Raises:
        Exception: If the INSERT or RunId retrieval fails.
    """
    target_type = target_type or get_target_type(config_path, configfile)
    create_pipeline_run_control_table(config_path, configfile, target_type)

    conn     = _get_target_connection(target_type, config_path, configfile)
    name_val = f"'{pipeline_name}'" if pipeline_name else "NULL"

    try:
        trg_database, trg_schema, _, _ = _get_target_db_schema(
            config_path, configfile, source_type, target_type
        )

        # BigQuery uses client API — manual RunId via MAX(RunId)+1 (no AUTOINCREMENT)
        if target_type.lower() == "bigquery":
            try:
                insert_sql = f"""
                    INSERT INTO `{trg_database}.{trg_schema}.PipelineRunControl`
                    SELECT
                        COALESCE(MAX(RunId), 0) + 1,
                        '{PipelineRunId}', '{pipeline_id}', {name_val},
                        '{source_type}', '{database}', '{schema}', '{table}', '{load_type}',
                        'STARTED', CURRENT_TIMESTAMP(), NULL, NULL, NULL, NULL,
                        '{cloud}', '{cloud_location}', NULL,
                        'STARTED', 'STARTED', 'STARTED',
                        'STARTED', 'RUNNING',
                        0, 0, 0, 0
                    FROM `{trg_database}.{trg_schema}.PipelineRunControl`
                """
                conn.query(insert_sql).result()

                rows = list(conn.query(f"""
                    SELECT RunId FROM `{trg_database}.{trg_schema}.PipelineRunControl`
                    ORDER BY StartTime DESC LIMIT 1
                """).result())

                run_id = int(rows[0][0]) if rows and rows[0][0] is not None else 0
                logger.info(f"[BigQuery] Pipeline run started | table='{table}' | RunId={run_id}")
                return run_id
            except Exception as err:
                logger.exception(f"[BigQuery] Failed to insert pipeline run start for '{table}': {err}")
                raise

        # Snowflake and Databricks use cursor-based INSERT
        cur = conn.cursor()
        try:
            insert_sql = f"""
                INSERT INTO {trg_database}.{trg_schema}.PipelineRunControl (
                    PipelineRunId, PipelineId, PipelineName, SourceType, SourceDatabase,
                    SourceSchema, SourceTable, LoadType, Status, StartTime, Cloud, CloudLocation,
                    ExtractStatus, RawLoadStatus, MergeStatus, TableRunStatus, PipelineRunStatus,
                    RowsExtracted, RowsWrittenToCloud, RowsMerged, RowsDeleted
                ) VALUES (
                    '{PipelineRunId}', '{pipeline_id}', {name_val},
                    '{source_type}', '{database}', '{schema}', '{table}', '{load_type}',
                    'STARTED', CURRENT_TIMESTAMP(), '{cloud}', '{cloud_location}',
                    'STARTED', 'STARTED', 'STARTED', 'STARTED', 'RUNNING',
                    0, 0, 0, 0
                )
            """
            cur.execute(insert_sql)

            # Snowflake: LAST_QUERY_ID() is called but RunId is fetched via ORDER BY as fallback
            if target_type.lower() == "snowflake":
                cur.execute("SELECT LAST_QUERY_ID()")
                cur.fetchone()  # consumed but not used — RunId fetched below

            cur.execute(f"""
                SELECT RunId FROM {trg_database}.{trg_schema}.PipelineRunControl
                ORDER BY StartTime DESC LIMIT 1
            """)
            row    = cur.fetchone()
            run_id = int(row[0]) if row else 0
            logger.info(f"Pipeline run started | table='{table}' | RunId={run_id}")
            return run_id

        except Exception as err:
            logger.exception(f"Failed to insert pipeline run start for '{table}': {err}")
            raise
        finally:
            cur.close()

    finally:
        if target_type.lower() != "bigquery":
            conn.close()


# =============================================================================
# FUNCTION : update_pipeline_run_end
# =============================================================================
def update_pipeline_run_end(
    run_id,
    status: str,
    rows_processed,
    last_run_ts,
    error_message,
    config_path: str,
    configfile: str,
    target_type: str = None,
    extract_status: str = None,
    raw_load_status: str = None,
    merge_status: str = None,
    rows_extracted: int = None,
    rows_written_to_cloud: int = None,
    rows_merged: int = None,
    rows_deleted: int = None
) -> None:

    target_type = target_type or get_target_type(config_path, configfile)
    conn = _get_target_connection(target_type, config_path, configfile)

    try:
        trg_database, trg_schema, _, _ = _get_target_db_schema(
            config_path, configfile, "sqlserver", target_type
        )

        error_sql = f"'{str(error_message).replace(chr(39), chr(39)*2)}'" if error_message else "NULL"
        ts_sql    = f"'{last_run_ts}'" if last_run_ts else "NULL"
        extract_sql = f"'{extract_status}'" if extract_status else "NULL"
        raw_sql = f"'{raw_load_status}'" if raw_load_status else "NULL"
        merge_sql = f"'{merge_status}'" if merge_status else "NULL"

        # ===== BIGQUERY =====
        if target_type.lower() == "bigquery":
            update_sql = f"""
                UPDATE `{trg_database}.{trg_schema}.PipelineRunControl`
                SET Status = '{status}',
                    EndTime = CURRENT_TIMESTAMP(),
                    DurationSeconds = TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), StartTime, SECOND),
                    RowsProcessed = {rows_processed if rows_processed else 0},
                    LastRunTimestamp = {ts_sql},
                    ErrorMessage = {error_sql},
                    ExtractStatus = {extract_sql},
                    RawLoadStatus = {raw_sql},
                    MergeStatus = {merge_sql},
                    TableRunStatus = '{status}',
                    RowsExtracted = {rows_extracted if rows_extracted is not None else rows_processed if rows_processed else 0},
                    RowsWrittenToCloud = {rows_written_to_cloud if rows_written_to_cloud is not None else rows_processed if rows_processed else 0},
                    RowsMerged = {rows_merged if rows_merged is not None else rows_processed if rows_processed else 0},
                    RowsDeleted = {rows_deleted if rows_deleted is not None else 0}
                WHERE RunId = {run_id}
            """
            conn.query(update_sql).result()
            _update_pipeline_aggregate_status_bigquery(conn, trg_database, trg_schema, run_id)
            logger.info(f"[BigQuery] PipelineRunControl updated | RunId={run_id}")
            return

        # ===== SNOWFLAKE / DATABRICKS =====
        cur = conn.cursor()
        if target_type.lower() == "snowflake":
            duration_expr = "DATEDIFF('SECOND', StartTime, CURRENT_TIMESTAMP())"
        else:
            duration_expr = "TIMESTAMPDIFF(SECOND, StartTime, CURRENT_TIMESTAMP())"
        cur.execute(f"""
            UPDATE {trg_database}.{trg_schema}.PipelineRunControl
            SET Status              = '{status}',
                EndTime             = CURRENT_TIMESTAMP(),
                DurationSeconds     = {duration_expr},
                RowsProcessed       = {rows_processed if rows_processed else 0},
                LastRunTimestamp    = {ts_sql},
                ErrorMessage        = {error_sql},
                ExtractStatus       = {extract_sql},
                RawLoadStatus       = {raw_sql},
                MergeStatus         = {merge_sql},
                TableRunStatus      = '{status}',
                RowsExtracted       = {rows_extracted if rows_extracted is not None else rows_processed if rows_processed else 0},
                RowsWrittenToCloud  = {rows_written_to_cloud if rows_written_to_cloud is not None else rows_processed if rows_processed else 0},
                RowsMerged          = {rows_merged if rows_merged is not None else rows_processed if rows_processed else 0},
                RowsDeleted         = {rows_deleted if rows_deleted is not None else 0}
            WHERE RunId = {run_id}
        """)
        _update_pipeline_aggregate_status(cur, trg_database, trg_schema, run_id)
        cur.close()

    finally:
        if target_type.lower() != "bigquery":
            conn.close()


def _update_pipeline_aggregate_status(cur, trg_database: str, trg_schema: str, run_id: int) -> None:
    cur.execute(
        f"SELECT PipelineRunId FROM {trg_database}.{trg_schema}.PipelineRunControl WHERE RunId = {run_id}"
    )
    row = cur.fetchone()
    if not row:
        return
    pipeline_run_id = row[0]
    cur.execute(f"""
        UPDATE {trg_database}.{trg_schema}.PipelineRunControl
        SET PipelineRunStatus = CASE
            WHEN EXISTS (
                SELECT 1 FROM {trg_database}.{trg_schema}.PipelineRunControl
                WHERE PipelineRunId = '{pipeline_run_id}' AND TableRunStatus <> 'SUCCESS'
            ) THEN 'FAILED'
            ELSE 'SUCCESS'
        END
        WHERE PipelineRunId = '{pipeline_run_id}'
    """)


def _update_pipeline_aggregate_status_bigquery(conn, trg_database: str, trg_schema: str, run_id: int) -> None:
    pipeline_sql = f"""
        SELECT PipelineRunId
        FROM `{trg_database}.{trg_schema}.PipelineRunControl`
        WHERE RunId = {run_id}
        LIMIT 1
    """
    rows = list(conn.query(pipeline_sql).result())
    if not rows:
        return
    pipeline_run_id = rows[0][0]
    update_sql = f"""
        UPDATE `{trg_database}.{trg_schema}.PipelineRunControl`
        SET PipelineRunStatus = (
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM `{trg_database}.{trg_schema}.PipelineRunControl`
                    WHERE PipelineRunId = '{pipeline_run_id}' AND TableRunStatus <> 'SUCCESS'
                ) THEN 'FAILED'
                ELSE 'SUCCESS'
            END
        )
        WHERE PipelineRunId = '{pipeline_run_id}'
    """
    conn.query(update_sql).result()
            
# =============================================================================
# FUNCTION : get_last_success_run_timestamp
# =============================================================================
def get_last_success_run_timestamp(
    pipeline_id: str,
    source_type: str,
    database: str,
    schema: str,
    table: str,
    config_path: str,
    configfile: str,
    target_type: str = None
) -> str:
    """
    Retrieves the LastRunTimestamp from the most recent successful run
    for a given pipeline and table. Returns a default backdated timestamp
    on first run when no prior record exists.

    Params:
        pipeline_id (str): Pipeline identifier.
        source_type (str): Source database type.
        database    (str): Source database name.
        schema      (str): Source schema name.
        table       (str): Source table name.
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.
        target_type (str): Target system type. Auto-detected if not provided.

    Returns:
        str: Timestamp string ('YYYY-MM-DD HH:MM:SS.mmm') or
             '1900-01-01 00:00:00.000' if no prior successful run exists.

    Raises:
        Exception: If the query execution fails.
    """
    target_type = target_type or get_target_type(config_path, configfile)
    create_pipeline_run_control_table(config_path, configfile, target_type)

    conn = _get_target_connection(target_type, config_path, configfile)
    cur  = conn.cursor()

    try:
        trg_database, trg_schema, _, _ = _get_target_db_schema(
            config_path, configfile, source_type, target_type
        )
        cur.execute(f"""
            SELECT LastRunTimestamp
            FROM {trg_database}.{trg_schema}.PipelineRunControl
            WHERE PipelineId    = '{pipeline_id}'
              AND SourceType    = '{source_type}'
              AND SourceDatabase = '{database}'
              AND SourceSchema  = '{schema}'
              AND SourceTable   = '{table}'
              AND LoadType      = 'TIMESTAMP'
              AND Status        = 'SUCCESS'
              AND LastRunTimestamp IS NOT NULL
            ORDER BY EndTime DESC
            LIMIT 1
        """)
        row = cur.fetchone()

        if row and row[0]:
            try:
                return row[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            except Exception:
                return str(row[0])

        logger.info(f"No prior successful run found for '{table}'. Using default timestamp.")
        return "1900-01-01 00:00:00.000"

    except Exception as err:
        logger.exception(f"Failed to retrieve last run timestamp for '{table}': {err}")
        raise
    finally:
        cur.close()
        conn.close()


# =============================================================================
# INCREMENTAL CONTROL TABLE HELPERS
# =============================================================================

def ensure_incremental_control_table_target(
    config_path: str,
    configfile: str,
    target_type: str = None
) -> None:
    """
    Creates the IncrementalControl table in the target if it does not exist.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.
        target_type (str): Target system type. Auto-detected if not provided.

    Returns:
        None

    Raises:
        ValueError : If target_type is unsupported.
        Exception  : If DDL execution fails.
    """
    target_type = target_type or get_target_type(config_path, configfile)

    if target_type.lower() == "snowflake":
        conn = snow_conn(config_path, configfile)
        cur  = conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS IncrementalControl (
                    TableName STRING, LastRunTimestamp TIMESTAMP_NTZ,
                    Status STRING, LastRunLSN STRING, FullLoadDone INTEGER
                )
            """)
            conn.commit()
            logger.info("IncrementalControl ensured in Snowflake.")
        except Exception as err:
            logger.exception(f"Failed to create IncrementalControl in Snowflake: {err}")
            raise
        finally:
            cur.close()
            conn.close()

    elif target_type.lower() == "databricks":
        conn = data_conn(config_path, configfile)
        cur  = conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS IncrementalControl (
                    TableName STRING, LastRunTimestamp TIMESTAMP,
                    Status STRING, LastRunLSN STRING, FullLoadDone INT
                )
            """)
            logger.info("IncrementalControl ensured in Databricks.")
        except Exception as err:
            logger.exception(f"Failed to create IncrementalControl in Databricks: {err}")
            raise
        finally:
            cur.close()
            conn.close()

    else:
        raise ValueError(f"Unsupported target type for IncrementalControl creation: '{target_type}'")


def get_last_run_timestamp_target(
    table_name: str,
    config_path: str,
    configfile: str,
    target_type: str = None
) -> str:
    """
    Reads LastRunTimestamp from the target IncrementalControl table.
    Returns a default backdated timestamp if no record is found.

    Params:
        table_name  (str): Table name to look up.
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.
        target_type (str): Target system type. Auto-detected if not provided.

    Returns:
        str: Timestamp string or '1900-01-01 00:00:00.000' as default.

    Raises:
        ValueError : If target_type is unsupported.
        Exception  : If the query fails.
    """
    target_type = target_type or get_target_type(config_path, configfile)
    ensure_incremental_control_table_target(config_path, configfile, target_type)

    if target_type.lower() == "snowflake":
        conn = snow_conn(config_path, configfile)
        cur  = conn.cursor()
        try:
            cur.execute("SELECT LastRunTimestamp FROM IncrementalControl WHERE TableName = %s", (table_name,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            return "1900-01-01 00:00:00.000"
        except Exception as err:
            logger.exception(f"Failed to get last run timestamp for '{table_name}' in Snowflake: {err}")
            raise
        finally:
            cur.close()
            conn.close()

    elif target_type.lower() == "databricks":
        conn = data_conn(config_path, configfile)
        cur  = conn.cursor()
        try:
            cur.execute(f"SELECT LastRunTimestamp FROM IncrementalControl WHERE TableName = '{table_name}'")
            row = cur.fetchone()
            if row and row[0]:
                try:
                    return row[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                except Exception:
                    return str(row[0])
            return "1900-01-01 00:00:00.000"
        except Exception as err:
            logger.exception(f"Failed to get last run timestamp for '{table_name}' in Databricks: {err}")
            raise
        finally:
            cur.close()
            conn.close()

    else:
        raise ValueError(f"Unsupported target type: '{target_type}'")


def update_last_run_timestamp_target(
    table_name: str,
    timestamp_value: str,
    status: str,
    config_path: str,
    configfile: str,
    target_type: str = None,
    full_load_done: int = 1,
    last_run_lsn: str = None
) -> None:
    """
    Upserts a control record in the target IncrementalControl table.

    Params:
        table_name      (str): Table name for the control record.
        timestamp_value (str): Timestamp to store as the last run watermark.
        status          (str): Run status ('SUCCESS', 'FAILED', etc.).
        config_path     (str): Path to the configuration directory.
        configfile      (str): Name of the configuration file.
        target_type     (str): Target system type. Auto-detected if not provided.
        full_load_done  (int): Flag indicating if full load is done. Default: 1.
        last_run_lsn    (str): Last LSN value, if applicable.

    Returns:
        None

    Raises:
        ValueError : If target_type is unsupported.
        Exception  : If the MERGE execution fails.
    """
    target_type = target_type or get_target_type(config_path, configfile)
    ensure_incremental_control_table_target(config_path, configfile, target_type)

    lsn_val = f"'{last_run_lsn}'" if last_run_lsn else "NULL"

    if target_type.lower() == "snowflake":
        conn = snow_conn(config_path, configfile)
        cur  = conn.cursor()
        try:
            cur.execute(f"""
                MERGE INTO IncrementalControl tgt
                USING (SELECT '{table_name}' AS TableName,
                              TO_TIMESTAMP_NTZ('{timestamp_value}') AS LastRunTimestamp,
                              '{status}' AS Status, {lsn_val} AS LastRunLSN,
                              {full_load_done} AS FullLoadDone) src
                ON tgt.TableName = src.TableName
                WHEN MATCHED THEN UPDATE SET
                    tgt.LastRunTimestamp = src.LastRunTimestamp, tgt.Status = src.Status,
                    tgt.LastRunLSN = src.LastRunLSN, tgt.FullLoadDone = src.FullLoadDone
                WHEN NOT MATCHED THEN INSERT (TableName, LastRunTimestamp, Status, LastRunLSN, FullLoadDone)
                VALUES (src.TableName, src.LastRunTimestamp, src.Status, src.LastRunLSN, src.FullLoadDone)
            """)
            conn.commit()
            logger.info(f"IncrementalControl updated in Snowflake | table='{table_name}'")
        except Exception as err:
            logger.exception(f"Failed to update IncrementalControl in Snowflake for '{table_name}': {err}")
            raise
        finally:
            cur.close()
            conn.close()

    elif target_type.lower() == "databricks":
        conn = data_conn(config_path, configfile)
        cur  = conn.cursor()
        try:
            cur.execute(f"""
                MERGE INTO IncrementalControl tgt
                USING (SELECT '{table_name}' AS TableName,
                              TIMESTAMP('{timestamp_value}') AS LastRunTimestamp,
                              '{status}' AS Status, {lsn_val} AS LastRunLSN,
                              {full_load_done} AS FullLoadDone) src
                ON tgt.TableName = src.TableName
                WHEN MATCHED THEN UPDATE SET
                    tgt.LastRunTimestamp = src.LastRunTimestamp, tgt.Status = src.Status,
                    tgt.LastRunLSN = src.LastRunLSN, tgt.FullLoadDone = src.FullLoadDone
                WHEN NOT MATCHED THEN INSERT (TableName, LastRunTimestamp, Status, LastRunLSN, FullLoadDone)
                VALUES (src.TableName, src.LastRunTimestamp, src.Status, src.LastRunLSN, src.FullLoadDone)
            """)
            logger.info(f"IncrementalControl updated in Databricks | table='{table_name}'")
        except Exception as err:
            logger.exception(f"Failed to update IncrementalControl in Databricks for '{table_name}': {err}")
            raise
        finally:
            cur.close()
            conn.close()

    else:
        raise ValueError(f"Unsupported target type: '{target_type}'")


# =============================================================================
# BACKWARD-COMPATIBILITY WRAPPERS
# =============================================================================

def update_last_run_timestamp_sqlserver(table_name: str, timestamp_value: str,
                                         status: str, config_path: str, configfile: str):
    """Backward-compatible wrapper — delegates to update_last_run_timestamp_target."""
    update_last_run_timestamp_target(table_name=table_name, timestamp_value=timestamp_value,
                                     status=status, config_path=config_path, configfile=configfile)


def get_last_run_timestamp_sqlserver(table_name: str, config_path: str, configfile: str) -> str:
    """Backward-compatible wrapper — delegates to get_last_run_timestamp_target."""
    return get_last_run_timestamp_target(table_name=table_name,
                                         config_path=config_path, configfile=configfile)


def check_table_exists_sqlserver(table_name: str, config_path: str, configfile: str,
                                  target_type: str = None) -> bool:
    """Backward-compatible wrapper — delegates to check_table_exists_target."""
    return check_table_exists_target(table_name, config_path, configfile, target_type=target_type)

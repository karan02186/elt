# =============================================================================
# File        : data_extraction.py
# Project     : Data Accelerator – ELT Framework
# Description : Source data extraction utilities — full load, timestamp-based
#               incremental extraction, and snapshot batch hash CDC with
#               soft-delete detection.

# =============================================================================

import os
import shutil
from datetime import datetime
from typing import Tuple, Optional

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    max as spark_max,
    col,
    md5,
    concat_ws,
    collect_list,
    sort_array,
    lit,
    sum as spark_sum,
    current_timestamp,
    when
)

from elt.src.control_table import update_pipeline_run_end
from elt.src.connections import query_execution
from elt.src.custom_logger import log_dataframe
from elt.src.cloud_aws import upload_to_s3, move_src_to_archive_aws
from elt.src.cloud_gcp import upload_to_gcp, move_src_to_archive_gcp
from elt.src.targets.target_loader import load_to_target

logger = logging.getLogger("data_accelerator")


# =============================================================================
# FUNCTION : extraction_full
# =============================================================================
def extraction_full(
    spark: SparkSession,
    src_db_type: str,
    database_name: str,
    schema_name: str,
    obj_type: str,
    src_tbl_name: str,
    insrt_time_col: str,
    updt_time_col: str,
    column_names: str,
    project_path: str,
    config_path: str,
    configfile: str
) -> Tuple[str, str, datetime]:
    """
    Performs a full load extraction of a source table and writes it to a
    local staging directory as Parquet.

    Params:
        spark          (SparkSession): Active SparkSession.
        src_db_type    (str)         : Source database type (e.g., 'sqlserver').
        database_name  (str)         : Source database name.
        schema_name    (str)         : Source schema name.
        obj_type       (str)         : Object type (e.g., 'table', 'view').
        src_tbl_name   (str)         : Source table name.
        insrt_time_col (str)         : Insert timestamp column name.
        updt_time_col  (str)         : Update timestamp column name.
        column_names   (str)         : Comma-separated column list or '*'.
        project_path   (str)         : Root path of the project.
        config_path    (str)         : Path to the configuration directory.
        configfile     (str)         : Name of the configuration file.

    Returns:
        Tuple[str, str, datetime]: Staging directory path, Parquet file name,
                                   and max timestamp from the extracted data.

    Raises:
        Exception: If query execution, staging dir creation, or write fails.
    """
    logger.info(f"[{src_tbl_name}] Starting FULL LOAD extraction.")

    query = f"SELECT {column_names} FROM {schema_name}.{src_tbl_name}"
    logger.info(f"[{src_tbl_name}] Query: {query}")

    try:
        df        = query_execution(spark, src_db_type, query, config_path, configfile)
        row_count = df.count()
        logger.info(f"[{src_tbl_name}] Fetched {row_count} rows.")
        log_dataframe(df)
    except Exception as err:
        logger.exception(f"[{src_tbl_name}] Query execution failed during full load: {err}")
        raise

    # Determine max timestamp — fall back to current UTC time if columns are null
    try:
        max_vals = df.agg(
            spark_max(insrt_time_col).alias("max_insrt"),
            spark_max(updt_time_col).alias("max_updt")
        ).collect()[0]
        max_timestamp = max(max_vals["max_insrt"], max_vals["max_updt"])
        if max_timestamp is None:
            max_timestamp = datetime.utcnow()
            logger.warning(
                f"[{src_tbl_name}] Timestamp columns returned NULL. Defaulting to UTC now."
            )
    except Exception as err:
        logger.warning(
            f"[{src_tbl_name}] Could not compute max timestamp: {err}. Defaulting to UTC now."
        )
        max_timestamp = datetime.utcnow()

    run_ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    staging_dir = os.path.join(
        project_path, "extract", src_db_type, database_name,
        schema_name, obj_type, src_tbl_name, "incremental", run_ts
    )

    try:
        os.makedirs(staging_dir, exist_ok=True)
        file_name = f"{src_tbl_name}.parquet"
        df.write.mode("overwrite").parquet(staging_dir)
        logger.info(f"[{src_tbl_name}] Full load saved → '{staging_dir}/{file_name}'")
    except Exception as err:
        logger.exception(f"[{src_tbl_name}] Failed to write full load to staging: {err}")
        raise

    return staging_dir, file_name, max_timestamp


# =============================================================================
# FUNCTION : extraction_incremental
# =============================================================================
def extraction_incremental(
    spark: SparkSession,
    src_db_type: str,
    database_name: str,
    schema_name: str,
    obj_type: str,
    src_tbl_name: str,
    insrt_time_col: str,
    updt_time_col: str,
    column_names: str,
    last_run_timestamp: str,
    project_path: str,
    config_path: str,
    configfile: str
) -> Tuple[Optional[str], Optional[str], Optional[datetime]]:
    """
    Performs a timestamp-based incremental extraction for a source table.
    Returns None values if no new rows are found since the last run.

    Params:
        spark               (SparkSession): Active SparkSession.
        src_db_type         (str)         : Source database type.
        database_name       (str)         : Source database name.
        schema_name         (str)         : Source schema name.
        obj_type            (str)         : Object type (e.g., 'table').
        src_tbl_name        (str)         : Source table name.
        insrt_time_col      (str)         : Insert timestamp column name.
        updt_time_col       (str)         : Update timestamp column name.
        column_names        (str)         : Comma-separated column list or '*'.
        last_run_timestamp  (str)         : Watermark from last successful run.
        project_path        (str)         : Root path of the project.
        config_path         (str)         : Path to the configuration directory.
        configfile          (str)         : Name of the configuration file.

    Returns:
        Tuple[Optional[str], Optional[str], Optional[datetime]]:
            Staging directory, Parquet file name, and max timestamp.
            All three are None if no incremental rows are found.

    Raises:
        Exception: If query execution or local staging write fails.
    """
    logger.info(
        f"[{src_tbl_name}] Starting INCREMENTAL extraction | since='{last_run_timestamp}'"
    )

    query = f"""
        SELECT {column_names}
        FROM {schema_name}.{src_tbl_name}
        WHERE {insrt_time_col} > '{last_run_timestamp}'
           OR {updt_time_col}  > '{last_run_timestamp}'
    """
    logger.info(f"[{src_tbl_name}] Incremental query: {query}")

    try:
        df        = query_execution(spark, src_db_type, query, config_path, configfile)
        row_count = df.count()
        logger.info(f"[{src_tbl_name}] Incremental rows fetched = {row_count}")
    except Exception as err:
        logger.exception(
            f"[{src_tbl_name}] Query execution failed during incremental extraction: {err}"
        )
        raise

    if row_count == 0:
        logger.info(f"[{src_tbl_name}] No new rows found. Skipping staging write.")
        return None, None, None

    log_dataframe(df)

    try:
        max_vals = df.agg(
            spark_max(insrt_time_col).alias("max_insrt"),
            spark_max(updt_time_col).alias("max_updt")
        ).collect()[0]
        max_timestamp = max(max_vals["max_insrt"], max_vals["max_updt"])
    except Exception as err:
        logger.warning(
            f"[{src_tbl_name}] Could not compute max timestamp: {err}. Defaulting to None."
        )
        max_timestamp = None

    staging_dir = os.path.join(
        project_path, "extract", src_db_type, database_name,
        schema_name, obj_type, src_tbl_name, "incremental"
    )

    try:
        # Clear previous staging output before writing fresh incremental data
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
        os.makedirs(staging_dir, exist_ok=True)
        file_name = f"{src_tbl_name}_incr.parquet"
        df.write.mode("overwrite").parquet(staging_dir)
        logger.info(f"[{src_tbl_name}] Incremental data saved → '{staging_dir}/{file_name}'")
    except Exception as err:
        logger.exception(
            f"[{src_tbl_name}] Failed to write incremental data to staging: {err}"
        )
        raise

    return staging_dir, file_name, max_timestamp


# =============================================================================
# HELPER : add_row_hash
# =============================================================================
def add_row_hash(df, pk):
    """
    Adds a 'hash_val' column to the DataFrame by computing an MD5 hash of
    all non-PK columns. Used for batch hash comparison in snapshot CDC.

    Params:
        df  (DataFrame): Input Spark DataFrame.
        pk  (str)      : Primary key column name to exclude from hashing.

    Returns:
        DataFrame: DataFrame with an additional 'hash_val' column.
    """
    pk_lower = [c.lower() for c in pk]

    non_pk_cols = [
        c for c in df.columns
        if c.lower() not in pk_lower
    ]

    return df.withColumn(
        "hash_val",
        md5(concat_ws("||", *non_pk_cols))
    )


# =============================================================================
# HELPER : compute_batch_hash
# =============================================================================
def compute_batch_hash(df) -> int:
    """
    Computes a single integer hash for an entire batch by summing hashed
    hash_val values. Used to detect whether a batch has changed since
    the last snapshot.

    Params:
        df (DataFrame): DataFrame containing a 'hash_val' column.

    Returns:
        int: Aggregated batch hash value. Returns 0 if DataFrame is empty.
    """
    result = (
        df.selectExpr("hash(hash_val) as h")
          .agg(spark_sum("h").alias("batch_hash"))
          .collect()[0]["batch_hash"]
    )
    return result or 0


# =============================================================================
# HELPER : extract_hash_batches
# =============================================================================
def extract_hash_batches(
    spark,
    schema: str,
    table: str,
    pk,
    pk_list,
    buckets: int,
    cfg: dict
) -> list:
    """
    Extracts source data in hash-based buckets using CHECKSUM modulo
    partitioning. Each bucket is returned as a separate DataFrame.

    Params:
        spark   (SparkSession): Active SparkSession.
        schema  (str)         : Source schema name.
        table   (str)         : Source table name.
        pk      (str)         : Primary key column for CHECKSUM bucketing.
        buckets (int)         : Total number of hash buckets.
        cfg     (dict)        : Parsed pipeline configuration dictionary.

    Returns:
        list[Tuple[int, DataFrame]]: List of (bucket_id, DataFrame) tuples.

    Raises:
        Exception: If any bucket query fails.
    """
    batches = []
    for bucket in range(buckets):
        logger.info(
            f"[BATCH EXTRACT] Extracting bucket {bucket}/{buckets} for '{schema}.{table}'"
        )
        query = f"""
            SELECT *
            FROM {schema}.{table}
            WHERE ABS(CHECKSUM({pk_list})) % {buckets} = {bucket}
        """
        try:
            df = query_execution(
                spark, "sqlserver", query, cfg["config_path"], cfg["configfile"]
            )
            batches.append((bucket, df))
        except Exception as err:
            logger.exception(
                f"[BATCH EXTRACT] Failed to extract bucket {bucket} for '{table}': {err}"
            )
            raise

    return batches


# =============================================================================
# FUNCTION : incremental_load_snapshot_compare
# =============================================================================
def incremental_load_snapshot_compare(
    spark,
database,
                    schema,
                    table,
                    pk,
    cfg: dict,
    run_id,
    cloud_type: str,
    target_type: str,
    buckets: int = 1
) -> int:
    """
    Performs a snapshot batch hash CDC load for a single table. For each
    hash bucket, compares today's data against the previous snapshot to detect
    inserts, updates, and soft-deletes. Writes deltas to cloud storage, updates
    the snapshot, loads to the target, and archives processed files.

    Params:
        spark       (SparkSession): Active SparkSession.
        meta        (dict)        : Metadata row for the table being processed.
        cfg         (dict)        : Parsed pipeline configuration dictionary.
        run_id      (int)         : Control table run identifier.
        cloud_type  (str)         : Cloud provider — 'aws' or 'gcp'.
        target_type (str)         : Target system (e.g., 'snowflake', 'databricks').
        buckets     (int)         : Number of hash buckets for partitioning. Default: 1.

    Returns:
        int: Total number of delta records written across all batches.

    Raises:
        KeyError   : If required metadata or config keys are missing.
        ValueError : If an unsupported cloud_type is provided.
        Exception  : If target load or snapshot write fails.
    """
    # -------------------------------------------------------------------------
    # Extract metadata fields
    # -------------------------------------------------------------------------
    try:
        # pk       = meta["PRIMARY_KEY_COL"].strip()
        pk_list = ", ".join(pk)
    except KeyError as key_err:
        logger.error(f"Missing metadata field: {key_err}. Check the metadata CSV.")
        raise

    logger.info(
        f"[{table}] Starting Snapshot Batch CDC | cloud='{cloud_type}' | buckets={buckets}"
    )

    # Auto-scale buckets for larger tables when caller passes default bucket=1
    try:
        if buckets <= 1:
            size_query = f"SELECT COUNT(1) AS row_cnt FROM {schema}.{table}"
            row_cnt_df = query_execution(
                spark, "sqlserver", size_query, cfg["config_path"], cfg["configfile"]
            )
            row_cnt = int(row_cnt_df.collect()[0]["row_cnt"])
            rows_per_bucket = int(cfg.get("snapshot_rows_per_bucket", 1_000_000))
            buckets = max(1, min(64, (row_cnt + rows_per_bucket - 1) // rows_per_bucket))
            logger.info(
                f"[{table}] Auto bucket sizing | row_cnt={row_cnt} | "
                f"rows_per_bucket={rows_per_bucket} | buckets={buckets}"
            )
    except Exception as bucket_err:
        logger.warning(f"[{table}] Auto bucket sizing skipped: {bucket_err}. Using buckets={buckets}")

    # -------------------------------------------------------------------------
    # Resolve base cloud storage path
    # -------------------------------------------------------------------------
    try:
        if cloud_type == "aws":
            bucket_name = cfg["aws"]["s3_bucket_name"]
            base_path   = f"s3a://{bucket_name}"
        elif cloud_type == "gcp":
            bucket_name = cfg["gcp"]["gcp_bucket_name"]
            base_path   = f"gs://{bucket_name}"
        elif cloud_type == "azure":
            container = cfg["azure"]["azure_container_name"]
            account = cfg["azure"]["azure_storage_account_name"]
            base_path = f"abfss://{container}@{account}.dfs.core.windows.net"
        else:
            raise ValueError(
                f"Unsupported cloud_type: '{cloud_type}'. Expected 'aws' or 'gcp'."
            )
    except KeyError as key_err:
        logger.error(f"[{table}] Missing cloud config key: {key_err}.")
        raise

    # Generate run timestamp ONCE per function call — used consistently across all batches
    # BUG FIX: Original script set run_timestamp at module level (outside function),
    # meaning it was frozen at import time and never updated between pipeline runs.
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # -------------------------------------------------------------------------
    # Extract all hash buckets from source
    # -------------------------------------------------------------------------
    try:
        batches = extract_hash_batches(spark, schema, table, pk, pk_list, buckets, cfg)
    except Exception as err:
        logger.exception(f"[{table}] Failed to extract hash batches: {err}")
        raise

    total_delta = 0

    # =========================================================================
    # PROCESS EACH BATCH BUCKET
    # =========================================================================
    for batch_id, today_df in batches:

        today_count = today_df.count()
        if today_count == 0:
            logger.info(f"[{table}] Batch {batch_id} is empty. Skipping.")
            continue

        logger.info(
            f"[{table}] ===== Processing Batch {batch_id} | rows={today_count} ====="
        )

        today_df            = add_row_hash(today_df, pk)
        snapshot_batch_path = (
            f"{base_path}/data/{database}/{schema}/{table}/snapshot/batch_{batch_id}"
        )

        # Read previous snapshot for this batch — expected to fail on first run
        try:
            prev_snapshot_df = spark.read.parquet(snapshot_batch_path)
            prev_active_df   = prev_snapshot_df.filter("soft_delete = 0")
            prev_deleted_df  = prev_snapshot_df.filter("soft_delete = 1")
        except Exception:
            logger.info(
                f"[{table}] No previous snapshot for batch {batch_id}. First run."
            )
            prev_active_df  = None
            prev_deleted_df = None

        # -----------------------------------------------------------------------
        # BATCH HASH SKIP LOGIC — skip unchanged batches to save compute
        # -----------------------------------------------------------------------
        today_batch_hash = compute_batch_hash(today_df)
        write_snapshot   = False

        if prev_active_df is None:
            write_snapshot = True
        else:
            prev_batch_hash = compute_batch_hash(prev_active_df)
            if prev_batch_hash != today_batch_hash:
                write_snapshot = True
            else:
                logger.info(f"[{table}] Batch {batch_id} unchanged — skipping.")

        if not write_snapshot:
            continue

        # -----------------------------------------------------------------------
        # CDC LOGIC — detect inserts, updates, and soft-deletes
        # -----------------------------------------------------------------------
        if prev_active_df is None:
            # First run — treat all rows as inserts
            delta_df          = today_df.withColumn("soft_delete", lit(0))
            final_snapshot_df = delta_df

        else:
            today_hash_df = today_df.select(*pk, "hash_val")
            prev_hash_df  = prev_active_df.select(*pk, "hash_val")

            # New PKs not in previous snapshot = inserts
            inserted_df = today_hash_df.select(*pk).subtract(prev_hash_df.select(*pk))

            # Same PKs but different hash = updates (excluding brand-new inserts)
            updated_df = (
                today_hash_df.subtract(prev_hash_df)
                .select(*pk)
                .subtract(inserted_df)
            )

            # PKs in previous snapshot missing from today = soft-deletes
            deleted_df = prev_hash_df.select(*pk).subtract(today_hash_df.select(*pk))

            insert_count = inserted_df.count()
            update_count = updated_df.count()
            delete_count = deleted_df.count()

            logger.info(
                f"[{table}] Batch {batch_id} CDC | "
                f"Inserts={insert_count} | Updates={update_count} | Deletes={delete_count}"
            )

            # Combine insert and update PKs to fetch latest row data
            delta_pks_df = inserted_df.union(updated_df)

            upsert_df = (
                today_df
                .join(delta_pks_df, pk, "inner")
                .withColumn("soft_delete", lit(0))
            )

            soft_delete_rows_df = (
                prev_active_df
                .join(deleted_df, pk, "inner")
                .withColumn("soft_delete", lit(1))
            )

            # Delta sent to target = upserts + soft deletes
            delta_df = upsert_df.unionByName(soft_delete_rows_df)

            # Preserve previous versions of updated rows in snapshot (for history)
            historical_updates = (
                prev_active_df
                .join(updated_df, pk, "inner")
                .withColumn("soft_delete", lit(1))
            )

            # Final snapshot = current rows + historical updates + soft-deletes + old deletes
            base_snapshot = (
                today_df.withColumn("soft_delete", lit(0))
                .unionByName(historical_updates)
                .unionByName(soft_delete_rows_df)
            )
            final_snapshot_df = (
                base_snapshot.unionByName(prev_deleted_df)
                if prev_deleted_df is not None
                else base_snapshot
            )

        # -----------------------------------------------------------------------
        # WRITE INCREMENTAL DELTA TO CLOUD
        # -----------------------------------------------------------------------
        delta_df = (
            delta_df
            .withColumn("load_ts", current_timestamp())
            .withColumn("insert_ts", when(delta_df["soft_delete"] == 0, current_timestamp()).otherwise(lit(None).cast("timestamp")))
            .withColumn("update_ts", when(delta_df["soft_delete"] == 0, current_timestamp()).otherwise(lit(None).cast("timestamp")))
            .withColumn("delete_ts", when(delta_df["soft_delete"] == 1, current_timestamp()).otherwise(lit(None).cast("timestamp")))
        )
        delta_count = delta_df.count()

        if delta_count > 0:
            incr_path = (
                f"{base_path}/data/{database}/{schema}/{table}"
                f"/incremental/{run_timestamp}/batch_{batch_id}"
            )
            try:
                delta_df.write.mode("overwrite").parquet(incr_path)
                logger.info(
                    f"[{table}] Delta written | batch={batch_id} | path='{incr_path}'"
                )
            except Exception as err:
                logger.exception(
                    f"[{table}] Failed to write delta for batch {batch_id}: {err}"
                )
                raise

        # -----------------------------------------------------------------------
        # MATERIALIZE SNAPSHOT BEFORE ARCHIVE (prevents lazy evaluation issues)
        # BUG FIX: Original script skipped this cache step in some code paths,
        # risking re-evaluation after the source snapshot was already archived/deleted.
        # -----------------------------------------------------------------------
        final_snapshot_df = final_snapshot_df.cache()
        final_snapshot_df.count()  # Force full materialisation

        # Archive old snapshot before overwriting
        source_folder  = f"data/{database}/{schema}/{table}/snapshot/batch_{batch_id}"
        archive_folder = (
            f"archive/{database}/{schema}/{table}/snapshot/{run_timestamp}/batch_{batch_id}"
        )

        try:
            if cloud_type == "aws":
                move_src_to_archive_aws(
                    folder_name=archive_folder,
                    s3_folder_path=source_folder,
                    config_path=cfg["config_path"],
                    configfile=cfg["configfile"]
                )
            elif cloud_type == "gcp":
                move_src_to_archive_gcp(
                    folder_name=archive_folder,
                    gcs_folder_path=source_folder,
                    config_path=cfg["config_path"],
                    configfile=cfg["configfile"]
                )
        except Exception as archive_err:
            # Non-fatal — no existing snapshot to archive on first run
            logger.warning(
                f"[{table}] Snapshot archive skipped for batch {batch_id} "
                f"(may not exist yet): {archive_err}"
            )

        # Write new snapshot
        try:
            final_snapshot_df.write.mode("overwrite").parquet(snapshot_batch_path)
            logger.info(
                f"[{table}] Snapshot updated | batch={batch_id} | path='{snapshot_batch_path}'"
            )
        except Exception as err:
            logger.exception(
                f"[{table}] Failed to write snapshot for batch {batch_id}: {err}"
            )
            raise
        finally:
            final_snapshot_df.unpersist()

        # -----------------------------------------------------------------------
        # LOAD DELTA TO TARGET AND ARCHIVE INCREMENTAL FILES
        # BUG FIX: Original script called total_delta += delta_count TWICE per
        # batch (once before target load and once after), doubling the count.
        # -----------------------------------------------------------------------
        if delta_count > 0:
            # Convert s3a:// → s3:// for target loader compatibility (AWS only)
            if cloud_type == "aws":
                cloud_path = (
                    f"s3://{bucket_name}/data/{database}/{schema}/{table}"
                    f"/incremental/{run_timestamp}/batch_{batch_id}"
                )
            else:
                cloud_path = (
                    f"gs://{bucket_name}/data/{database}/{schema}/{table}"
                    f"/incremental/{run_timestamp}/batch_{batch_id}"
                )

            logger.info(
                f"[{table}] Loading delta to target | "
                f"path='{cloud_path}' | target='{target_type}'"
            )

            try:
                load_to_target(
                    target_type=target_type,
                    cfg=cfg,
                    database=database,
                    schema=schema,
                    table=table,
                    primary_keys=pk,
                    cloud_path=cloud_path,
                )
                logger.info(f"[{table}] Target load successful for batch {batch_id}.")
            except Exception as err:
                logger.exception(
                    f"[{table}] Target load failed for batch {batch_id}: {err}"
                )
                raise

            # Archive incremental files only after a successful target load
            incr_source_folder  = (
                f"data/{database}/{schema}/{table}/incremental/{run_timestamp}/"
            )
            incr_archive_folder = (
                f"archive/{database}/{schema}/{table}/incremental/{run_timestamp}/"
            )

            try:
                if cloud_type == "aws":
                    move_src_to_archive_aws(
                        folder_name=incr_archive_folder,
                        s3_folder_path=incr_source_folder,
                        config_path=cfg["config_path"],
                        configfile=cfg["configfile"]
                    )
                elif cloud_type == "gcp":
                    move_src_to_archive_gcp(
                        folder_name=incr_archive_folder,
                        gcs_folder_path=incr_source_folder,
                        config_path=cfg["config_path"],
                        configfile=cfg["configfile"]
                    )
                logger.info(
                    f"[{table}] Incremental files archived for batch {batch_id}."
                )
            except Exception as archive_err:
                logger.warning(
                    f"[{table}] Incremental archive failed for batch {batch_id}: {archive_err}"
                )

            # Accumulate delta count ONCE per batch after successful load
            total_delta += delta_count

    # =========================================================================
    # UPDATE CONTROL TABLE ONCE AFTER ALL BATCHES COMPLETE
    # BUG FIX: Original script called update_pipeline_run_end inside the batch
    # loop, updating the control table after every bucket instead of once at end.
    # It also called `return total_delta` inside the loop, causing early exit
    # after the first batch — remaining buckets were never processed.
    # =========================================================================
    try:
        update_pipeline_run_end(
            run_id=run_id,
            status="SUCCESS",
            rows_processed=total_delta,
            last_run_ts=None,
            error_message=None,
            config_path=cfg["config_path"],
            configfile=cfg["configfile"],
            target_type=target_type,
            extract_status="SUCCESS",
            raw_load_status="SUCCESS",
            merge_status="SUCCESS",
            rows_extracted=total_delta,
            rows_written_to_cloud=total_delta,
            rows_merged=total_delta,
            rows_deleted=0
        )
        logger.info(f"[{table}] Control table updated | total_delta={total_delta}")
    except Exception as err:
        logger.exception(f"[{table}] Failed to update control table: {err}")
        raise

    logger.info(f"[{table}] Snapshot Batch CDC complete | total_delta={total_delta}")
    return total_delta

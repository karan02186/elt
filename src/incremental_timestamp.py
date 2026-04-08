# =============================================================================
# File        : incremental_timestamp.py
# Project     : Data Accelerator – ELT Framework
# Description : Handles timestamp-based incremental loads in hybrid mode —
#               detects inserts, updates, and soft-deletes by comparing
#               source data against a stored snapshot in cloud storage.
# =============================================================================

import logging
from datetime import datetime

from pyspark.sql.functions import lit, max as spark_max

from elt.src.connections import query_execution
from elt.src.control_table import update_pipeline_run_end, get_last_success_run_timestamp
from elt.src.targets.target_loader import load_to_target
from elt.src.cloud_aws import move_src_to_archive_aws
from elt.src.cloud_gcp import move_src_to_archive_gcp
from elt.src.cloud_azure import move_src_to_archive_azure

logger = logging.getLogger("data_accelerator")


def incremental_load_timestamp(
    spark,
database,
                    schema,
                    table,
                    incr_cols,
                    pk,
    cfg: dict,
    run_id: str,
    cloud_type: str,
    target_type: str,
    config_path: str,
    configfile: str,
    pipeline_id: str,
    src_db_type: str
) -> int:
    """
    Executes a timestamp-based incremental load in hybrid mode for a single table.

    Detects new inserts and updates via timestamp columns, and identifies
    soft-deletes by comparing current source PKs against the previous snapshot.
    Writes the incremental delta to cloud storage, updates the snapshot, loads
    to the target, and updates the pipeline control table.

    Steps:
        1. Fetch incremental rows (inserts + updates) from source.
        2. Fetch current PKs from source for delete detection.
        3. Read previous snapshot from cloud storage.
        4. Detect soft-deletes by subtracting current PKs from snapshot PKs.
        5. Merge incremental rows and soft-delete rows into final delta.
        6. Write final delta to cloud storage (incremental path).
        7. Archive old snapshot and write updated snapshot to cloud storage.
        8. Load final delta to target system.
        9. Update pipeline run control table with result.

    Params:
        spark       (SparkSession): Active SparkSession.
        meta        (dict)        : Metadata row for the table being processed.
        cfg         (dict)        : Parsed pipeline configuration dictionary.
        run_id      (str)         : Control table run identifier.
        cloud_type  (str)         : Cloud provider — 'aws' or 'gcp'.
        target_type (str)         : Target system (e.g., 'snowflake', 'databricks').
        config_path (str)         : Path to the configuration directory.
        configfile  (str)         : Name of the configuration file.
        pipeline_id (str)         : Pipeline identifier for control table lookup.
        src_db_type (str)         : Source database type (e.g., 'sqlserver').

    Returns:
        int: Total number of records written (inserts + updates + soft-deletes).
             Returns 0 if no changes are detected.

    Raises:
        KeyError   : If required metadata or config keys are missing.
        ValueError : If an unsupported cloud_type is provided.
        Exception  : For any unexpected failure during the load process.
    """

    # -------------------------------------------------------------------------
    # Extract table metadata fields
    # -------------------------------------------------------------------------
    try:

        pk_list = ", ".join(pk)
    except KeyError as key_err:
        logger.error(f"Missing metadata field: {key_err}. Check the metadata CSV.")
        raise

    logger.info(f"[{table}] Starting timestamp incremental load (Hybrid Mode)")

    # Parse insert and update timestamp columns from the comma-separated config value.
    # If only one column is provided, it is used for both insert and update detection.
    cols      = incr_cols.replace('"', "").split(",")
    insrt_col = cols[0].strip()
    updt_col  = cols[1].strip() if len(cols) > 1 else insrt_col

    logger.info(f"[{table}] Incremental columns | insert='{insrt_col}' | update='{updt_col}'")

    # -------------------------------------------------------------------------
    # Resolve base cloud storage path
    # -------------------------------------------------------------------------
    try:
        if cloud_type == "aws":
            bucket    = cfg["aws"]["s3_bucket_name"]
            base_path = f"s3a://{bucket}"
        elif cloud_type == "gcp":
            bucket    = cfg["gcp"]["gcp_bucket_name"]
            base_path = f"gs://{bucket}"
        elif cloud_type == "azure":
            container = cfg["azure"]["azure_container_name"]
            account = cfg["azure"]["azure_storage_account_name"]
            base_path = f"abfss://{container}@{account}.dfs.core.windows.net"
        else:
            raise ValueError(
                f"Unsupported cloud_type: '{cloud_type}'. Expected 'aws', 'gcp' or 'azure'."
            )
    except KeyError as key_err:
        logger.error(
            f"Missing cloud config key: {key_err}. "
            f"Check the [{cloud_type}] section in '{configfile}'."
        )
        raise

    snapshot_path = f"{base_path}/data/{database}/{schema}/{table}/snapshot"

    # =========================================================================
    # STEP 1: Fetch incremental rows (inserts + updates) from source
    # =========================================================================
    try:
        last_ts = get_last_success_run_timestamp(
            pipeline_id, src_db_type, database, schema, table,
            config_path, configfile, target_type
        )
        logger.info(f"[{table}] Last successful run timestamp = '{last_ts}'")
    except Exception as err:
        logger.exception(f"[{table}] Failed to retrieve last run timestamp: {err}")
        raise

    incr_query = f"""
        SELECT *
        FROM {schema}.{table}
        WHERE {insrt_col} > '{last_ts}'
           OR {updt_col}  > '{last_ts}'
    """

    try:
        incr_df    = query_execution(spark, src_db_type, incr_query, config_path, configfile)
        incr_df    = incr_df.withColumn("soft_delete", lit(0))
        incr_count = incr_df.count()
        logger.info(f"[{table}] Incremental rows fetched (inserts/updates) = {incr_count}")
    except Exception as err:
        logger.exception(f"[{table}] Failed to fetch incremental rows: {err}")
        raise

    # =========================================================================
    # STEP 2: Fetch current PKs from source for delete detection
    # =========================================================================
    try:
        current_pk_df = query_execution(
            spark, src_db_type,
            f"SELECT {pk_list} FROM {schema}.{table}",
            config_path, configfile
        )
        logger.info(f"[{table}] Current PKs fetched from source.")
    except Exception as err:
        logger.exception(f"[{table}] Failed to fetch current PKs: {err}")
        raise

    # =========================================================================
    # STEP 3 & 4: Read previous snapshot and detect soft-deletes
    # On first run, no snapshot exists — delete detection is skipped gracefully.
    # =========================================================================
    delete_df = None
    try:
        prev_snapshot_df = spark.read.parquet(snapshot_path)
        prev_active_df   = prev_snapshot_df.filter("soft_delete = 0")

        # Keys present in snapshot but absent from source = soft-deleted rows
        deleted_keys_df = prev_active_df.select(*pk).subtract(current_pk_df.select(*pk))
        delete_count    = deleted_keys_df.count()
        logger.info(f"[{table}] Soft-deleted rows detected = {delete_count}")

        if delete_count > 0:
            # Mark deleted rows with soft_delete=1 for downstream processing
            delete_df = (
                prev_active_df
                .join(deleted_keys_df, pk, "inner")
                .withColumn("soft_delete", lit(1))
            )

    except Exception:
        # No snapshot found — expected on the first run
        logger.info(
            f"[{table}] No previous snapshot found. "
            f"Skipping delete detection (first run)."
        )

    # =========================================================================
    # STEP 5: Merge incremental rows and soft-delete rows into final delta
    # =========================================================================
    final_df    = incr_df.unionByName(delete_df) if delete_df is not None else incr_df
    final_count = final_df.count()

    if final_count == 0:
        logger.info(f"[{table}] No changes detected. Updating control table and exiting.")
        update_pipeline_run_end(
            run_id, "SUCCESS", 0, last_ts, None,
            config_path, configfile, target_type=target_type
        )
        return 0

    logger.info(
        f"[{table}] Final delta row count (inserts + updates + deletes) = {final_count}"
    )

    # =========================================================================
    # STEP 6: Write final delta to cloud storage (incremental path)
    # =========================================================================
    run_timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    incremental_path = (
        f"{base_path}/data/{database}/{schema}/{table}/incremental/{run_timestamp}"
    )

    try:
        final_df.write.mode("overwrite").parquet(incremental_path)
        logger.info(f"[{table}] Delta written to cloud | path='{incremental_path}'")
    except Exception as err:
        logger.exception(f"[{table}] Failed to write delta to cloud storage: {err}")
        raise

    # =========================================================================
    # STEP 7: Archive old snapshot and write updated snapshot
    # =========================================================================
    logger.info(f"[{table}] Updating snapshot | path='{snapshot_path}'")

    try:
        current_df = query_execution(
            spark, src_db_type,
            f"SELECT * FROM {schema}.{table}",
            config_path, configfile
        )
        current_df = current_df.withColumn("soft_delete", lit(0))
        logger.info(f"[{table}] Full current dataset fetched for snapshot update.")
    except Exception as err:
        logger.exception(f"[{table}] Failed to fetch full dataset for snapshot: {err}")
        raise

    # Re-detect deletes against the latest full pull for an accurate snapshot.
    # BUG FIX: Original code reused a stale `prev_snapshot_df` variable from
    # Step 3 here without re-reading it, which could silently reference a
    # DataFrame from an already-overwritten or cached state. We re-read it
    # explicitly and handle first-run gracefully with a fresh try/except.
    deleted_df = None
    try:
        prev_snapshot_df = spark.read.parquet(snapshot_path)
        prev_active_df   = prev_snapshot_df.filter("soft_delete = 0")
        deleted_keys_df  = prev_active_df.select(*pk).subtract(current_df.select(*pk))

        if deleted_keys_df.count() > 0:
            deleted_df = (
                prev_active_df
                .join(deleted_keys_df, pk, "inner")
                .withColumn("soft_delete", lit(1))
            )
    except Exception:
        logger.info(
            f"[{table}] No previous snapshot found for snapshot update. "
            f"Writing fresh snapshot."
        )

    snapshot_final_df = (
        current_df.unionByName(deleted_df) if deleted_df is not None else current_df
    )

    # Materialise to avoid re-computation during archive + write
    snapshot_final_df = snapshot_final_df.cache()
    snapshot_final_df.count()

    # Archive the existing snapshot before overwriting
    source_folder  = f"data/{database}/{schema}/{table}/snapshot/"
    archive_folder = f"archive/{database}/{schema}/{table}/snapshot/{run_timestamp}/"

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
        elif cloud_type == "azure":
            move_src_to_archive_azure(
                folder_name=archive_folder,
                blob_folder_path=source_folder,
                config_path=cfg["config_path"],
                configfile=cfg["configfile"]
            )
        logger.info(f"[{table}] Old snapshot archived to '{archive_folder}'.")
    except Exception as archive_err:
        # Non-fatal — if no existing snapshot to archive, proceed to write the new one
        logger.warning(
            f"[{table}] Snapshot archive skipped (may not exist yet): {archive_err}"
        )

    # Write the new snapshot
    try:
        snapshot_final_df.write.mode("overwrite").parquet(snapshot_path)
        logger.info(f"[{table}] New snapshot written to '{snapshot_path}'.")
    except Exception as err:
        logger.exception(f"[{table}] Failed to write new snapshot: {err}")
        raise
    finally:
        snapshot_final_df.unpersist()

    # =========================================================================
    # STEP 8: Load final delta to target system
    # =========================================================================
    # Convert s3a:// → s3:// for target loader compatibility (AWS only)
    if cloud_type == "aws":
        cloud_path_final = incremental_path.replace("s3a://", "s3://")
    elif cloud_type == "gcp":
        cloud_path_final = incremental_path  # gs:// is already
    elif cloud_type == "azure":
        cloud_path_final = incremental_path
    else:
        raise ValueError(f"Unsupported cloud_type: '{cloud_type}'")

    logger.info(
        f"[{table}] Loading delta to target | "
        f"path='{cloud_path_final}' | target='{target_type}'"
    )

    try:
        load_to_target(
            target_type=target_type,
            cfg=cfg,
            database=database,
            schema=schema,
            table=table,
            primary_keys=pk,
            cloud_path=cloud_path_final,
        )
        logger.info(f"[{table}] Target load completed successfully.")
    except Exception as err:
        logger.exception(f"[{table}] Target load failed: {err}")
        raise

    # =========================================================================
    # STEP 9: Update pipeline run control table
    # =========================================================================
    # Extract the maximum update timestamp from the incremental batch
    # to use as the watermark for the next run.
    # BUG FIX: Original code called select on incr_df which already had
    # soft_delete column added but updt_col may not exist after unionByName
    # with delete_df — we safely extract max_ts from incr_df before the union.
    try:
        max_ts = incr_df.select(spark_max(updt_col)).collect()[0][0]
        logger.info(f"[{table}] Max timestamp from incremental batch = '{max_ts}'")
    except Exception as err:
        logger.warning(
            f"[{table}] Could not extract max timestamp: {err}. Defaulting to None."
        )
        max_ts = None

    try:
        update_pipeline_run_end(
            run_id, "SUCCESS", final_count, max_ts, None,
            config_path, configfile, target_type=target_type
        )
        logger.info(
            f"[{table}] Control table updated | rows={final_count} | max_ts='{max_ts}'"
        )
    except Exception as err:
        logger.exception(f"[{table}] Failed to update control table: {err}")
        raise

    return final_count
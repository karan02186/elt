# =============================================================================
# File        : incr_load.py
# Project     : Data Accelerator – ELT Framework
# Description : Orchestrates the unified incremental load pipeline — iterates
#               over metadata-driven table configs and dispatches each table
#               to either a timestamp-based or snapshot batch CDC load path.
# =============================================================================

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from elt.src.parse_config import parse_config
from elt.src.custom_logger import configure_logging, log_dataframe
from elt.src.connections import spark_conn, query_execution
from elt.src.helper import _cloud_location_path_generate

from elt.src.cloud_aws import upload_to_s3, move_src_to_archive_aws
from elt.src.cloud_gcp import move_src_to_archive_gcp

from elt.src.data_extraction import extraction_incremental, incremental_load_snapshot_compare
from elt.src.incremental_timestamp import incremental_load_timestamp

from elt.src.control_table import (
    check_table_exists_target,
    create_pipeline_run_control_table,
    insert_pipeline_run_start,
    update_pipeline_run_end,
    get_last_success_run_timestamp
)

logger = logging.getLogger("data_accelerator")

# Resolve the project root relative to this file's location
project_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


# =============================================================================
# FUNCTION : incremental_load_src_to_cloud
# =============================================================================
def incremental_load_src_to_cloud(
    src_db_type: str,
    cloud_type: str,
    target_type: str,
    pipeline_id: str = None,
    pipeline_name: str = None
) -> int:
    """
    Entry point for the unified incremental load pipeline. Reads table metadata
    from a CSV file and processes each table using either a timestamp-based
    incremental load or a snapshot batch CDC approach.

    For each table:
      - Checks and creates the PipelineRunControl table if absent.
      - Determines load type (TIMESTAMP or SNAPSHOT) from metadata.
      - Dispatches to the appropriate load handler.
      - Accumulates and returns the total records processed.

    Params:
        src_db_type   (str)         : Source database type (e.g., 'sqlserver').
        cloud_type    (str)         : Cloud provider — 'aws' or 'gcp'.
        target_type   (str)         : Target system (e.g., 'snowflake', 'databricks').
        pipeline_id   (str, optional): Unique identifier for this pipeline run.
                                       Auto-generated if not provided.
        pipeline_name (str, optional): Human-readable pipeline name.
                                       Defaults to pipeline_id if not provided.

    Returns:
        int: Total number of records processed across all tables.

    Raises:
        FileNotFoundError : If the metadata CSV file does not exist.
        KeyError          : If required config keys are missing.
        Exception         : For any unexpected failure during pipeline execution.
    """

    # -------------------------------------------------------------------------
    # Step 1: Resolve config paths and generate a unique pipeline run ID
    # -------------------------------------------------------------------------
    config_dir  = f"{project_path}/config"
    config_path = f"{config_dir}/pipeline_config.cfg"
    configfile  = f"{src_db_type}-{cloud_type}-{target_type}.cfg"

    pipeline_run_id = (
        f"{pipeline_id}_{datetime.now():%Y%m%d%H%M%S}"
        if pipeline_id
        else f"{src_db_type}_{cloud_type}+{target_type}_{datetime.now():%Y%m%d%H%M%S}"
    )

    # -------------------------------------------------------------------------
    # Step 2: Initialise the pipeline logger with run context
    # -------------------------------------------------------------------------
    logger = configure_logging(
        8,
        pipeline_id=pipeline_id,
        src=src_db_type,
        cloud=cloud_type,
        target=target_type
    )

    logger.info("=" * 60)
    logger.info("Unified Incremental Load — START")
    logger.info(f"pipeline_run_id='{pipeline_run_id}' | config='{configfile}'")
    logger.info("=" * 60)

    total_records = 0

    # -------------------------------------------------------------------------
    # Step 3: Parse the full pipeline configuration
    # -------------------------------------------------------------------------
    try:
        cfg = parse_config(
            config_path,
            configfile=configfile,
            src_db_type=src_db_type,
            cloud_type=cloud_type,
            target_type=target_type
        )
        cfg["config_path"] = config_path
        cfg["configfile"]  = configfile
        logger.info("Pipeline configuration loaded successfully.")
    except KeyError as key_err:
        logger.error(f"Missing config key during pipeline setup: {key_err}")
        raise
    except Exception as err:
        logger.exception(f"Failed to load pipeline configuration: {err}")
        raise

    # -------------------------------------------------------------------------
    # Step 4: Ensure the PipelineRunControl table exists in the target
    # -------------------------------------------------------------------------
    try:
        if not check_table_exists_target("PipelineRunControl", config_path, configfile, target_type=target_type):
            logger.info("PipelineRunControl table not found. Creating now.")
            create_pipeline_run_control_table(config_path, configfile, target_type=target_type)
            logger.info("PipelineRunControl table created successfully.")
    except Exception as err:
        logger.exception(f"Failed to verify or create PipelineRunControl table: {err}")
        raise

    # -------------------------------------------------------------------------
    # Step 5: Initialise SparkSession
    # -------------------------------------------------------------------------
    try:
        sql_jar = cfg["spark"]["sqlserver_jar_file_path"]
        spark   = spark_conn(sql_jar, cfg, cloud_type)
        logger.info("SparkSession initialised successfully.")
    except KeyError:
        logger.error("Missing 'sqlserver_jar_file_path' in [spark] config section.")
        raise
    except Exception as err:
        logger.exception(f"Failed to initialise SparkSession: {err}")
        raise

    # -------------------------------------------------------------------------
    # Step 6: Load table metadata from the CSV file
    # -------------------------------------------------------------------------
    metadata_path = f"{project_path}/metadata/{src_db_type}_metadata.csv"

    try:
        meta_df = spark.read.csv(metadata_path, header=True, inferSchema=True)
        log_dataframe(meta_df)
        logger.info(f"Metadata loaded | path='{metadata_path}' | rows={meta_df.count()}")
    except FileNotFoundError:
        logger.error(f"Metadata file not found: '{metadata_path}'")
        raise
    except Exception as err:
        logger.exception(f"Failed to load metadata CSV from '{metadata_path}': {err}")
        raise

    # -------------------------------------------------------------------------
    # Step 7: Iterate over each table in the metadata and run the load
    # -------------------------------------------------------------------------
    for row in meta_df.collect():
        meta     = row.asDict()
        table    = meta["SRC_TBL_NAME"]
        database = meta["DATABASE_NAME"]
        schema   = meta["SCHEMA_NAME"]
        incr_cols = meta["INCR_LOGIC_COL_NAME"]
        # pk       = meta["PRIMARY_KEY_COL"]

        pk = [col.strip() for col in meta["PRIMARY_KEY_COL"].split(",")]
        pk_str = ", ".join(pk)

        # Determine load type — SNAPSHOT if no incremental column is defined
        load_type = (
            "SNAPSHOT"
            if not incr_cols or str(incr_cols).strip().lower() in ("none", "null", "")
            else "TIMESTAMP"
        )

        logger.info(f"Processing table='{table}' | load_type='{load_type}'")

        # Generate the cloud storage destination path for this table
        try:
            cloud_location = _cloud_location_path_generate(cfg, cloud_type, database, schema, table)
        except (KeyError, ValueError) as err:
            logger.error(f"Failed to generate cloud path for '{table}': {err}. Skipping table.")
            continue

        # Register the pipeline run start in the control table
        try:
            run_id = insert_pipeline_run_start(
                pipeline_run_id,
                pipeline_id=pipeline_id,
                pipeline_name=pipeline_name or pipeline_id,
                source_type=src_db_type,
                database=database,
                schema=schema,
                table=table,
                load_type=load_type,
                cloud=cloud_type,
                cloud_location=cloud_location,
                config_path=config_path,
                configfile=configfile,
                target_type=target_type
            )
        except Exception as err:
            logger.exception(f"Failed to insert pipeline run start for '{table}': {err}. Skipping table.")
            continue

        # ------------------------------------------------------------------
        # Dispatch to the appropriate load handler based on load type
        # ------------------------------------------------------------------
        try:
            if load_type == "TIMESTAMP":
                logger.info(f"Dispatching '{table}' to timestamp incremental load.")
                rows = incremental_load_timestamp(
                    spark=spark,
                    database=database,
                    schema=schema,
                    table=table,
                    incr_cols=incr_cols,
                    pk=pk,
                    cfg=cfg,
                    run_id=run_id,
                    cloud_type=cloud_type,
                    target_type=target_type,
                    config_path=config_path,
                    configfile=configfile,
                    pipeline_id=pipeline_id,
                    src_db_type=src_db_type
                )

            else:
                logger.info(f"Dispatching '{table}' to snapshot batch CDC load.")
                rows = incremental_load_snapshot_compare(
                    spark=spark,
                    database=database,
                    schema=schema,
                    table=table,
                    pk=pk,
                    cfg=cfg,
                    run_id=run_id,
                    cloud_type=cloud_type,
                    target_type=target_type,
                    buckets=1   # Number of hash buckets for snapshot comparison — configurable
                )

            total_records += rows or 0
            logger.info(f"Table '{table}' processed successfully | rows={rows or 0}")

        except Exception as err:
            logger.exception(f"Load failed for table '{table}': {err}. Continuing with next table.")
            continue

    # -------------------------------------------------------------------------
    # Step 8: Stop Spark and log pipeline completion summary
    # -------------------------------------------------------------------------
    try:
        spark.stop()
        logger.info("SparkSession stopped.")
    except Exception as err:
        logger.warning(f"Error stopping SparkSession: {err}")

    logger.info("=" * 60)
    logger.info(f"Unified Incremental Load — COMPLETE | total_records={total_records}")
    logger.info("=" * 60)

    return total_records
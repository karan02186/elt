import os
import configparser
from google.cloud import bigquery
from datetime import datetime
import logging

logger = logging.getLogger("data_accelerator")


# -----------------------------
# READ CONFIG FILE
# -----------------------------
def read_config(config_path):
    """
    Reads configuration file and returns config object.
    """
    try:
        config = configparser.ConfigParser()
        files = config.read(config_path)

        if not files:
            raise Exception(f"Config file not found: {config_path}")

        logger.info(f"Reading config from: {config_path}")
        return config

    except Exception as e:
        logger.exception(f"Error reading config: {e}")
        raise


# -----------------------------
# BIGQUERY CONNECTION
# -----------------------------
def bigquery_connection(cfg):
    """
    Creates BigQuery client using service account JSON.
    """
    try:
        client = bigquery.Client.from_service_account_json(
            cfg["bigquery"]["gcp_service_account_key_path"],
            project=cfg["bigquery"]["project"]
        )
        return client

    except Exception as e:
        logger.exception(f"Error creating BigQuery connection: {e}")
        raise


# -----------------------------
# BUILD GCS PATH
# -----------------------------
def build_gcs_path(bucket_name, dataset, schema, table):
    """
    Builds GCS path for incremental files.
    """
    try:
        return f"gs://{bucket_name}/data/{dataset}/{schema}/{table}/incremental/*"
    except Exception as e:
        logger.exception(f"Error building GCS path: {e}")
        raise


# -----------------------------
# GET BUSINESS COLUMNS FROM RAW
# -----------------------------
def get_columns(client, project, dataset, table):
    """
    Retrieves business columns from RAW table
    excluding metadata columns.
    """
    try:
        query = f"""
            SELECT column_name
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
        """

        query_job = client.query(query)

        # Exclude RAW metadata columns
        exclude = ['soft_delete', 'load_ts', 'hash_val', 'file_name',
                   'SOFT_DELETE', 'LOAD_TS', 'HASH_VAL', 'FILE_NAME']
        cols = [row.column_name for row in query_job if row.column_name not in exclude]

        return cols

    except Exception as e:
        logger.exception(f"Error retrieving columns: {e}")
        raise


# -----------------------------
# GENERATE MERGE SQL
# -----------------------------
def generate_merge_sql(target_table, staging_table, primary_keys, columns):
    """
    Generates MERGE SQL dynamically using business columns.

    """
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]
    try:
        pk_condition = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
        # pk_condition = " AND ".join([f"t.{pk} = r.{pk}" for pk in primary_keys])

        # Update columns (exclude PK)
        update_cols = [c for c in columns if c not in primary_keys]
        update_set = ",\n        ".join([f"t.{c} = s.{c}" for c in update_cols])

        # Insert columns
        insert_names = ", ".join(columns + ['insert_ts', 'update_ts', 'is_deleted'])
        insert_values = ", ".join([f"s.{c}" for c in columns] + ['CURRENT_TIMESTAMP()', 'CURRENT_TIMESTAMP()', '0'])

        merge_sql = f"""
MERGE `{target_table}` t
USING (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(
                   PARTITION BY {", ".join(primary_keys)}
                   ORDER BY load_ts DESC
               ) rn
        FROM `{staging_table}`
    )
    WHERE rn = 1
) s
ON {pk_condition}

WHEN MATCHED AND s.soft_delete = 1 THEN
    UPDATE SET
        t.is_deleted = 1,
        t.delete_ts = CURRENT_TIMESTAMP(),
        t.update_ts = CURRENT_TIMESTAMP()

WHEN MATCHED AND s.soft_delete = 0 THEN
    UPDATE SET
        {update_set},
        t.update_ts = CURRENT_TIMESTAMP(),
        t.is_deleted = 0

WHEN NOT MATCHED AND s.soft_delete = 0 THEN
    INSERT ({insert_names})
    VALUES ({insert_values})
"""
        return merge_sql

    except Exception as e:
        logger.exception(f"Error generating merge SQL: {e}")
        raise


# -----------------------------
# TRUNCATE RAW TABLE
# -----------------------------
def truncate_raw_table(client, table_id):
    """
    Truncates RAW staging table after merge.
    """
    try:
        sql = f"TRUNCATE TABLE `{table_id}`"
        client.query(sql).result()
        logger.info("RAW table truncated")

    except Exception as e:
        logger.exception(f"Error truncating RAW table: {e}")
        raise


# -----------------------------
# GET TABLE SCHEMA
# -----------------------------
def get_table_schema(client, table_id):
    """
    Retrieves BigQuery table schema for controlled load.
    """
    try:
        table = client.get_table(table_id)
        return table.schema

    except Exception as e:
        logger.exception(f"Error fetching table schema: {e}")
        raise


# -----------------------------
# MAIN LOAD FUNCTION
# -----------------------------
def load_to_bigquery(cfg, dataset, schema, table, primary_keys):
    """
    Main pipeline:
    Load Parquet -> RAW
    Merge RAW -> TARGET
    Truncate RAW
    """

    start_time = datetime.now()

    try:
        client = bigquery_connection(cfg)

        project = cfg["bigquery"]["project"]
        bucket = cfg["bigquery"]["bucket_name"]

        raw_table = f"{project}.{dataset}_raw.{table}"
        tgt_table = f"{project}.{dataset}_tgt.{table}"

        gcs_path = build_gcs_path(bucket, dataset, schema, table)

        logger.info(f"[BigQuery] Starting load process | path='{gcs_path}'")

        # Get RAW table schema to enforce schema load
        schema = get_table_schema(client, raw_table)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
            ignore_unknown_values=True
        )

        # Load Parquet files into RAW
        logger.info(f"[BigQuery] Loading into RAW table: {raw_table}")
        load_job = client.load_table_from_uri(
            gcs_path,
            raw_table,
            job_config=job_config
        )
        load_job.result()
        copied_rows = int(load_job.output_rows or 0)
        logger.info(f"[BigQuery] RAW load completed | rows={copied_rows}")

        if copied_rows == 0:
            logger.warning(
                f"[BigQuery] No rows loaded into RAW for {dataset}.{schema}.{table}. Skipping MERGE."
            )
            return

        # Get business columns
        columns = get_columns(client, project, f"{dataset}_raw", table)

        # Generate merge SQL
        merge_sql = generate_merge_sql(tgt_table, raw_table, primary_keys, columns)
        # Execute merge
        logger.info(f"[BigQuery] Running MERGE into TARGET: {tgt_table}")
        client.query(merge_sql).result()

        # Truncate RAW after merge
        truncate_raw_table(client, raw_table)

        end_time = datetime.now()

        logger.info(
            f"[BigQuery] Pipeline completed successfully | start={start_time} | end={end_time}"
        )

    except Exception as e:
        logger.exception(f"[BigQuery] Pipeline failed: {e}")
        raise

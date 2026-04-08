import os
import configparser
from google.cloud import bigquery
from datetime import datetime


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

        print("Reading config from:", config_path)
        return config

    except Exception as e:
        print("Error reading config:", str(e))
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
        print("Error creating BigQuery connection:", str(e))
        raise


# -----------------------------
# BUILD GCS PATH
# -----------------------------
def build_gcs_path(bucket_name, dataset, schema, table):
    """
    Builds GCS path for incremental files.
    """
    try:
        return f"gs://{bucket_name}/data/{dataset.lower()}/{schema.lower()}/{table.lower()}/incremental/*"
    except Exception as e:
        print("Error building GCS path:", str(e))
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
        exclude = ['soft_delete', 'load_ts', 'row_hash', 'file_name',
                   'SOFT_DELETE', 'LOAD_TS', 'ROW_HASH', 'FILE_NAME']
        cols = [row.column_name for row in query_job if row.column_name not in exclude]

        return cols

    except Exception as e:
        print("Error retrieving columns:", str(e))
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
                   ORDER BY {", ".join(primary_keys)}
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
        print("Error generating merge SQL:", str(e))
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
        print("RAW table truncated")

    except Exception as e:
        print("Error truncating RAW table:", str(e))
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
        print("Error fetching table schema:", str(e))
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

        raw_table = f"{project}.{dataset.lower()}_raw.{table.lower()}"
        tgt_table = f"{project}.{dataset.lower()}_tgt.{table.lower()}"

        gcs_path = build_gcs_path(bucket, dataset, schema, table)

        print("Starting Load Process")
        print("GCS Path:", gcs_path)

        # Get RAW table schema to enforce schema load
        schema = get_table_schema(client, raw_table)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
            ignore_unknown_values=True
        )

        # Load Parquet files into RAW
        print(f"Loading data into RAW table: {raw_table}")
        load_job = client.load_table_from_uri(
            gcs_path,
            raw_table,
            job_config=job_config
        )
        load_job.result()
        print("COPY INTO RAW Completed")

        # Get business columns
        columns = get_columns(client, project, f"{dataset.lower()}_raw", table.lower())

        # Generate merge SQL
        merge_sql = generate_merge_sql(tgt_table, raw_table, primary_keys, columns)
        print(merge_sql)

        # Execute merge
        print("Running MERGE into TARGET")
        client.query(merge_sql).result()

        # Truncate RAW after merge
        truncate_raw_table(client, raw_table)

        end_time = datetime.now()

        print("MERGE Completed")
        print("Pipeline Completed Successfully")
        print("Start Time:", start_time)
        print("End Time:", end_time)

    except Exception as e:
        print("Pipeline failed:", str(e))
        raise
from elt.src.targets.snowflake_loader import load_to_snowflake
from elt.src.targets.databricks_loader import load_to_databricks
from elt.src.targets.bigquery_loader import load_to_bigquery


def load_to_target(
        target_type,
        cfg,
        database,
        schema,
        table,
        primary_keys,
        cloud_path
    ):
    Audit_Columns = {
        "soft_delete": "__soft_delete",
        "is_deleted": "__is_deleted",
        "load_ts": "__load_ts",
        "insert_ts": "__insert_timestamp",
        "update_ts": "__update_timestamp",
        "delete_ts": "__delete_timestamp"
    }

    # Normalize audit column case to uppercase
    Audit_Columns = {k: v.upper() for k, v in Audit_Columns.items()}

    target_type = target_type.lower()

    if target_type == "snowflake":
        print("Loading to Snowflake...")
        load_to_snowflake(
            cfg,
            database,
            schema,
            table,
            primary_keys
        )

    elif target_type == "databricks":
        print("Loading to Databricks...")
        load_to_databricks(
            cfg,
            database,
            schema,
            table,
            primary_keys,
            cloud_path
        )
    elif target_type == "bigquery":
        print("Loading to BigQuery...")
        load_to_bigquery(
            cfg,
            database,
            schema,
            table,
            primary_keys
        )

    else:
        raise ValueError(f"Unsupported target type: {target_type}")

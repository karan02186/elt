from elt.src.targets.snowflake_loader import load_to_snowflake
from elt.src.targets.databricks_loader import load_to_databricks
from elt.src.targets.bigquery_loader import load_to_bigquery
import logging

logger = logging.getLogger("data_accelerator")


def load_to_target(
        target_type,
        cfg,
        database,
        schema,
        table,
        primary_keys,
        cloud_path
    ):
    target_type = target_type.lower()

    if target_type == "snowflake":
        logger.info("Loading to Snowflake...")
        load_to_snowflake(
            cfg,
            database,
            schema,
            table,
            primary_keys
        )

    elif target_type == "databricks":
        logger.info("Loading to Databricks...")
        load_to_databricks(
            cfg,
            database,
            schema,
            table,
            primary_keys,
            cloud_path
        )
    elif target_type == "bigquery":
        logger.info("Loading to BigQuery...")
        load_to_bigquery(
            cfg,
            database,
            schema,
            table,
            primary_keys
        )

    else:
        raise ValueError(f"Unsupported target type: {target_type}")

# =============================================================================
# File        : helper.py
# Project     : Data Accelerator – ELT Framework
# Description : General-purpose helper utilities for the ELT pipeline —
#               currently handles cloud storage path generation for AWS and GCP.
# Author      : Data Engineering Team
# Created     : 2024-01-01
# Modified    : 2025-01-01
# Version     : 1.1.0
# =============================================================================

import logging

logger = logging.getLogger("data_accelerator")


def _cloud_location_path_generate(cfg: dict, cloud_type: str, database: str, schema: str, table: str) -> str:
    """
    Generates the cloud storage path for a given table based on the cloud provider.

    Params:
        cfg        (dict): Parsed pipeline configuration dictionary.
        cloud_type (str) : Cloud provider — 'aws' or 'gcp'.
        database   (str) : Source database name.
        schema     (str) : Source schema name.
        table      (str) : Source table name.

    Returns:
        str: Fully qualified cloud storage path (s3a:// or gs://).

    Raises:
        KeyError   : If required bucket/project keys are missing in the config.
        ValueError : If an unsupported cloud_type is provided.
    """
    logger.debug(f"Generating cloud path | cloud='{cloud_type}' | table='{database}.{schema}.{table}'")

    try:
        if cloud_type == "aws":
            bucket = cfg['aws']['s3_bucket_name']
            cloud_location = f"s3a://{bucket}/data/{database}/{schema}/{table}"

        elif cloud_type == "gcp":
            bucket = cfg['gcp']['gcp_bucket_name']
            cloud_location = f"gs://{bucket}/data/{database}/{schema}/{table}"

        elif cloud_type == "azure":

            container = cfg["azure"]["azure_container_name"]
            account = cfg["azure"]["azure_storage_account_key"]

            cloud_location = (
                f"abfss://{container}@{account}.dfs.core.windows.net/"
                f"data/{database}/{schema}/{table}"
            )

        else:
            logger.error(f"Unsupported cloud_type: '{cloud_type}'. Expected 'aws' or 'gcp'.")
            raise ValueError(f"Unsupported cloud_type: '{cloud_type}'. Expected 'aws' or 'gcp'.")

    except KeyError as key_err:
        logger.error(f"Missing config key while generating cloud path: {key_err}. Check the [{cloud_type}] section in config.")
        raise

    logger.debug(f"Cloud path generated: '{cloud_location}'")
    return cloud_location
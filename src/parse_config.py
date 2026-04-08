import os
import json
import configparser
from typing import Dict, Any
import logging

# Set up the global logger
# logger = logging.getLogger(__name__)
logger = logging.getLogger("data_accelerator")



def parse_config(config_path: str,configfile:str = None, src_db_type: str = None, cloud_type: str = None,
                 target_type: str = None) -> Dict[str, Dict[str, Any]]:
    """
    Parses the configuration file located at config_path and returns the configuration
    as a dictionary with nested dictionaries for each section.

    Params:
        config_path (str): The path to the configuration file.
        src_db_type (str, optional): The type of database (e.g., 'sqlserver', 'mysql', 'postgresql').
        cloud_type (str, optional): The type of cloud (e.g., 'aws', 'azure', 'gcp').

    Returns:
        Dict[str, Dict[str, Any]]: A dictionary containing the parsed configuration sections
        and their key-value pairs.

    Raises:
        FileNotFoundError: If the specified config file cannot be found.
        KeyError: If a required key is missing from the configuration file.
        ValueError: If an unsupported db_type or cloud_type is provided.
        Exception: For any other unforeseen errors during the configuration parsing process.
    """
    logger.info(f"parse config for {src_db_type},{cloud_type},{target_type}")
    try:
        # Get the directory path from the given config file path
        config_dir = os.path.dirname(config_path)
        logger.info(f"config file is :{configfile}")
        
        # List of configuration files to read
        config_files = [
            os.path.join(config_dir, "pipeline_config.cfg"),
            os.path.join(config_dir, configfile),
        ]
        logger.info(f"filea are :{config_files}")

        # Initialize configparser to read the configuration file
        config = configparser.RawConfigParser(interpolation=None)

        # Read all config files
        read_files = config.read(config_files)
        logger.info(f"read files :{read_files}")
        
        # Check if any config file was missing
        if not read_files:
            print(f"No configuration files found in {config_dir}")
            raise FileNotFoundError(f"No configuration files found in {config_dir}")

        # Initialize dictionary to store parsed values
        parsed_config: Dict[str, Dict[str, Any]] = {}

        # Parse Common configuration
        if config.has_section('SPARK'):
            parsed_config['spark'] = {k: config.get('SPARK', k) for k in config.options('SPARK')}

        # Source DB
        if src_db_type:
            logger.info(f"src is {src_db_type}")
            src_db_type = src_db_type.upper()
            if config.has_section(src_db_type):
                parsed_config[src_db_type.lower()] = {
                    k: config.get(src_db_type, k, fallback=None) for k in config.options(src_db_type)
                }

        # Target type (Snowflake / Databricks)
        if target_type:
            target_type_upper = target_type.upper()

            if config.has_section(target_type_upper):
                parsed_config[target_type.lower()] = {
                    k: config.get(target_type_upper, k, fallback=None) for k in config.options(target_type_upper)
                }

            # Example: SQLSERVER_SNOWFLAKE or MYSQL_DATABRICKS
            if src_db_type:
                composite_key = f"{src_db_type}_{target_type_upper}"
                if config.has_section(composite_key):
                    merged_target = parsed_config.get(target_type.lower(), {}).copy()
                    merged_target.update({
                        k: config.get(composite_key, k, fallback=None) for k in config.options(composite_key)
                    })
                    parsed_config[target_type.lower()] = merged_target

        # Handle Cloud Configuration
        cloud_sections = {
            "AWS": ["aws_access_key_id", "aws_secret_access_key", "aws_region", "s3_bucket_name", "s3_bucket_path"],
            "AZURE": ["azure_storage_account_name", "azure_storage_account_key", "azure_container_name",
                      "azure_blob_service_url", "azure_sas_token","azure_connection_string"],
            "GCP": ["gcp_project_id", "gcp_service_account_key_path", "gcp_bucket_name", "gcp_storage_url"]
        }

        if cloud_type:
            # logger.info(f"cloud type is {cloud_type}")
            cloud_type_upper = cloud_type.upper()
            # logger.info(f"cloud name is {cloud_type_upper}")
            if cloud_type_upper in cloud_sections and config.has_section(cloud_type_upper):
                parsed_config[cloud_type.lower()] = {
                    key: config.get(cloud_type_upper, key, fallback=None) for key in cloud_sections[cloud_type_upper]
                }
                logger.info(f"after cloud is {parsed_config}")
        logger.info(f"at end is {parsed_config}")
        return parsed_config

    except (FileNotFoundError, KeyError, ValueError) as err:
        logger.exception(f"Configuration error: {err}")
        raise Exception(f"Error parsing the configuration file: {err}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred while parsing the config files: {e}")
        raise Exception(f"An unexpected error occurred while parsing the config files: {e}")

# cfg = parse_config(
#     r"C:\Users\Neelkant\PycharmProjects\personal\dla\config\pipeline_config.cfg"
# )
#
# # Pretty print JSON
# print(json.dumps(cfg, indent=4))

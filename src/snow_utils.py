import pandas as pd
import snowflake.connector
from datetime import timedelta

from elt.src.parse_config import parse_config

import logging

# Set up the global logger
# logger = logging.getLogger(__name__)
logger = logging.getLogger("data_accelerator")


def last_run_time_def(snowflake_conn: snowflake.connector.connection.SnowflakeConnection, src_tbl_name: str,
                      sf_database: str, sf_schema: str) -> any:
    """
    Retrieves and adjusts the last run time from the control table in Snowflake.

    Parameters:
        snowflake_conn (snowflake.connector.connection.SnowflakeConnection): Snowflake connection object.
        src_tbl_name (str): The source table name.
        sf_database (str): The Snowflake database name.
        sf_schema (str): The Snowflake schema name.

    Returns:
        str: The adjusted last run time in the format '%Y-%m-%d %H:%M:%S.%f'.
             If no successful runs are found, returns None.
    """
    # Create a cursor object to interact with the Snowflake database
    cursor = snowflake_conn.cursor()

    query = (
            f"SELECT MAX_TIMESTAMP FROM {sf_database}.{sf_schema}.PIPELINE_CONTROL_TABLE"
            + "\n"
              f"WHERE SOURCE_OBJECT_NAME = '{src_tbl_name}'"
            + "\n"
              f"AND STATUS = 'SUCCESS'"
            + "\n"
              f"AND MAX_TIMESTAMP <> '9999-12-31 23:59:59.999'"
            + "\n"
              f"AND ROWS_INSERTED <> 0 "
            + "\n"
              f"ORDER BY RUN_ID DESC LIMIT 1;"
    )

    # Execute the query
    cursor.execute(query)

    result = cursor.fetchone()

    if result and result[0] is not None:
        last_run_time = result[0]
        # Subtract 10 minutes from last_run_time
        new_last_run_time = last_run_time - timedelta(minutes=10)
        # Format the new last run time
        formatted_last_run_time = new_last_run_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # Close the cursor if it's a local cursor
        if cursor:
            cursor.close()

        return formatted_last_run_time
    else:
        logger.info("No successful runs found for the specified conditions.")

        # Close the cursor if it's a local cursor
        if cursor:
            cursor.close()

        return None


def cols_in_snowflake(snowflake_conn: snowflake.connector.connection.SnowflakeConnection,
                      src_db_type, trg_tbl_name: str, config_path: str,configfile:str) -> list:
    """
    Retrieves column names from a Snowflake table, excluding specific control columns.

    Params:
        snowflake_conn (snowflake.connector.connection.SnowflakeConnection): Snowflake connection object.
        trg_tbl_name (str): The name of the Snowflake table.
        config_path (str): The path to the configuration file.

    Returns:
        list: A list of column names in the specified Snowflake table, excluding 'EDW_INSERTED_TIMESTAMP'
              and 'EDW_UPDATED_TIMESTAMP'.
    """
    # Parse the configuration file to get Snowflake connection parameters
    parsed_config = parse_config(config_path,configfile, src_db_type)

    # Extracting Snowflake parameters from parsed_config dictionary
    trg_database = parsed_config['snowflake']['database']
    trg_schema = parsed_config['snowflake']['schema']

    # Create a cursor object to interact with the Snowflake database
    cursor = snowflake_conn.cursor()

    # Construct the query to retrieve column names
    cols_query = (
            f"SELECT COLUMN_NAME FROM {trg_database.upper()}.INFORMATION_SCHEMA.COLUMNS \n"
            +
            f"WHERE TABLE_NAME = '{trg_tbl_name.upper()}' AND TABLE_SCHEMA = '{trg_schema.upper()}' \n"
            +
            f"AND COLUMN_NAME NOT IN ('EDW_INSERTED_TIMESTAMP', 'EDW_UPDATED_TIMESTAMP') \n"
            +
            f"ORDER BY ORDINAL_POSITION"
    )

    # Execute the query and fetch the data
    data = cursor.execute(cols_query).fetchall()

    # Extract column names from the fetched data
    column_names = [row[0] for row in data]

    # Close the cursor if it's a local cursor
    if cursor:
        cursor.close()

    return column_names


def alter_tbl_add_col(snowflake_conn: snowflake.connector.connection.SnowflakeConnection, src_db_type: str,
                      trg_tbl_name: str, additional_columns_data_types: dict, config_path: str,configfile:str) -> None:
    """
    Executes queries in Snowflake to alter the table and add new columns if any.

    Params:
        snowflake_conn (snowflake.connector.connection.SnowflakeConnection): Snowflake connection object.
        src_db_type (str): The name of the source database (e.g., "sqlserver", "postgresql", "mongodb", "mysql").
        trg_tbl_name (str): The Snowflake target table name.
        additional_columns_data_types (dict): Dictionary containing column names and their respective data types.
        config_path (str): The path to the configuration file.

    Returns:
        None
    """
    from elt.src.constants import project_path

    # Retrieve configuration parameters
    parsed_config = parse_config(config_path,configfile, src_db_type)
    trg_database = parsed_config['snowflake']['database']
    trg_schema = parsed_config['snowflake']['schema']
    stg_database = parsed_config['snowflake']['stg_database']
    stg_schema = parsed_config['snowflake']['stg_schema']
    dtype_comp_file_path = f'{project_path}/metadata/datatype_mapping.csv'

    # Create a cursor object to interact with the Snowflake database
    cursor = snowflake_conn.cursor()

    # Read the datatype mapping from the CSV file
    mapping = pd.read_csv(dtype_comp_file_path)

    # Alter table for each new column
    for col_name, data_type in additional_columns_data_types.items():
        # Loop through the rows of the 'mapping' DataFrame to find the Snowflake equivalent of the src_db datatype
        for index, row in mapping.iterrows():
            if row[src_db_type.upper()].lower() == data_type.lower():
                sf_dtype = row['SNOWFLAKE']

                # Generate and execute the ALTER TABLE statements for both the target and staging tables
                alter_trg_tbl = f"ALTER TABLE {trg_database}.{trg_schema}.{trg_tbl_name} ADD {col_name} {sf_dtype};"
                alter_stg_tbl = f"ALTER TABLE {stg_database}.{stg_schema}.{trg_tbl_name} ADD {col_name} {sf_dtype};"

                # Execute the alter table statements
                cursor.execute(alter_trg_tbl)
                cursor.execute(alter_stg_tbl)

    # Close the cursor if it's a local cursor
    if cursor:
        cursor.close()


def alter_tbl_del_col(snowflake_conn: snowflake.connector.connection.SnowflakeConnection, src_db_type: str,
                      trg_tbl_name: str, col_name: str, config_path: str, configfile:str) -> None:
    """
    Executes a query in Snowflake to alter the table and drop a column.

    Params:
        snowflake_conn (snowflake.connector.connection.SnowflakeConnection): Snowflake connection object.
        src_db_type (str): The name of the source database (e.g., "sqlserver", "postgresql", "mongodb", "mysql").
        trg_tbl_name (str): The Snowflake target table name.
        col_name (str): The name of the column to be dropped.
        config_path (str): Path to the configuration file.

    Returns:
        None
    """
    # Retrieve configuration parameters
    parsed_config = parse_config(config_path, configfile, src_db_type)
    trg_database = parsed_config['snowflake']['database']
    trg_schema = parsed_config['snowflake']['schema']

    # Create a cursor object to interact with the Snowflake database
    cursor = snowflake_conn.cursor()

    # Construct the ALTER TABLE query to drop the column
    tbl_cols_query = (
            f"ALTER TABLE {trg_database}.{trg_schema}.{trg_tbl_name} "
            + "\n"
              f"DROP COLUMN {col_name};"
    )

    # Execute the query
    cursor.execute(tbl_cols_query)

    # Close the cursor if it's a local cursor
    if cursor:
        cursor.close()

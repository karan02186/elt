# =============================================================================
# File        : connections.py
# Project     : Data Accelerator – ELT Framework
# Description : Database and cloud connection utilities — Spark, Snowflake,
#               Databricks, BigQuery, SQL Server (JDBC + pyodbc).
# =============================================================================


from pyspark.sql import SparkSession, DataFrame          # type: ignore
from pyspark.sql.utils import AnalysisException         # type: ignore

import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

import pyodbc
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from databricks import sql as databricks_sql
from databricks.sql.exc import OperationalError as DatabricksOperationalError

from elt.src.parse_config import parse_config

import logging

logger = logging.getLogger("data_accelerator")


# def spark_conn(sqlserver_jar: str, cfg: dict, cloud_type: str) -> SparkSession:
#     """
#     Builds and returns a local SparkSession configured for the specified cloud provider.
#     """
#
#     logger.info(f"Building SparkSession | cloud='{cloud_type}'")
#
#     try:
#
#         builder = (
#             SparkSession.builder
#             .master("local[*]")
#             .appName("Unified_Incremental_Load")
#             .config("spark.driver.memory", "4g")
#             .config("spark.local.dir", "D:/spark_temp")
#         )
#
#         # Ensure jar path is file:///
#         sqlserver_jar = "file:///" + sqlserver_jar.replace("\\", "/")
#
#         jars = [sqlserver_jar]
#
#         if cloud_type == "gcp":
#             gcs_jar = cfg["spark"]["gcs_connector_jar_file_path"]
#             gcs_jar = "file:///" + gcs_jar.replace("\\", "/")
#             jars.append(gcs_jar)
#
#         builder = builder.config("spark.jars", ",".join(jars))
#
#         if cloud_type == "aws":
#             builder = (
#                 builder
#                 .config("spark.hadoop.fs.s3a.access.key",    cfg["aws"]["aws_access_key_id"])
#                 .config("spark.hadoop.fs.s3a.secret.key",    cfg["aws"]["aws_secret_access_key"])
#                 .config("spark.hadoop.fs.s3a.endpoint",      "s3.eu-north-1.amazonaws.com")
#                 .config("spark.hadoop.fs.s3a.region",        cfg["aws"]["aws_region"])
#                 .config("spark.hadoop.fs.s3a.impl",          "org.apache.hadoop.fs.s3a.S3AFileSystem")
#             )
#
#         elif cloud_type == "gcp":
#             builder = (
#                 builder
#                 .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
#                 .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
#                         cfg["gcp"]["gcp_service_account_key_path"])
#                 .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#                 .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
#                         "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
#             )
#
#         spark = builder.getOrCreate()
#         logger.info("SparkSession created successfully.")
#         return spark
#
#     except Exception as err:
#         logger.exception(f"Failed to create SparkSession: {err}")
#         raise

def spark_conn(sqlserver_jar, cfg, cloud_type):

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("Unified_Incremental_Load")
        .config("spark.driver.memory", "4g")
    )

    # -----------------------------------
    # Combine required jars dynamically
    # -----------------------------------
    jars = [sqlserver_jar]

    if cloud_type == "gcp":
        gcs_jar = cfg["spark"]["gcs_connector_jar_file_path"]
        jars.append(gcs_jar)

    builder = builder.config(
        "spark.jars",
        ",".join(jars)
    )

    # ---------------- AWS ----------------
    if cloud_type == "aws":

        builder = (
            builder
            .config(
                "spark.hadoop.fs.s3a.access.key",
                cfg["aws"]["aws_access_key_id"]
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                cfg["aws"]["aws_secret_access_key"]
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                "s3.eu-north-1.amazonaws.com"
            )
            .config(
                "spark.hadoop.fs.s3a.region",
                cfg["aws"]["aws_region"]
            )
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
        )

    # ---------------- GCP ----------------
    elif cloud_type == "gcp":

        builder = (
            builder
            .config(
                "spark.hadoop.google.cloud.auth.service.account.enable",
                "true"
            )
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                cfg["gcp"]["gcp_service_account_key_path"]
            )
            .config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
            )
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
            )
        )
    elif cloud_type == "azure":

        account = cfg["azure"]["azure_storage_account_name"]
        key = cfg["azure"]["azure_storage_account_key"]

        builder = builder \
            .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6"
        ) \
            .config(
            f"spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net",
            key
        )
    return builder.getOrCreate()

def data_conn(config_path: str, configfile: str):
    """
    Creates and returns a Databricks SQL connection.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        databricks.sql.client.Connection: Active Databricks SQL connection.

    Raises:
        KeyError                  : If required Databricks keys are missing in config.
        DatabricksOperationalError: If the connection to Databricks fails.
        Exception                 : For any other unexpected failure.
    """
    logger.info(f"Establishing Databricks connection | config='{configfile}'")
    try:
        parsed_config = parse_config(
            config_path, configfile=configfile, src_db_type=None, cloud_type=None, target_type='databricks'
        )

        try:
            server_hostname = parsed_config['databricks']['server_hostname']
            http_path       = parsed_config['databricks']['http_path']
            access_token    = parsed_config['databricks']['access_token']
        except KeyError as key_err:
            logger.error(f"Missing Databricks config key: {key_err}. Check [databricks] section in '{configfile}'.")
            raise

        conn = databricks_sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )

        logger.info("Databricks connection established.")
        return conn

    except DatabricksOperationalError as err:
        logger.exception(f"Databricks operational error: {err}")
        raise Exception(f"Failed to connect to Databricks: {err}") from err
    except Exception as err:
        logger.exception(f"Unexpected error establishing Databricks connection: {err}")
        raise


def snow_conn(config_path: str, configfile: str) -> SnowflakeConnection:
    """
    Creates and returns a Snowflake connection.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        SnowflakeConnection: Active Snowflake connection object.

    Raises:
        KeyError        : If required Snowflake keys are missing in config.
        SnowflakeError  : If the Snowflake connection attempt fails.
        Exception       : For any other unexpected failure.
    """
    logger.info(f"Establishing Snowflake connection | config='{configfile}'")
    try:
        parsed_config = parse_config(
            config_path, configfile=configfile, src_db_type=None, cloud_type=None, target_type='snowflake'
        )

        try:
            account   = parsed_config['snowflake']['account']
            user      = parsed_config['snowflake']['user']
            password  = parsed_config['snowflake']['password']
            role      = parsed_config['snowflake']['role']
            warehouse = parsed_config['snowflake']['warehouse']
            database  = parsed_config['snowflake']['database']
        except KeyError as key_err:
            logger.error(f"Missing Snowflake config key: {key_err}. Check [snowflake] section in '{configfile}'.")
            raise

        snowflake_conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            role=role,
            warehouse=warehouse,
            database=database
        )

        logger.info(f"Snowflake connection established | account='{account}' | database='{database}'")
        return snowflake_conn

    except SnowflakeError as err:
        logger.exception(f"Snowflake connection error: {err}")
        raise Exception(f"Failed to connect to Snowflake: {err}") from err
    except Exception as err:
        logger.exception(f"Unexpected error establishing Snowflake connection: {err}")
        raise


def bigquery_conn(config_path: str, configfile: str) -> bigquery.Client:
    """
    Creates and returns a BigQuery client using a GCP service account key.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        google.cloud.bigquery.Client: Authenticated BigQuery client.

    Raises:
        KeyError         : If required BigQuery keys are missing in config.
        GoogleCloudError : If the BigQuery client creation fails.
        Exception        : For any other unexpected failure.
    """
    logger.info(f"Establishing BigQuery connection | config='{configfile}'")
    try:
        parsed_config = parse_config(
            config_path, configfile=configfile, src_db_type=None, cloud_type=None, target_type='bigquery'
        )

        try:
            key_path = parsed_config["bigquery"]["gcp_service_account_key_path"]
            project  = parsed_config["bigquery"]["project"]
        except KeyError as key_err:
            logger.error(f"Missing BigQuery config key: {key_err}. Check [bigquery] section in '{configfile}'.")
            raise

        client = bigquery.Client.from_service_account_json(key_path, project=project)
        logger.info(f"BigQuery client created | project='{project}'")
        return client

    except GoogleCloudError as err:
        logger.exception(f"GCP error creating BigQuery client: {err}")
        raise Exception(f"Failed to create BigQuery client: {err}") from err
    except Exception as err:
        logger.exception(f"Unexpected error establishing BigQuery connection: {err}")
        raise


# def query_execution(
#     spark: SparkSession,
#     src_db_type: str,
#     query: str,
#     config_path: str,
#     configfile: str
# ) -> DataFrame:
#     """
#     Executes a SQL query against a source database via Spark JDBC
#     and returns the result as a Spark DataFrame.
#     """
#
#     logger.info(f"Executing query | source = '{src_db_type}'")
#
#     try:
#         parsed_config = parse_config(
#             config_path,
#             configfile=configfile,
#             src_db_type=src_db_type
#         )
#
#         if src_db_type.lower() == "sqlserver":
#
#             server   = parsed_config['sqlserver']['server_name']
#             user     = parsed_config['sqlserver']['user']
#             password = parsed_config['sqlserver']['password']
#             database = parsed_config['sqlserver']['database_name']
#
#             jdbc_url = (
#                 f"jdbc:sqlserver://{server}:1433;"
#                 f"databaseName={database};"
#                 "encrypt=false;"
#                 "trustServerCertificate=true;"
#             )
#
#             logger.info("Reading data from SQL Server via Spark JDBC")
#
#             df = (
#                 spark.read.format("jdbc")
#                 .option("url", jdbc_url)
#                 .option("user", user)
#                 .option("password", password)
#                 .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
#                 .option("dbtable", f"({query}) as src")
#                 .load()
#             )
#
#         else:
#             raise ValueError(f"Unsupported source database: '{src_db_type}'")
#
#         logger.info("Query executed successfully.")
#         return df
#
#     except Exception as err:
#         logger.exception(f"Error executing query on '{src_db_type}': {err}")
#         raise Exception(f"Query execution failed: {err}") from err

def query_execution(spark: SparkSession, src_db_type: str, query: str, config_path: str, configfile: str) -> DataFrame:
    """
    Executes a query on a specified source database and returns the result as a Spark DataFrame.

    Params:
        spark: SparkSession object
        src_db_type (str): The name of the source database (e.g., "sqlserver", "postgresql", "mongodb", "mysql").
        query (str): The SQL query.
        config_path (str): The path to the configuration file.

    Returns:
        DataFrame: A DataFrame containing the query results.

    Raises:
        ValueError: If the source database name is not supported.
        Exception: If any error occurs during the execution of the query.
    """
    logger.info("query_execution started")
    try:
        # Parse the configuration file to get database connection parameters

        parsed_config = parse_config(config_path, configfile=configfile, src_db_type=src_db_type)

        if src_db_type.lower() == "sqlserver":
            # Extract parameters for SQL Server
            sql_server_name = parsed_config[src_db_type.lower()]['server_name']
            sql_server_user = parsed_config[src_db_type.lower()]['user']
            sql_server_password = parsed_config[src_db_type.lower()]['password']
            sql_server_database = parsed_config[src_db_type.lower()]['database_name']

            logger.info("SQL Server query execution started")

            base_jdbc_url = f"jdbc:sqlserver://{sql_server_name}:1433;databaseName={sql_server_database}"

            try:
                # 🔹 Try secure TLS connection first
                sql_server_options = {
                    "url": f"{base_jdbc_url};encrypt=true;trustServerCertificate=true;",
                    "user": sql_server_user,
                    "password": sql_server_password,
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "longStringLength": "2147483647",
                }

                logger.info("Attempting secure TLS connection for SQL Server JDBC.")
                result = spark.read \
                    .format("jdbc") \
                    .options(**sql_server_options) \
                    .option("query", query) \
                    .load()

            except Exception as e:
                error_msg = str(e)
                if "TLS10" in error_msg or "SSLHandshakeException" in error_msg:
                    logger.warning(
                        "Secure JDBC connection failed due to TLS mismatch. "
                        "Retrying with encrypt=false based on fallback policy."
                    )
                    sql_server_options["url"] = f"{base_jdbc_url};encrypt=false;trustServerCertificate=true;"
                    result = spark.read \
                        .format("jdbc") \
                        .options(**sql_server_options) \
                        .option("query", query) \
                        .load()
                else:
                    # Raise other exceptions normally
                    raise

            logger.info("SQL Server query executed successfully.")



        else:
            # Handle invalid database type
            logger.error(f"Unsupported source database: {src_db_type}")
            raise ValueError(f"Unsupported source database: {src_db_type}")

        return result

    except (AnalysisException, Exception) as error:
        # Raise an exception if any error occurs
        logger.exception(f"Error executing query: {error}")
        raise Exception(f"Error executing query: {error}")

def sqlserver_conn(config_path: str, configfile: str):
    """
    Creates and returns a pyodbc SQL Server connection for DDL operations.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        pyodbc.Connection: Active pyodbc SQL Server connection.

    Raises:
        KeyError   : If required SQL Server keys are missing in config.
        pyodbc.Error: If the connection attempt fails.
        Exception  : For any other unexpected failure.
    """
    logger.info(f"Establishing SQL Server (pyodbc) connection | config='{configfile}'")
    try:
        parsed_config = parse_config(config_path, configfile=configfile, src_db_type='sqlserver')

        try:
            server   = parsed_config['sqlserver']['server_name']
            database = parsed_config['sqlserver']['database_name']
            username = parsed_config['sqlserver']['user']
            password = parsed_config['sqlserver']['password']
        except KeyError as key_err:
            logger.error(f"Missing SQL Server config key: {key_err}. Check [sqlserver] section in '{configfile}'.")
            raise

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
            "Encrypt=yes;TrustServerCertificate=yes"
        )

        connection = pyodbc.connect(conn_str)
        logger.info(f"SQL Server connection established | server='{server}' | database='{database}'")
        return connection

    except pyodbc.Error as err:
        logger.exception(f"pyodbc error connecting to SQL Server: {err}")
        raise Exception(f"Failed to connect to SQL Server: {err}") from err
    except Exception as err:
        logger.exception(f"Unexpected error establishing SQL Server connection: {err}")
        raise


def execute_ddl_sql(sql_text: str, config_path: str, configfile: str) -> None:
    """
    Executes a DDL SQL statement on SQL Server using pyodbc.
    Intended for schema-level operations such as CREATE, ALTER, or DROP.

    Params:
        sql_text    (str): DDL SQL statement to execute.
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        None

    Raises:
        KeyError    : If required SQL Server keys are missing in config.
        pyodbc.Error: If DDL execution fails at the database level.
        Exception   : For any other unexpected failure.
    """
    logger.info("Executing DDL SQL statement.")
    try:
        parsed_config = parse_config(config_path, configfile=configfile, src_db_type='sqlserver')

        try:
            server   = parsed_config['sqlserver']['server_name']
            database = parsed_config['sqlserver']['database_name']
            username = parsed_config['sqlserver']['user']
            password = parsed_config['sqlserver']['password']
        except KeyError as key_err:
            logger.error(f"Missing SQL Server config key: {key_err}. Check [sqlserver] section in '{configfile}'.")
            raise

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
            "Encrypt=yes;TrustServerCertificate=yes"
        )

        with pyodbc.connect(conn_str) as conn, conn.cursor() as cursor:
            cursor.execute(sql_text)
            conn.commit()

        logger.info("DDL statement executed and committed successfully.")

    except pyodbc.Error as err:
        logger.exception(f"pyodbc error executing DDL: {err}")
        raise Exception(f"DDL execution failed: {err}") from err
    except Exception as err:
        logger.exception(f"Unexpected error executing DDL: {err}")
        raise

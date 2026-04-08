# # # batching checksum
from elt.src.incr_load import incremental_load_src_to_cloud
# incremental_load_src_to_cloud("sqlserver", "gcp","bigquery")
# # # incremental_load_sqlserver_to_s3("sqlserver", "aws","snowflake")
incremental_load_src_to_cloud("sqlserver", "gcp","bigquery")


#
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .appName("test") \
#     .master("local[*]") \
#     .getOrCreate()
#
# print("Spark started")
# spark.stop()
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .appName("ReadParquet") \
#     .getOrCreate()
#
# df = spark.read.parquet(r"C:\Users\Admin\Downloads\data_bikestores_production_OrderDetails_incremental_20260403_145311_batch_0_part-00001-d2f9e777-e02e-465b-9249-bb549630c8ac-c000.snappy.parquet")
# # Convert to pandas
# pdf = df.toPandas()
#
# # Write CSV
# pdf.to_csv(r"D:\sec_incr.csv", index=False)
#
# df.show()
# df.printSchema()



# def spark_conn(sqlserver_jar: str, cfg: dict, cloud_type: str) -> SparkSession:
#     """
#     Builds and returns a local SparkSession configured for the specified cloud provider.
#     Supports AWS (S3A) and GCP (GCS) storage backends.
#
#     Params:
#         sqlserver_jar (str) : Path to the SQL Server JDBC JAR file.
#         cfg           (dict): Parsed pipeline configuration dictionary.
#         cloud_type    (str) : Cloud provider — 'aws' or 'gcp'.
#
#     Returns:
#         SparkSession: Configured and active SparkSession.
#
#     Raises:
#         ValueError  : If an unsupported cloud_type is provided.
#         Exception   : If SparkSession creation fails.
#     """
#     logger.info(f"Building SparkSession | cloud='{cloud_type}'")
#
#     try:
#         builder = (
#             SparkSession.builder
#             .master("local[*]")
#             .appName("Unified_Incremental_Load")
#             .config("spark.driver.memory", "4g")
#         )
#
#         # Combine required JARs — always include SQL Server; add GCS connector for GCP
#         jars = [sqlserver_jar]
#         if cloud_type == "gcp":
#             jars.append(cfg["spark"]["gcs_connector_jar_file_path"])
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
#                 .config("spark.hadoop.google.cloud.auth.service.account.enable",          "true")
#                 .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",    cfg["gcp"]["gcp_service_account_key_path"])
#                 .config("spark.hadoop.fs.gs.impl",                                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#                 .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",                     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
#             )
#         elif cloud_type == "azure":
#
#             account = cfg["azure"]["azure_storage_account_name"]
#             key = cfg["azure"]["azure_storage_account_key"]
#
#             builder = builder \
#                 .config(
#                 "spark.jars.packages",
#                 "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6"
#             ) \
#                 .config(
#                 f"spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net",
#                 key
#             )
#
#
#         else:
#             raise ValueError(f"Unsupported cloud_type '{cloud_type}'. Expected 'aws' or 'gcp'.")
#
#         spark = builder.getOrCreate()
#         logger.info("SparkSession created successfully.")
#         return spark
#
#     except ValueError:
#         raise
#     except Exception as err:
#         logger.exception(f"Failed to create SparkSession: {err}")
#         raise Exception(f"SparkSession creation failed: {err}") from err
#

# def query_execution(
#     spark: SparkSession,
#     src_db_type: str,
#     query: str,
#     config_path: str,
#     configfile: str
# ) -> DataFrame:
#     """
#     Executes a SQL query against a source database via Spark JDBC and returns
#     the result as a Spark DataFrame. Attempts a secure TLS connection first,
#     falling back to an unencrypted connection on TLS handshake failures.
#
#     Params:
#         spark       (SparkSession): Active SparkSession.
#         src_db_type (str)         : Source DB type — currently supports 'sqlserver'.
#         query       (str)         : SQL query to execute.
#         config_path (str)         : Path to the configuration directory.
#         configfile  (str)         : Name of the configuration file.
#
#     Returns:
#         DataFrame: Spark DataFrame containing the query results.
#
#     Raises:
#         ValueError       : If an unsupported src_db_type is provided.
#         AnalysisException: If Spark cannot analyse or execute the query.
#         Exception        : For any other unexpected failure during execution.
#     """
#     logger.info(f"Executing query | source = '{src_db_type}'")
#     try:
#         parsed_config = parse_config(config_path, configfile=configfile, src_db_type=src_db_type)
#
#         if src_db_type.lower() == "sqlserver":
#             try:
#                 server   = parsed_config[src_db_type.lower()]['server_name']
#                 user     = parsed_config[src_db_type.lower()]['user']
#                 password = parsed_config[src_db_type.lower()]['password']
#                 database = parsed_config[src_db_type.lower()]['database_name']
#             except KeyError as key_err:
#                 logger.error(f"Missing SQL Server config key: {key_err}. Check [{src_db_type}] section in '{configfile}'.")
#                 raise
#
#             base_url = f"jdbc:sqlserver://{server}:1433;databaseName={database}"
#             jdbc_options = {
#                 "user":     user,
#                 "password": password,
#                 "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
#             }
#
#             try:
#                 # Attempt secure TLS connection first
#                 logger.info("Attempting SQL Server connection with TLS enabled.")
#                 jdbc_options["url"] = (
#                     f"{base_url};encrypt=true;"
#                     "trustServerCertificate=true;"
#                     "sslProtocol=TLSv1.2;"
#                 )
#                 result = (
#                     spark.read.format("jdbc")
#                     .options(**jdbc_options)
#                     .option("query", query)
#                     .load()
#                 )
#                 logger.info("SQL Server TLS connection successful.")
#
#             except Exception as tls_err:
#                 error_msg = str(tls_err)
#                 if "TLS10" in error_msg or "SSLHandshakeException" in error_msg:
#                     # Fallback to unencrypted if TLS version mismatch is detected
#                     logger.warning("TLS handshake failed. Retrying with encrypt=false.")
#                     jdbc_options["url"] = f"{base_url};encrypt=false;trustServerCertificate=true;"
#                     result = (
#                         spark.read.format("jdbc")
#                         .options(**jdbc_options)
#                         .option("query", query)
#                         .load()
#                     )
#                     logger.info("SQL Server fallback (non-TLS) connection successful.")
#                 else:
#                     logger.exception(f"SQL Server JDBC connection failed: {tls_err}")
#                     raise
#
#         else:
#             logger.error(f"Unsupported source database type: '{src_db_type}'")
#             raise ValueError(f"Unsupported source database: '{src_db_type}'")
#
#         logger.info("Query executed successfully.")
#         return result
#
#     except (ValueError, AnalysisException):
#         raise
#     except Exception as err:
#         logger.exception(f"Error executing query on '{src_db_type}': {err}")
#         raise Exception(f"Query execution failed: {err}") from err
#

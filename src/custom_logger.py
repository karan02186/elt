import os
import io
import sys
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import DataFrame # type: ignore[import]

import logging
from logging.handlers import RotatingFileHandler

# Set up the global logger
# logger = logging.getLogger(__name__)
logger = logging.getLogger("data_accelerator")

CURRENT_LOG_FILE = None

# def configure_logging(log_days: int) -> logging.Logger:
#     """
#     Configures logging for the migration process.

#     Params:
#         log_days (int): Number of days to retain log files.

#     Returns:
#         logging.Logger: A configured logger instance.
#     """
#     from src.full_load import project_path

#     global CURRENT_LOG_FILE


#     # Generate a timestamp for unique file names
#     timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

#     log_dir = f"{project_path}/log"
#     log_file = f"{log_dir}/log_{timestamp}.log"
#     CURRENT_LOG_FILE = log_file

#     # Create the log directory if it doesn't exist
#     if not os.path.exists(log_dir):
#         os.makedirs(log_dir)

#     # Remove old log files beyond the specified retention period
#     retention_threshold = datetime.now() - timedelta(days=log_days)

#     for file_name in os.listdir(log_dir):
#         file_path = os.path.join(log_dir, file_name)

#         # Ensure it's a log file
#         if file_name.startswith("log_") and file_name.endswith(".log"):
#             # Extract timestamp from the filename
#             try:
#                 file_timestamp = datetime.strptime(file_name[4:-4], "%Y-%m-%d_%H-%M-%S")
#                 # Delete file if it's older than the retention period
#                 if file_timestamp < retention_threshold:
#                     os.remove(file_path)
#             except ValueError:
#                 continue  # Skip files that don't match expected format

#     if not os.path.isfile(log_file):
#         open(log_file, 'w').close()

#     # Create a logger with a unique name for your application
#     logger = logging.getLogger("data_accelerator")
#     logger.setLevel(logging.INFO)

#     # Ensure no duplicate handlers
#     if not logger.handlers:
#         log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

#         log_handler = RotatingFileHandler(log_file, maxBytes=1048576, backupCount=5)
#         log_handler.setFormatter(log_formatter)

#         logger.addHandler(log_handler)

#     # Disable logging from third-party libraries
#     logging.getLogger("aws").setLevel(logging.CRITICAL)
#     logging.getLogger("azure").setLevel(logging.CRITICAL)
#     logging.getLogger("gcp").setLevel(logging.CRITICAL)
#     logging.getLogger("snowflake").setLevel(logging.CRITICAL)
#     logging.getLogger("urllib3").setLevel(logging.CRITICAL)

#     return logger

import logging
import os
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

CURRENT_LOG_FILE = None  # keep this in case you ever use it elsewhere

def configure_logging(
    log_days: int,
    pipeline_id: str | None = None,
    src: str | None = None,
    cloud: str | None = None,
    target: str | None = None,
) -> logging.Logger:
    """
    Configure logging.

    If pipeline_id + src + cloud + target are provided, we log to:
      log_<src>-<cloud>-<target>_<pipeline_id>.log

    Otherwise we fall back to a timestamp-based global log file.
    """

    from elt.src.constants import project_path  # you already have this import pattern

    global CURRENT_LOG_FILE

    log_dir = os.path.join(project_path, "log")
    os.makedirs(log_dir, exist_ok=True)

    # ---------- choose filename ----------
    if pipeline_id and src and cloud and target:
        base_name = f"log_{src}-{cloud}-{target}_{pipeline_id}.log"
    elif pipeline_id:
        # fallback if only id is known
        base_name = f"log_{pipeline_id}.log"
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"log_{ts}.log"

    log_file = os.path.join(log_dir, base_name)
    CURRENT_LOG_FILE = log_file

    # ---------- retention: delete old log_* files ----------
    cutoff = datetime.now() - timedelta(days=log_days)
    for fname in os.listdir(log_dir):
        if not fname.startswith("log_"):
            continue
        fpath = os.path.join(log_dir, fname)
        if not os.path.isfile(fpath):
            continue
        mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
        if mtime < cutoff:
            try:
                os.remove(fpath)
            except OSError:
                pass

    # ---------- per-pipeline logger ----------
    logger_name = f"data_accelerator.{pipeline_id}" if pipeline_id else "data_accelerator"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # avoid duplicate handlers if called multiple times
    # logger.handlers = []

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.propagate = False

    return logger


def log_dataframe(df, max_rows: int = 10) -> None:
    """
    Logs the contents of either a Pandas or Spark DataFrame to the specified logger.

    Params:
        df: The DataFrame (Pandas or Spark) to log.
        max_rows (int, optional): The maximum number of rows to log. Defaults to 10.

    Returns:
        None
    """
    try:
        # If it's a Pandas DataFrame
        if isinstance(df, pd.DataFrame):
            df_str = df.head(max_rows).to_string()

            # Log based on the specified log level
            logger.info(f"Pandas DataFrame (first {max_rows} rows):\n{df_str}")

        # If it's a Spark DataFrame
        elif isinstance(df, DataFrame):
            # Redirecting the output of show() to a string
            buf = io.StringIO()
            sys.stdout = buf  # Redirect stdout to capture the output
            df.show(n=max_rows, truncate=True, vertical=False)
            sys.stdout = sys.__stdout__  # Reset stdout
            log_output = buf.getvalue()

            # Log the DataFrame contents as string
            logger.info(f"Spark DataFrame (first {max_rows} rows):\n{log_output}")

        else:
            logger.error("Unsupported DataFrame type")

    except Exception as e:
        logger.exception(f"Failed to log DataFrame contents: {e}")
        raise Exception(f"Failed to log DataFrame contents: {e}")
    

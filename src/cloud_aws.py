# =============================================================================
# File        : cloud_aws.py
# Project     : Data Accelerator – ELT Framework
# Description : AWS S3 utility functions — client creation, file upload,
#               and source-to-archive movement.
# =============================================================================

import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from botocore.client import BaseClient
from boto3.exceptions import S3UploadFailedError
from typing import Any as any, Tuple
from tqdm import tqdm  # type: ignore

from elt.src.parse_config import parse_config

import logging

logger = logging.getLogger("data_accelerator")


def aws_s3_client(config_path: str, configfile: str) -> Tuple[BaseClient, str]:
    """
    Creates and returns an authenticated AWS S3 client and bucket name.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        Tuple[BaseClient, str]: Authenticated S3 client and bucket name.

    Raises:
        KeyError      : If required AWS keys are missing in the config.
        BotoCoreError : For low-level AWS SDK errors.
        ClientError   : If AWS returns a service-level error.
        Exception     : For any other unexpected failure.
    """
    logger.info(f"Creating AWS S3 client | config='{configfile}'")
    try:
        parsed_config = parse_config(config_path, configfile=configfile, cloud_type='aws')

        try:
            aws_access_key_id     = parsed_config['aws']['aws_access_key_id']
            aws_secret_access_key = parsed_config['aws']['aws_secret_access_key']
            bucket_name           = parsed_config['aws']['s3_bucket_name']
            aws_region            = parsed_config['aws']['aws_region']
        except KeyError as key_err:
            logger.error(f"Missing AWS config key: {key_err}. Check the [aws] section in '{configfile}'.")
            raise

        client = boto3.client(
            service_name='s3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )

        logger.info(f"S3 client created | region='{aws_region}' | bucket='{bucket_name}'")
        return client, bucket_name

    except (BotoCoreError, ClientError) as aws_err:
        logger.exception(f"AWS error while creating S3 client: {aws_err}")
        raise Exception(f"Failed to create AWS S3 client: {aws_err}") from aws_err

    except Exception as err:
        logger.exception(f"Unexpected error while creating S3 client: {err}")
        raise


def move_src_to_archive_aws(
    folder_name: str,
    s3_folder_path: str,
    config_path: str,
    configfile: str
) -> None:
    """
    Moves Parquet, JSON, and _SUCCESS files from a source S3 prefix to an
    archive prefix within the same bucket. Each file is copied then deleted.

    Params:
        folder_name    (str): Destination archive prefix in S3.
        s3_folder_path (str): Source S3 prefix to move files from.
        config_path    (str): Path to the configuration directory.
        configfile     (str): Name of the configuration file.

    Returns:
        None

    Raises:
        ClientError : For AWS service errors during listing, copy, or delete.
        Exception   : For unexpected failures during the archive operation.
    """
    logger.info(f"Starting archive | source='{s3_folder_path}' | dest='{folder_name}'")

    s3, bucket_name = aws_s3_client(config_path, configfile)
    archive_path = folder_name

    # Verify source folder exists
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder_path)
        if 'Contents' not in response:
            logger.warning(f"Source folder '{s3_folder_path}' is empty or does not exist. Skipping.")
            return
    except ClientError as err:
        logger.exception(f"Failed to list objects in '{s3_folder_path}': {err}")
        raise Exception(f"Error listing S3 objects: {err}") from err

    # Ensure archive folder exists; create if not
    try:
        s3.head_object(Bucket=bucket_name, Key=archive_path)
        logger.info(f"Archive folder '{archive_path}' already exists.")
    except ClientError as err:
        if err.response['Error']['Code'] == '404':
            try:
                s3.put_object(Bucket=bucket_name, Key=archive_path)
                logger.info(f"Archive folder '{archive_path}' created.")
            except Exception as create_err:
                logger.exception(f"Failed to create archive folder: {create_err}")
                raise
        else:
            logger.exception(f"Error checking archive folder: {err}")
            raise

    # Filter eligible files — only Parquet, JSON and _SUCCESS are moved
    files_to_move = [
        obj['Key'] for obj in response.get('Contents', [])
        if obj['Key'].endswith((".parquet", ".json", "_SUCCESS"))
    ]

    if not files_to_move:
        logger.warning(f"No eligible files found in '{s3_folder_path}'. Nothing to archive.")
        return

    logger.info(f"Archiving {len(files_to_move)} file(s) from '{s3_folder_path}'.")
    successfully_moved = 0

    with tqdm(total=len(files_to_move), desc="Archiving files to S3", unit="file") as pbar:
        for key in files_to_move:
            # Preserve relative path structure under the archive prefix
            destination_key = f"{archive_path}/{key[len(s3_folder_path):].lstrip('/')}"
            try:
                s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': key}, Bucket=bucket_name, Key=destination_key)
                s3.delete_object(Bucket=bucket_name, Key=key)
                logger.info(f"Archived: '{key}' → '{destination_key}'")
                successfully_moved += 1
                pbar.update(1)
            except ClientError as err:
                logger.exception(f"Failed to move '{key}': {err}")
                raise Exception(f"Archive failed for '{key}': {err}") from err
            except Exception as err:
                logger.exception(f"Unexpected error moving '{key}': {err}")
                raise

    if successfully_moved == len(files_to_move):
        logger.info(f"Archive complete: {successfully_moved}/{len(files_to_move)} file(s) moved.")
    else:
        logger.warning(f"Archive partial: {successfully_moved}/{len(files_to_move)} file(s) moved. Check logs.")


def upload_to_s3(
    local_folder: str,
    s3_folder_path: str,
    s3_file_name: any,
    config_path: str,
    configfile: str
) -> None:
    """
    Uploads Parquet and JSON files from a local directory to an S3 bucket path.
    Parquet files use the provided `s3_file_name`; JSON files keep their original names.

    Params:
        local_folder   (str): Path to the local directory containing files.
        s3_folder_path (str): Target S3 prefix where files will be uploaded.
        s3_file_name   (any): Target filename for Parquet files in S3.
        config_path    (str): Path to the configuration directory.
        configfile     (str): Name of the configuration file.

    Returns:
        None

    Raises:
        FileNotFoundError   : If the local folder or a file does not exist.
        NoCredentialsError  : If AWS credentials are missing — aborts upload.
        S3UploadFailedError : If the S3 upload fails for a specific file.
        KeyError            : If required AWS config keys are missing.
        Exception           : For any other unexpected per-file failure.
    """
    logger.info(f"Starting S3 upload | folder='{local_folder}' | s3_path='{s3_folder_path}'")

    # Parse config and set credentials as environment variables for boto3
    try:
        parsed_config = parse_config(config_path, configfile=configfile, src_db_type=None, cloud_type='aws')
        aws_access_key_id     = parsed_config['aws']['aws_access_key_id']
        aws_secret_access_key = parsed_config['aws']['aws_secret_access_key']
        bucket_name           = parsed_config['aws']['s3_bucket_name']
    except KeyError as key_err:
        logger.error(f"Missing AWS config key: {key_err}. Check [aws] section in '{configfile}'.")
        raise
    except Exception as cfg_err:
        logger.exception(f"Failed to parse config for S3 upload: {cfg_err}")
        raise

    os.environ['AWS_ACCESS_KEY_ID']     = aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    s3 = boto3.client('s3')

    # Discover eligible files in local folder
    try:
        files_to_upload = [f for f in os.listdir(local_folder) if f.endswith((".parquet", ".json"))]
    except FileNotFoundError:
        logger.error(f"Local folder '{local_folder}' not found.")
        raise
    except PermissionError as err:
        logger.error(f"Permission denied reading '{local_folder}': {err}")
        raise

    total_files    = len(files_to_upload)
    uploaded_count = 0

    if total_files == 0:
        logger.warning(f"No Parquet or JSON files found in '{local_folder}'. Upload skipped.")
        return

    logger.info(f"Found {total_files} file(s) to upload.")

    with tqdm(total=total_files, desc="Uploading files to S3", unit="file") as pbar:
        for file in files_to_upload:
            local_file_path = os.path.join(local_folder, file)
            # Parquet files use the caller-specified name; JSON files keep their original name
            s3_file_path = (
                f"{s3_folder_path.rstrip('/')}/{s3_file_name}"
                if file.endswith(".parquet")
                else f"{s3_folder_path.rstrip('/')}/{file}"
            )

            try:
                s3.upload_file(local_file_path, bucket_name, s3_file_path)
                logger.info(f"Uploaded '{file}' → s3://{bucket_name}/{s3_file_path}")
                uploaded_count += 1
                pbar.update(1)

            except FileNotFoundError:
                logger.error(f"File '{file}' not found during upload. Skipping.")
            except NoCredentialsError:
                logger.critical("AWS credentials unavailable. Aborting upload.")
                return
            except S3UploadFailedError as err:
                logger.error(f"S3 upload failed for '{file}': {err}")
            except ClientError as err:
                logger.exception(f"AWS ClientError uploading '{file}': {err}")
            except Exception as err:
                logger.exception(f"Unexpected error uploading '{file}': {err}")

    if uploaded_count == total_files:
        logger.info(f"Upload complete: {uploaded_count}/{total_files} file(s) → s3://{bucket_name}/{s3_folder_path}")
    else:
        logger.warning(f"Upload partial: {uploaded_count}/{total_files} file(s) uploaded. Check logs for errors.")
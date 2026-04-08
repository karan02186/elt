# =============================================================================
# File        : cloud_gcp.py
# Project     : Data Accelerator – ELT Framework
# Description : GCP Cloud Storage utility functions — client creation,
#               file upload, and source-to-archive movement.
# =============================================================================

import os
from datetime import datetime
from typing import Any as any, Tuple
from tqdm import tqdm  # type: ignore

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound

from elt.src.parse_config import parse_config

import logging

logger = logging.getLogger("data_accelerator")


def gcp_storage_client(config_path: str, configfile: str) -> Tuple[storage.Client, str]:
    """
    Creates and returns an authenticated GCP Cloud Storage client and bucket name.

    Params:
        config_path (str): Path to the configuration directory.
        configfile  (str): Name of the configuration file.

    Returns:
        Tuple[storage.Client, str]: Authenticated GCP storage client and bucket name.

    Raises:
        KeyError        : If required GCP keys are missing in the config.
        GoogleCloudError: For GCP service-level errors during client creation.
        Exception       : For any other unexpected failure.
    """
    logger.info(f"Creating GCP Storage client | config='{configfile}'")
    try:
        parsed_config = parse_config(config_path, configfile=configfile, cloud_type='gcp')

        try:
            gcp_project_id              = parsed_config['gcp']['gcp_project_id']
            gcp_service_account_key_path = parsed_config['gcp']['gcp_service_account_key_path']
            bucket_name                 = parsed_config['gcp']['gcp_bucket_name']
        except KeyError as key_err:
            logger.error(f"Missing GCP config key: {key_err}. Check the [gcp] section in '{configfile}'.")
            raise

        client = storage.Client.from_service_account_json(
            gcp_service_account_key_path, project=gcp_project_id
        )

        logger.info(f"GCP Storage client created | project='{gcp_project_id}' | bucket='{bucket_name}'")
        return client, bucket_name

    except GoogleCloudError as gcp_err:
        logger.exception(f"GCP error while creating Storage client: {gcp_err}")
        raise Exception(f"Failed to create GCP Storage client: {gcp_err}") from gcp_err

    except Exception as err:
        logger.exception(f"Unexpected error while creating GCP Storage client: {err}")
        raise


def move_src_to_archive_gcp(
    folder_name: str,
    gcs_folder_path: str,
    config_path: str,
    configfile: str
) -> None:
    """
    Moves Parquet and JSON files from a source GCS prefix to a timestamped
    archive prefix within the same bucket. Each blob is copied then deleted.

    Params:
        folder_name    (str): Archive base prefix in GCS.
        gcs_folder_path(str): Source GCS prefix to move files from.
        config_path    (str): Path to the configuration directory.
        configfile     (str): Name of the configuration file.

    Returns:
        None

    Raises:
        GoogleCloudError: For GCP service errors during blob listing or copy.
        Exception       : For unexpected failures during the archive operation.
    """
    logger.info(f"Starting GCS archive | source='{gcs_folder_path}' | dest='{folder_name}'")

    storage_client, bucket_name = gcp_storage_client(config_path, configfile)
    gcp_bucket = storage_client.bucket(bucket_name)

    # Append a timestamp to the archive path to prevent overwriting previous runs
    timestamp    = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_path = f"{folder_name}/{gcs_folder_path}_{timestamp}/"

    # List and filter eligible blobs from the source prefix
    try:
        blobs         = list(storage_client.list_blobs(bucket_name, prefix=gcs_folder_path))
        files_to_move = [b for b in blobs if b.name.endswith((".parquet", ".json"))]

        if not files_to_move:
            logger.warning(f"No eligible files found in '{gcs_folder_path}'. Skipping archive.")
            return

        logger.info(f"Archiving {len(files_to_move)} file(s) to '{archive_path}'.")

    except GoogleCloudError as err:
        logger.exception(f"GCP error while listing blobs in '{gcs_folder_path}': {err}")
        raise Exception(f"Failed to list GCS blobs: {err}") from err
    except Exception as err:
        logger.exception(f"Unexpected error listing blobs in '{gcs_folder_path}': {err}")
        raise

    successfully_moved = 0

    with tqdm(total=len(files_to_move), desc="Archiving files to GCS", unit="file") as pbar:
        for blob in files_to_move:
            destination_name = f"{archive_path}{blob.name[len(gcs_folder_path) + 1:]}"
            destination_blob = gcp_bucket.blob(destination_name)
            try:
                # Copy blob to archive location
                destination_blob.rewrite(blob)
                logger.info(f"Copied '{blob.name}' → '{destination_name}'")

                # Delete original only after successful copy
                try:
                    blob.delete()
                    logger.info(f"Deleted source blob '{blob.name}'.")
                except NotFound:
                    logger.warning(f"Blob '{blob.name}' already deleted or not found during cleanup.")
                except Exception as del_err:
                    logger.warning(f"Failed to delete '{blob.name}' after copy: {del_err}")

                successfully_moved += 1
                pbar.update(1)

            except GoogleCloudError as err:
                logger.exception(f"GCP error moving '{blob.name}': {err}")
                raise Exception(f"Archive failed for '{blob.name}': {err}") from err
            except Exception as err:
                logger.exception(f"Unexpected error moving '{blob.name}': {err}")
                raise

    if successfully_moved == len(files_to_move):
        logger.info(f"Archive complete: {successfully_moved}/{len(files_to_move)} file(s) moved.")
    else:
        logger.warning(f"Archive partial: {successfully_moved}/{len(files_to_move)} file(s) moved. Check logs.")


def upload_to_gcp(
    local_folder: str,
    gcp_folder_path: str,
    gcp_file_name: any,
    config_path: str,
    configfile: str
) -> None:
    """
    Uploads Parquet and JSON files from a local directory to a GCS bucket path.
    Parquet files use the provided `gcp_file_name`; JSON files keep their original names.

    Params:
        local_folder   (str): Path to the local directory containing files.
        gcp_folder_path(str): Target GCS prefix where files will be uploaded.
        gcp_file_name  (any): Target filename for Parquet files in GCS.
        config_path    (str): Path to the configuration directory.
        configfile     (str): Name of the configuration file.

    Returns:
        None

    Raises:
        FileNotFoundError: If the local folder or a specific file is not found.
        GoogleCloudError : If a GCP service error occurs during upload.
        KeyError         : If required GCP config keys are missing.
        Exception        : For any other unexpected per-file failure.
    """
    logger.info(f"Starting GCS upload | folder='{local_folder}' | gcs_path='{gcp_folder_path}'")

    storage_client, bucket_name = gcp_storage_client(config_path, configfile)
    gcp_bucket = storage_client.bucket(bucket_name)

    # Discover eligible files in the local folder
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

    # Ensure folder path ends with '/' for correct GCS blob paths
    gcp_folder_path = gcp_folder_path.rstrip("/") + "/"

    with tqdm(total=total_files, desc="Uploading files to GCS", unit="file") as pbar:
        for file in files_to_upload:
            local_file_path = os.path.join(local_folder, file)
            # Parquet files use the caller-specified name; JSON files keep their original name
            gcp_blob_path = (
                f"{gcp_folder_path}{gcp_file_name}"
                if file.endswith(".parquet")
                else f"{gcp_folder_path}{file}"
            )

            try:
                blob = gcp_bucket.blob(gcp_blob_path)
                blob.upload_from_filename(local_file_path)
                logger.info(f"Uploaded '{file}' → gs://{bucket_name}/{gcp_blob_path}")
                uploaded_count += 1
                pbar.update(1)

            except FileNotFoundError:
                logger.error(f"File '{file}' not found at '{local_file_path}'. Skipping.")
            except GoogleCloudError as err:
                logger.exception(f"GCP error uploading '{file}': {err}")
            except Exception as err:
                logger.exception(f"Unexpected error uploading '{file}': {err}")

    if uploaded_count == total_files:
        logger.info(f"Upload complete: {uploaded_count}/{total_files} file(s) → gs://{bucket_name}/{gcp_folder_path}")
    else:
        logger.warning(f"Upload partial: {uploaded_count}/{total_files} file(s) uploaded. Check logs for errors.")
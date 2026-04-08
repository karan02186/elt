import os
from tqdm import tqdm # type: ignore[import]
from datetime import datetime
from azure.core.exceptions import ResourceExistsError
from elt.src.parse_config import parse_config
from azure.storage.blob import BlobServiceClient
from typing import Tuple
import logging


def azure_blob_client(config_path: str, configfile: str) -> Tuple[BlobServiceClient, str]:
    """
    Creates an Azure Blob client.

    Params:
        config_path (str): The path to the configuration file.

    Returns:
        (BlobServiceClient, str): A tuple containing the Azure Blob client and the container name.

    Raises:
        Exception: If any error occurs during the creation of the Azure Blob client.
    """
    try:
        # Parse the configuration file to get Azure Blob connection parameters
        parsed_config = parse_config(config_path, configfile=configfile, cloud_type='azure')

        # Extracting Azure Blob parameters from parsed_config dictionary
        container_name = parsed_config['azure']['azure_container_name']

        if "azure_connection_string" in parsed_config["azure"]:
            # Method 1: Use connection string
            blob_service_client = BlobServiceClient.from_connection_string(
                parsed_config["azure"]["azure_connection_string"])

        elif "azure_storage_account_name" in parsed_config["azure"] and "azure_storage_account_key" in parsed_config[
            "azure"]:
            # Method 2: Use account name & key
            account_url = f"https://{parsed_config['azure']['azure_storage_account_name']}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url,
                                                    credential=parsed_config["azure"]["azure_storage_account_key"])

        elif "azure_blob_service_url" in parsed_config["azure"] and "sas_token" in parsed_config["azure"]:
            # Method 3: Use account URL with SAS Token
            blob_service_client = BlobServiceClient(account_url=parsed_config["azure"]["azure_blob_service_url"],
                                                    credential=parsed_config["azure"]["sas_token"])

        else:
            logger.error("Missing required Azure Blob Storage authentication parameters.")
            raise ValueError("Missing required Azure Blob Storage authentication parameters.")

        return blob_service_client, container_name

    except Exception as error:
        # Raise an exception if any error occurs
        logger.exception(f"Error creating Azure Blob client: {error}")
        raise Exception(f"Error creating Azure Blob client: {error}")


# Set up the global logger
# logger = logging.getLogger(__name__)
logger = logging.getLogger("data_accelerator")


def move_src_to_archive_azure(folder_name: str, blob_folder_path: str, config_path: str,configfile :str) -> None:
    """
    Move source files from a specified Azure Blob container to an archive folder within the same container with progress
    indication.

    The function will copy JSON and Parquet files from the source folder to the archive folder
    and then delete the original files from the source folder.

    Params:
       folder_name (str): The base folder name for the archive.
       blob_folder_path (str): The blob folder path where the source files are located.
       config_path (str): The path to the configuration file.

    Returns:
       None
    """
    # Get the Azure Blob client and container name using azure_blob_client
    blob_service_client, container_name = azure_blob_client(config_path,configfile=configfile)
    container_client = blob_service_client.get_container_client(container_name)

    # Generate a timestamp to use in the archive folder path
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_path = f"{folder_name}/{blob_folder_path}_{timestamp}/"

    # Check if the source folder exists by listing its contents
    try:
        blob_list = container_client.list_blobs(name_starts_with=blob_folder_path)
        blobs = [blob.name for blob in blob_list if blob.name.endswith(".parquet") or blob.name.endswith(".json")]
        if not blobs:
            logger.warning(f"No JSON or Parquet files found in '{blob_folder_path}' to move.")
            return
        logger.info(f"Found {len(blobs)} files to move from '{blob_folder_path}' to '{archive_path}'")

    except Exception as e:
        logger.exception(f"Error while listing blobs: {e}")
        raise Exception(f"Error while listing blobs: {e}")

    # Ensure the archive folder exists or create it if it doesn't
    try:
        # Create a "dummy" blob to represent the folder (Azure doesn't have real folders, just blob prefixes)
        container_client.get_blob_client(f"{archive_path}/.placeholder").upload_blob("", overwrite=True)
        logger.info(f"Archive folder '{archive_path}' created in Azure Blob Storage.")
    except Exception as e:
        logger.exception(f"Failed to create archive folder: {e}")
        raise Exception(f"Failed to create archive folder: {e}")

    # Initialize the progress bar
    all_move_successfully = False
    with tqdm(total=len(blobs), desc="Moving files to archive", unit="file") as pbar:
        for blob_name in blobs:
            source_blob = container_client.get_blob_client(blob_name)
            destination_blob_name = f"{archive_path}{blob_name[len(blob_folder_path) + 1:]}"
            destination_blob = container_client.get_blob_client(destination_blob_name)
            try:
                # Copy the blob from the source folder to the archive folder
                destination_blob.start_copy_from_url(source_blob.url)
                logger.info(f"Copied '{blob_name}' to archive location '{destination_blob_name}'")

                # Delete original blob after successful copy
                try:
                    source_blob.delete_blob()
                    logger.info(f"Deleted original file '{blob_name}' after archiving.")
                except Exception as e:
                    logger.warning(f"Failed to delete '{blob_name}' after copying: {e}.")

                pbar.update(1)  # Update the progress bar
                all_move_successfully = True
            except Exception as e:
                logger.exception(f"Error moving '{blob_name}': {e}")
                raise Exception(f"Error moving '{blob_name}': {e}")

    # Display the success message only if all files moved successfully
    if all_move_successfully:
        logger.info(
            f"All files from '{blob_folder_path}' successfully moved to archive folder '{archive_path}' and deleted."
        )
    else:
        logger.warning("Some files may have failed to move. Check logs for details.")


def upload_to_azure_blob(local_folder: str, azure_folder_path: str, azure_file_name: any, config_path: str,configfile:str) -> None:
    """
    Upload files from a local folder to an Azure Blob Storage container with a progress bar.

    The function uploads `.parquet` and `.json` files from the specified local folder to the given Azure folder path.
    The `.parquet` files are uploaded with a specified name, while `.json` files retain their local file names.

    Params:
        local_folder (str): Path to the local folder containing the files to upload.
        azure_folder_path (str): Azure folder path where the files will be uploaded.
        azure_file_name (str): The name to use for `.parquet` files in Azure Blob Storage.
        config_path (str): The path to the configuration file.

    Returns:
        None
    """
    # Get the Azure Blob client and container name
    blob_service_client, container_name = azure_blob_client(config_path,configfile=configfile)
    container_client = blob_service_client.get_container_client(container_name)

    # Get a list of files to upload
    files_to_upload = [file for file in os.listdir(local_folder) if file.endswith((".parquet", ".json"))]
    total_files = len(files_to_upload)  # Total files to upload
    uploaded_count = 0  # Track the number of successful uploads

    if total_files == 0:
        logger.warning(f"No `.parquet` or `.json` files found in local folder: {local_folder}")
        return

    # Ensure azure_folder_path ends with `/` for correct blob paths
    azure_folder_path = azure_folder_path.rstrip("/") + "/"

    # Initialize the progress bar
    with tqdm(total=total_files, desc="Uploading files to Azure Blob Storage", unit="file") as pbar:
        for file in files_to_upload:
            local_file_path = os.path.join(local_folder, file)
            azure_blob_path = f"{azure_folder_path}{azure_file_name}" if file.endswith(
                ".parquet") else f"{azure_folder_path}{file}"

            try:
                # Upload the file to Azure Blob Storage
                blob_client = container_client.get_blob_client(azure_blob_path)
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

                logger.info(f"Successfully uploaded '{file}' to Azure path '{azure_blob_path}'")
                uploaded_count += 1
                pbar.update(1)  # Update the progress bar
            except FileNotFoundError:
                logger.error(f"File '{file}' not found in '{local_folder}'")
            except ResourceExistsError:
                logger.warning(f"File '{file}' already exists in Azure Blob Storage: {azure_blob_path}")
            except Exception as e:
                logger.exception(f"Unexpected error while uploading '{file}': {e}")

    # Log final status
    if uploaded_count == total_files:
        logger.info(f"All {total_files} files successfully uploaded to Azure Blob Storage: {azure_folder_path}")
    else:
        logger.warning(f"Upload completed with issues: {uploaded_count}/{total_files} files uploaded successfully.")

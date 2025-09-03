import os
from metadata.utils.logger import ingestion_logger
logger = ingestion_logger()

def ensure_directory_exists(directory_path: str):
    """
    Check if a directory exists and create it if it doesn't.

    :param directory_path: Path to the directory to check/create.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.debug(f"Directory created: {directory_path}")
    else:
        logger.debug(f"Directory already exists: {directory_path}")

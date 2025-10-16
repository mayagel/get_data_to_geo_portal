"""
File system scanning and archive extraction utilities
"""

import os
import zipfile
import py7zr
import rarfile
from typing import Optional, List, Tuple
from pathlib import Path
import logging

logger = logging.getLogger("GISIngestion.scanner")


def scan_root_directory(root_path: str, folder_prefix: str) -> List[str]:
    """
    Scan root directory for folders starting with specified prefix
    
    Args:
        root_path: Root directory to scan
        folder_prefix: Prefix to match (e.g., "A-")
        
    Returns:
        List of matching folder paths
    """
    matching_folders = []
    
    try:
        if not os.path.exists(root_path):
            logger.error(f"Root path does not exist: {root_path}")
            return matching_folders
        
        for item in os.listdir(root_path):
            item_path = os.path.join(root_path, item)
            
            if os.path.isdir(item_path) and item.startswith(folder_prefix):
                matching_folders.append(item_path)
                logger.debug(f"Found matching folder: {item_path}")
        
        logger.info(f"Found {len(matching_folders)} folders starting with '{folder_prefix}'")
        
    except Exception as e:
        logger.error(f"Error scanning root directory: {e}")
    
    return matching_folders


def find_gis_resources(folder_path: str) -> Tuple[Optional[str], Optional[str], List[str]]:
    """
    Find GIS resources in a folder (GIS subfolder, compressed files, GDB)
    
    Args:
        folder_path: Path to folder to scan
        
    Returns:
        Tuple of (gis_folder_path, gdb_path, compressed_files)
    """
    gis_folder = None
    gdb_path = None
    compressed_files = []
    
    try:
        items = os.listdir(folder_path)
        
        for item in items:
            item_path = os.path.join(folder_path, item)
            item_lower = item.lower()
            
            # Check for GIS folder
            if os.path.isdir(item_path) and item_lower == 'gis':
                gis_folder = item_path
                logger.debug(f"Found GIS folder: {item_path}")
            
            # Check for GDB
            elif os.path.isdir(item_path) and item_lower.endswith('.gdb'):
                gdb_path = item_path
                logger.debug(f"Found GDB: {item_path}")
            
            # Check for compressed files
            elif os.path.isfile(item_path):
                if item_lower.endswith(('.7z', '.zip', '.rar')):
                    compressed_files.append(item_path)
                    logger.debug(f"Found compressed file: {item_path}")
        
    except Exception as e:
        logger.error(f"Error scanning folder '{folder_path}': {e}")
    
    return gis_folder, gdb_path, compressed_files


def extract_archive(archive_path: str, extract_to: Optional[str] = None) -> bool:
    """
    Extract compressed archive (7z, zip, or rar)
    
    Args:
        archive_path: Path to archive file
        extract_to: Destination directory (defaults to archive's directory)
        
    Returns:
        True if successful, False otherwise
    """
    if extract_to is None:
        extract_to = os.path.dirname(archive_path)
    
    archive_lower = archive_path.lower()
    
    try:
        # Check if extraction folder already exists
        archive_name = os.path.splitext(os.path.basename(archive_path))[0]
        expected_folder = os.path.join(extract_to, archive_name)
        
        if os.path.exists(expected_folder):
            logger.info(f"Archive already extracted: {expected_folder}")
            return True
        
        # Extract based on file type
        if archive_lower.endswith('.zip'):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            logger.info(f"Successfully extracted ZIP: {archive_path}")
            return True
            
        elif archive_lower.endswith('.7z'):
            with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                archive.extractall(path=extract_to)
            logger.info(f"Successfully extracted 7Z: {archive_path}")
            return True
            
        elif archive_lower.endswith('.rar'):
            with rarfile.RarFile(archive_path) as rar_ref:
                rar_ref.extractall(extract_to)
            logger.info(f"Successfully extracted RAR: {archive_path}")
            return True
            
        else:
            logger.warning(f"Unsupported archive format: {archive_path}")
            return False
            
    except Exception as e:
        logger.error(f"Error extracting archive '{archive_path}': {e}")
        return False


def get_source_directory_name(folder_path: str, folder_prefix: str) -> str:
    """
    Extract the source directory name (up to and including the A- prefix)
    
    Args:
        folder_path: Full path to folder
        folder_prefix: Prefix to match (e.g., "A-")
        
    Returns:
        Source directory path up to the A- folder
    """
    parts = Path(folder_path).parts
    
    for i, part in enumerate(parts):
        if part.startswith(folder_prefix):
            # Return path up to and including this folder
            return str(Path(*parts[:i+1]))
    
    return folder_path

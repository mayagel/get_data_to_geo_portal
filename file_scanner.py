"""
File system scanning and archive extraction utilities
"""

import os
import zipfile
import py7zr
import rarfile
import subprocess
from typing import Optional, List, Tuple
from pathlib import Path
import logging
import shutil

logger = logging.getLogger("GISIngestion.scanner")

# Find extraction tools
SEVEN_ZIP_PATH = None
UNRAR_PATH = None

# Check for 7-Zip
SEVEN_ZIP_LOCATIONS = [
    r"C:\Program Files\7-Zip\7z.exe",
    r"C:\Program Files (x86)\7-Zip\7z.exe",
]

for path in SEVEN_ZIP_LOCATIONS:
    if os.path.exists(path):
        SEVEN_ZIP_PATH = path
        logger.debug(f"Found 7-Zip: {path}")
        break

if not SEVEN_ZIP_PATH:
    seven_zip_in_path = shutil.which("7z")
    if seven_zip_in_path:
        SEVEN_ZIP_PATH = seven_zip_in_path
        logger.debug(f"Found 7-Zip in PATH: {seven_zip_in_path}")

# Check for UnRAR
UNRAR_LOCATIONS = [
    r"C:\Program Files\WinRAR\UnRAR.exe",
    r"C:\Program Files (x86)\WinRAR\UnRAR.exe",
]

for path in UNRAR_LOCATIONS:
    if os.path.exists(path):
        UNRAR_PATH = path
        rarfile.UNRAR_TOOL = path
        logger.debug(f"Found UnRAR: {path}")
        break

if not UNRAR_PATH:
    unrar_in_path = shutil.which("unrar")
    if unrar_in_path:
        UNRAR_PATH = unrar_in_path
        rarfile.UNRAR_TOOL = unrar_in_path
        logger.debug(f"Found UnRAR in PATH: {unrar_in_path}")

# Determine which tool to use for RAR extraction
if UNRAR_PATH:
    logger.debug(f"Will use UnRAR for RAR extraction: {UNRAR_PATH}")
elif SEVEN_ZIP_PATH:
    logger.debug(f"Will use 7-Zip for RAR extraction: {SEVEN_ZIP_PATH}")
else:
    logger.warning("No RAR extraction tool found. RAR files will be skipped.")


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


def _add_to_skip_list(archive_path: str, skip_file: str) -> None:
    """
    Add archive path to skip list file to prevent re-extraction
    
    Args:
        archive_path: Full path to the archive file
        skip_file: Path to the skip list file
    """
    try:
        with open(skip_file, 'a', encoding='utf-8') as f:
            f.write(f"{archive_path}\n")
        logger.debug(f"Added to skip list: {archive_path}")
    except Exception as e:
        logger.warning(f"Could not write to {skip_file}: {e}")


def extract_archive(archive_path: str, extract_to: Optional[str] = None) -> bool:
    """
    Extract compressed archive (7z, zip, or rar)
    Tracks extracted files in skip_the_extract.txt to avoid re-extraction
    
    Args:
        archive_path: Path to archive file
        extract_to: Destination directory (defaults to archive's directory)
        
    Returns:
        True if successful, False otherwise
    """
    if extract_to is None:
        extract_to = os.path.dirname(archive_path)
    
    archive_lower = archive_path.lower()
    skip_file = "skip_the_extract.txt"
    
    # Check if this archive was already extracted
    try:
        if os.path.exists(skip_file):
            with open(skip_file, 'r', encoding='utf-8') as f:
                extracted_files = set(line.strip() for line in f if line.strip())
            
            if archive_path in extracted_files:
                logger.info(f"Archive already extracted previously (found in {skip_file}): {archive_path}")
                return True
        else:
            # Create the file if it doesn't exist
            open(skip_file, 'w', encoding='utf-8').close()
            logger.debug(f"Created {skip_file} for tracking extracted archives")
    except Exception as e:
        logger.warning(f"Could not read {skip_file}: {e}. Proceeding with extraction.")
    
    try:
        # Check if extraction folder already exists
        archive_name = os.path.splitext(os.path.basename(archive_path))[0]
        expected_folder = os.path.join(extract_to, archive_name)
        
        if os.path.exists(expected_folder):
            logger.info(f"Archive already extracted: {expected_folder}")
            # Add to skip list so we don't check again next time
            _add_to_skip_list(archive_path, skip_file)
            return True
        
        # Extract based on file type
        if archive_lower.endswith('.zip'):
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            logger.info(f"Successfully extracted ZIP: {archive_path}")
            
            # Add to skip list
            _add_to_skip_list(archive_path, skip_file)
            return True
            
        elif archive_lower.endswith('.7z'):
            with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                archive.extractall(path=extract_to)
            logger.info(f"Successfully extracted 7Z: {archive_path}")
            
            # Add to skip list
            _add_to_skip_list(archive_path, skip_file)
            return True
            
        elif archive_lower.endswith('.rar'):
            # Try UnRAR first (preferred for RAR files)
            if UNRAR_PATH:
                try:
                    with rarfile.RarFile(archive_path) as rar_ref:
                        rar_ref.extractall(extract_to)
                    logger.info(f"Successfully extracted RAR using UnRAR: {archive_path}")
                    
                    # Add to skip list
                    _add_to_skip_list(archive_path, skip_file)
                    return True
                except Exception as e:
                    logger.error(f"UnRAR extraction failed: {e}")
                    # Fall through to try 7-Zip
            
            # Use 7-Zip if UnRAR is not available
            if SEVEN_ZIP_PATH:
                try:
                    # 7-Zip command: 7z x archive.rar -oOutputDir -y
                    # x = extract with full paths
                    # -o = output directory (no space after -o)
                    # -y = assume Yes on all queries
                    cmd = [SEVEN_ZIP_PATH, 'x', archive_path, f'-o{extract_to}', '-y']
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    logger.info(f"Successfully extracted RAR using 7-Zip: {archive_path}")
                    
                    # Add to skip list
                    _add_to_skip_list(archive_path, skip_file)
                    return True
                except subprocess.CalledProcessError as e:
                    logger.error(f"7-Zip extraction failed for '{archive_path}': {e.stderr}")
                    return False
                except Exception as e:
                    logger.error(f"Error extracting RAR with 7-Zip '{archive_path}': {e}")
                    return False
            
            # No extraction tool available
            logger.error(
                f"Cannot extract RAR file '{archive_path}': No RAR extraction tool found. "
                f"Please install WinRAR or 7-Zip. "
                f"Skipping this archive."
            )
            return False
            
        else:
            logger.warning(f"Unsupported archive format: {archive_path}")
            return False
            
    except Exception as e:
        logger.error(f"Error extracting archive '{archive_path}': {e}")
        return False


def get_source_directory_name(folder_path: str, folder_prefix: str) -> str:
    """
    Extract the source directory name (up to and including the folder starting with prefix)
    
    Args:
        folder_path: Full path to folder
        folder_prefix: Prefix to match (e.g., "-A")
        
    Returns:
        Full path up to and including the folder that starts with the prefix (e.g., "-A")
    """
    # Normalize path
    folder_path = os.path.normpath(folder_path)
    parts = Path(folder_path).parts
    
    # Find the folder that starts with the prefix
    for i, part in enumerate(parts):
        if part.startswith(folder_prefix):
            # Return path up to and including this folder
            result = str(Path(*parts[:i+1]))
            logger.debug(f"Extracted source directory: {result} from {folder_path}")
            return result
    
    # If no match found, return the folder path itself
    logger.warning(f"No folder with prefix '{folder_prefix}' found in path: {folder_path}")
    return folder_path

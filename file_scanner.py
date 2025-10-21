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
        break

if not SEVEN_ZIP_PATH:
    seven_zip_in_path = shutil.which("7z")
    if seven_zip_in_path:
        SEVEN_ZIP_PATH = seven_zip_in_path

# Check for UnRAR
UNRAR_LOCATIONS = [
    r"C:\Program Files\WinRAR\UnRAR.exe",
    r"C:\Program Files (x86)\WinRAR\UnRAR.exe",
]

for path in UNRAR_LOCATIONS:
    if os.path.exists(path):
        UNRAR_PATH = path
        rarfile.UNRAR_TOOL = path
        break

if not UNRAR_PATH:
    unrar_in_path = shutil.which("unrar")
    if unrar_in_path:
        UNRAR_PATH = unrar_in_path
        rarfile.UNRAR_TOOL = unrar_in_path

# Determine which tool to use for RAR extraction
if not UNRAR_PATH and not SEVEN_ZIP_PATH:
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
            
            # Check for GDB
            elif os.path.isdir(item_path) and item_lower.endswith('.gdb'):
                gdb_path = item_path
            
            # Check for compressed files
            elif os.path.isfile(item_path):
                if item_lower.endswith(('.7z', '.zip', '.rar')):
                    compressed_files.append(item_path)
        
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
    except Exception as e:
        logger.warning(f"Could not write to {skip_file}: {e}")


def get_extracted_files_dir() -> str:
    """
    Get the extraction directory, creating it if needed
    
    Returns:
        Path to extraction directory
    """
    try:
        from config import EXTRACTED_FILES_DIR
        extract_dir = EXTRACTED_FILES_DIR
    except ImportError:
        extract_dir = os.path.join(os.path.dirname(__file__), 'extracted_files')
    
    # Create directory if it doesn't exist
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
    
    return extract_dir


def get_extraction_tracker_file() -> str:
    """
    Get the extraction tracker file path
    
    Returns:
        Path to tracker file
    """
    try:
        from config import EXTRACTED_FILES_TRACKER
        return EXTRACTED_FILES_TRACKER
    except ImportError:
        return "extracted_here_files.txt"


def extract_archive(archive_path: str, extract_to: Optional[str] = None, source_directory_name: str = None) -> bool:
    """
    Extract compressed archive (7z, zip, or rar)
    Tracks extracted files in extracted_here_files.txt to avoid re-extraction
    Extracts to configured EXTRACTED_FILES_DIR/source_directory_name
    
    Args:
        archive_path: Path to archive file
        extract_to: Destination directory (defaults to EXTRACTED_FILES_DIR)
        source_directory_name: Name of source directory (for organizing extracted files)
        
    Returns:
        True if successful, False otherwise
    """
    if extract_to is None:
        extract_to = get_extracted_files_dir()
        
    # If source_directory_name is provided, create subdirectory for it
    if source_directory_name:
        extract_to = os.path.join(extract_to, source_directory_name)
        if not os.path.exists(extract_to):
            os.makedirs(extract_to)
    
    archive_lower = archive_path.lower()
    skip_file = get_extraction_tracker_file()
    
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
            return result
    
    # If no match found, return the folder path itself
    logger.warning(f"No folder with prefix '{folder_prefix}' found in path: {folder_path}")
    return folder_path


def get_extraction_user(archive_path: str) -> str:
    """
    Get the user who extracted the archive (current user)
    
    Args:
        archive_path: Path to archive file
        
    Returns:
        Username
    """
    try:
        from config import CURRENT_USER
        return CURRENT_USER
    except ImportError:
        return os.getenv('USERNAME', 'unknown')






def organize_gdbs_in_source_directory(source_directory_name: str) -> int:
    """
    After extraction, find all GDBs recursively and copy them to source directory root
    
    Args:
        source_directory_name: Name of source directory
        
    Returns:
        Number of GDBs organized
    """
    try:
        extract_dir = get_extracted_files_dir()
        source_extract_dir = os.path.join(extract_dir, source_directory_name)
        
        if not os.path.exists(source_extract_dir):
            logger.warning(f"Source extraction directory doesn't exist: {source_extract_dir}")
            return 0
        
        logger.info(f"Organizing GDB files in {source_extract_dir}")
        
        # Find all GDBs recursively in the source directory
        all_gdbs = find_all_gdbs_recursively(source_extract_dir)
        
        if not all_gdbs:
            logger.info(f"No GDB files found in {source_extract_dir}")
            return 0
        
        # Copy GDBs that are not already at root level to the root
        gdbs_at_root = []
        gdbs_to_copy = []
        
        for gdb_path in all_gdbs:
            gdb_name = os.path.basename(gdb_path)
            gdb_parent = os.path.dirname(gdb_path)
            
            if gdb_parent == source_extract_dir:
                # Already at root level
                gdbs_at_root.append(gdb_path)
            else:
                # Needs to be copied to root
                gdbs_to_copy.append((gdb_path, gdb_name))
        
        # Copy GDBs to root level
        for gdb_path, gdb_name in gdbs_to_copy:
            dest_path = os.path.join(source_extract_dir, gdb_name)
            
            # Handle name conflicts
            if os.path.exists(dest_path):
                # Add counter to make unique
                counter = 1
                base_name = gdb_name.replace('.gdb', '')
                while os.path.exists(dest_path):
                    dest_path = os.path.join(source_extract_dir, f"{base_name}_{counter}.gdb")
                    counter += 1
            
            try:
                shutil.copytree(gdb_path, dest_path)
                logger.info(f"Copied GDB to root: {gdb_name} -> {os.path.basename(dest_path)}")
                gdbs_at_root.append(dest_path)
            except Exception as e:
                logger.error(f"Could not copy {gdb_name}: {e}")
        
        logger.info(f"Organized {len(gdbs_at_root)} GDB file(s)")
        return len(gdbs_at_root)
        
    except Exception as e:
        logger.error(f"Error organizing GDBs: {e}")
        return 0




def find_all_gdbs_recursively(directory: str) -> List[str]:
    """
    Find all GDB files in a directory recursively
    
    Args:
        directory: Path to directory to search
        
    Returns:
        List of GDB paths
    """
    try:
        gdb_paths = []
        
        if not os.path.exists(directory):
            return gdb_paths
        
        # Recursively search for all .gdb directories
        for root, dirs, files in os.walk(directory):
            for dir_name in dirs:
                if dir_name.lower().endswith('.gdb'):
                    gdb_path = os.path.join(root, dir_name)
                    gdb_paths.append(gdb_path)
        
        logger.info(f"Found {len(gdb_paths)} GDB file(s) in {directory}")
        return gdb_paths
        
    except Exception as e:
        logger.error(f"Error finding GDBs: {e}")
        return []




def get_gis_resources_size_gb(directory: str, max_size_gb: float = None) -> float:
    """
    Calculate the size of only GIS-relevant resources at first level:
    - Compressed files (.zip, .7z, .rar)
    - Files ending with .gdb (not directories)
    
    Args:
        directory: Path to directory
        max_size_gb: Optional maximum size threshold. If exceeded, stops calculation
        
    Returns:
        Size in GB of only compressed files and .gdb files at first level
    """
    try:
        total_size = 0
        max_size_bytes = max_size_gb * (1024 ** 3) if max_size_gb else None
        
        # Function to check files in a single directory level
        def check_directory_level(dir_path):
            nonlocal total_size
            
            try:
                items = os.listdir(dir_path)
            except (OSError, PermissionError) as e:
                logger.error(f"Cannot access directory {dir_path}: {e}")
                return False
            
            for item in items:
                item_path = os.path.join(dir_path, item)
                
                try:
                    # Only check FILES (not directories)
                    if os.path.isfile(item_path) and not os.path.islink(item_path):
                        item_lower = item.lower()
                        
                        # Check if it's a compressed file or .gdb file
                        if item_lower.endswith(('.zip', '.7z', '.rar', '.gdb')):
                            total_size += os.path.getsize(item_path)
                            
                            # Early exit check
                            if max_size_bytes and total_size > max_size_bytes:
                                return True  # Signal to stop
                                
                except (OSError, FileNotFoundError) as e:
                    logger.warning(f"Could not access {item_path}: {e}")
                    continue
            
            return False  # Continue
        
        # Check main directory
        should_stop = check_directory_level(directory)
        if should_stop:
            size_gb = total_size / (1024 ** 3)
            return size_gb
        
        # Also check GIS subfolder if it exists
        gis_folder = os.path.join(directory, 'GIS')
        if os.path.isdir(gis_folder):
            should_stop = check_directory_level(gis_folder)
            if should_stop:
                size_gb = total_size / (1024 ** 3)
                return size_gb
        
        # Convert bytes to GB
        size_gb = total_size / (1024 ** 3)
        return size_gb
        
    except Exception as e:
        logger.error(f"Error calculating GIS resources size: {e}")
        return 0.0



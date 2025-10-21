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
        logger.info(f"Created extraction directory: {extract_dir}")
    
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
            logger.debug(f"Created extraction subdirectory: {extract_to}")
    
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


def get_extracted_gdb_path(archive_path: str) -> Optional[str]:
    """
    Get the path where GDB should be after extraction from an archive
    
    Args:
        archive_path: Path to the original archive file
        
    Returns:
        Path to extracted GDB location or None if not found
    """
    extract_dir = get_extracted_files_dir()
    archive_name = os.path.splitext(os.path.basename(archive_path))[0]
    extracted_folder = os.path.join(extract_dir, archive_name)
    
    if os.path.exists(extracted_folder):
        # Search for GDB in extracted folder
        _, gdb_path, _ = find_gis_resources(extracted_folder)
        if gdb_path:
            return gdb_path
        
        # Also check for GIS subfolder
        gis_folder = os.path.join(extracted_folder, 'GIS')
        if os.path.exists(gis_folder):
            _, gdb_path, _ = find_gis_resources(gis_folder)
            if gdb_path:
                return gdb_path
    
    return None


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


def check_if_directory_already_processed(source_dir: str) -> bool:
    """
    Check if a directory has already been processed by checking extracted_here_files.txt
    
    Args:
        source_dir: Source directory to check
        
    Returns:
        True if already processed, False otherwise
    """
    try:
        tracker_file = get_extraction_tracker_file()
        
        if not os.path.exists(tracker_file):
            return False
        
        # Normalize the source directory path
        source_dir_normalized = os.path.normpath(source_dir).lower()
        
        with open(tracker_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Check if this line's directory matches our source directory
                line_dir = os.path.dirname(os.path.normpath(line)).lower()
                if line_dir == source_dir_normalized:
                    logger.info(f"Directory already processed (found in tracker): {source_dir}")
                    return True
        
        return False
        
    except Exception as e:
        logger.warning(f"Could not check tracker file: {e}")
        return False


def copy_gdb_files_only(source_dir: str) -> int:
    """
    Copy only .gdb directories from source directory to extracted_files directory
    
    Args:
        source_dir: Source directory to copy from
        
    Returns:
        Number of GDB directories copied
    """
    try:
        extract_dir = get_extracted_files_dir()
        
        logger.info(f"Copying only GDB files from {source_dir} to {extract_dir}")
        
        copied_count = 0
        for item in os.listdir(source_dir):
            source_path = os.path.join(source_dir, item)
            
            # Only copy .gdb directories
            if os.path.isdir(source_path) and item.lower().endswith('.gdb'):
                dest_path = os.path.join(extract_dir, item)
                
                # Skip if already exists in destination
                if os.path.exists(dest_path):
                    logger.debug(f"Skipping {item}, already exists in extraction directory")
                    continue
                
                try:
                    shutil.copytree(source_path, dest_path)
                    logger.info(f"Copied GDB: {item}")
                    copied_count += 1
                except Exception as e:
                    logger.warning(f"Could not copy {item}: {e}")
        
        logger.info(f"Copied {copied_count} GDB file(s) to extraction directory")
        return copied_count
        
    except Exception as e:
        logger.error(f"Error copying GDB files: {e}")
        return 0


def organize_gdbs_in_source_directory(source_directory_name: str) -> int:
    """
    After extraction, find all GDBs recursively, copy them to source directory root,
    and delete all non-GDB files
    
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
        
        # Now delete everything except GDB folders at root level
        logger.info(f"Cleaning up non-GDB files from {source_extract_dir}")
        gdb_names_at_root = {os.path.basename(g).lower() for g in gdbs_at_root}
        
        removed_count = 0
        items_to_remove = []
        
        # First, collect all items to remove
        try:
            for item in os.listdir(source_extract_dir):
                item_path = os.path.join(source_extract_dir, item)
                
                # Keep only .gdb directories (not files ending with .gdb, only directories)
                if os.path.isdir(item_path) and item.lower().endswith('.gdb') and item.lower() in gdb_names_at_root:
                    logger.debug(f"Keeping GDB directory: {item}")
                    continue
                
                # Also keep .gitkeep files
                if item.lower().endswith('.gitkeep'):
                    continue
                
                # Everything else should be removed
                items_to_remove.append((item, item_path))
        except Exception as e:
            logger.error(f"Error listing directory {source_extract_dir}: {e}")
        
        # Now remove all non-GDB items
        for item, item_path in items_to_remove:
            try:
                if os.path.isdir(item_path):
                    logger.info(f"Removing non-GDB directory: {item}")
                    shutil.rmtree(item_path)
                else:
                    logger.info(f"Removing non-GDB file: {item}")
                    os.remove(item_path)
                removed_count += 1
            except Exception as e:
                logger.warning(f"Could not remove {item}: {e}")
        
        logger.info(f"Organized {len(gdbs_at_root)} GDB file(s), removed {removed_count} non-GDB items")
        
        # Verify cleanup - list what remains
        try:
            remaining_items = os.listdir(source_extract_dir)
            gdb_items = [item for item in remaining_items if item.lower().endswith('.gdb')]
            non_gdb_items = [item for item in remaining_items if not item.lower().endswith('.gdb') and not item.lower().endswith('.gitkeep')]
            
            if non_gdb_items:
                logger.warning(f"WARNING: {len(non_gdb_items)} non-GDB items still remain: {non_gdb_items}")
            else:
                logger.info(f"Cleanup verified: Only {len(gdb_items)} GDB directories remain")
        except Exception as e:
            logger.warning(f"Could not verify cleanup: {e}")
        
        return len(gdbs_at_root)
        
    except Exception as e:
        logger.error(f"Error organizing GDBs: {e}")
        return 0


def cleanup_extracted_files_dir() -> None:
    """
    Clean up extracted files directory by removing all empty source directories
    """
    try:
        extract_dir = get_extracted_files_dir()
        
        if not os.path.exists(extract_dir):
            logger.debug("Extraction directory doesn't exist, nothing to clean")
            return
        
        logger.info(f"Cleaning up extraction directory: {extract_dir}")
        
        removed_count = 0
        for item in os.listdir(extract_dir):
            item_path = os.path.join(extract_dir, item)
            
            # Skip .gitkeep files
            if item.lower().endswith('.gitkeep'):
                continue
            
            # Remove empty directories
            if os.path.isdir(item_path):
                try:
                    # Check if directory is empty or only contains .gitkeep
                    contents = os.listdir(item_path)
                    if not contents or (len(contents) == 1 and contents[0].lower().endswith('.gitkeep')):
                        shutil.rmtree(item_path)
                        logger.debug(f"Removed empty directory: {item}")
                        removed_count += 1
                except Exception as e:
                    logger.warning(f"Could not remove {item}: {e}")
        
        logger.info(f"Cleanup complete. Removed {removed_count} empty directories")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


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
                    logger.debug(f"Found GDB: {gdb_path}")
        
        logger.info(f"Found {len(gdb_paths)} GDB file(s) in {directory} (recursive search)")
        return gdb_paths
        
    except Exception as e:
        logger.error(f"Error finding GDBs: {e}")
        return []


def find_all_gdbs_in_extracted_dir() -> List[str]:
    """
    Find all GDB files in the extracted_files directory (recursively)
    
    Returns:
        List of GDB paths
    """
    try:
        extract_dir = get_extracted_files_dir()
        gdb_paths = []
        
        if not os.path.exists(extract_dir):
            return gdb_paths
        
        # Recursively search for all .gdb directories
        for root, dirs, files in os.walk(extract_dir):
            for dir_name in dirs:
                if dir_name.lower().endswith('.gdb'):
                    gdb_path = os.path.join(root, dir_name)
                    gdb_paths.append(gdb_path)
                    logger.debug(f"Found GDB: {gdb_path}")
        
        logger.info(f"Found {len(gdb_paths)} GDB file(s) in extraction directory (recursive search)")
        return gdb_paths
        
    except Exception as e:
        logger.error(f"Error finding GDBs: {e}")
        return []


def get_gis_resources_size_gb(directory: str, max_size_gb: float = None) -> float:
    """
    Calculate the size of only GIS-relevant resources (compressed files and .gdb directories)
    Checks first level + GIS subfolder if it exists (not fully recursive)
    This is much faster than calculating the entire directory size
    
    Args:
        directory: Path to directory
        max_size_gb: Optional maximum size threshold. If exceeded, stops calculation
        
    Returns:
        Size in GB of only compressed files and .gdb directories at first level (and GIS subfolder)
    """
    try:
        total_size = 0
        max_size_bytes = max_size_gb * (1024 ** 3) if max_size_gb else None
        
        # Function to check a single directory level
        def check_directory_level(dir_path):
            nonlocal total_size
            
            try:
                items = os.listdir(dir_path)
            except (OSError, PermissionError) as e:
                logger.error(f"Cannot access directory {dir_path}: {e}")
                return
            
            for item in items:
                item_path = os.path.join(dir_path, item)
                
                try:
                    # Check if it's a .gdb directory
                    if os.path.isdir(item_path) and item.lower().endswith('.gdb'):
                        # Calculate size of entire .gdb directory (recursively inside the .gdb)
                        for gdb_root, gdb_subdirs, gdb_files in os.walk(item_path):
                            for gdb_file in gdb_files:
                                try:
                                    file_path = os.path.join(gdb_root, gdb_file)
                                    if not os.path.islink(file_path):
                                        total_size += os.path.getsize(file_path)
                                        
                                        # Early exit check
                                        if max_size_bytes and total_size > max_size_bytes:
                                            return True  # Signal to stop
                                except (OSError, FileNotFoundError) as e:
                                    logger.warning(f"Could not get size of {file_path}: {e}")
                    
                    # Check if it's a compressed file
                    elif os.path.isfile(item_path) and item.lower().endswith(('.zip', '.7z', '.rar')):
                        if not os.path.islink(item_path):
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
            logger.debug(f"GIS resources in {directory} exceeded {max_size_gb} GB threshold at {size_gb:.2f} GB")
            return size_gb
        
        # Also check GIS subfolder if it exists
        gis_folder = os.path.join(directory, 'GIS')
        if os.path.isdir(gis_folder):
            should_stop = check_directory_level(gis_folder)
            if should_stop:
                size_gb = total_size / (1024 ** 3)
                logger.debug(f"GIS resources in {directory} exceeded {max_size_gb} GB threshold at {size_gb:.2f} GB")
                return size_gb
        
        # Convert bytes to GB
        size_gb = total_size / (1024 ** 3)
        logger.debug(f"GIS resources in {directory}: {size_gb:.2f} GB (compressed files + .gdb directories at first level)")
        return size_gb
        
    except Exception as e:
        logger.error(f"Error calculating GIS resources size: {e}")
        return 0.0


def get_directory_size_gb(directory: str, max_size_gb: float = None) -> float:
    """
    Calculate the total size of a directory in GB using OS-native commands for speed
    Works on both Windows and Linux
    Can stop early if max_size_gb threshold is exceeded
    
    Args:
        directory: Path to directory
        max_size_gb: Optional maximum size threshold. If exceeded, stops calculation
        
    Returns:
        Size in GB (or current size if max_size_gb was exceeded)
    """
    try:
        import platform
        
        # Note: OS-native commands (PowerShell, du) don't support early exit easily
        # So we always use the fallback method when max_size_gb is specified for better control
        if max_size_gb is not None:
            logger.debug(f"Using fallback method with early exit for {directory}")
            return _get_directory_size_gb_fallback(directory, max_size_gb)
        
        if platform.system() == 'Windows':
            # Use PowerShell for Windows
            cmd = f'powershell -Command "(Get-ChildItem -Path \'{directory}\' -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum"'
            
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0 and result.stdout.strip():
                total_bytes = int(result.stdout.strip())
                size_gb = total_bytes / (1024 ** 3)
                logger.debug(f"Directory {directory} size: {size_gb:.2f} GB")
                return size_gb
            else:
                logger.warning(f"PowerShell command failed, falling back to Python method")
                return _get_directory_size_gb_fallback(directory, max_size_gb)
                
        elif platform.system() == 'Linux':
            # Use du command for Linux (much faster than os.walk)
            # du -sb returns size in bytes
            result = subprocess.run(
                ['du', '-sb', directory],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0 and result.stdout.strip():
                # du output format: "SIZE\tDIRECTORY"
                total_bytes = int(result.stdout.split()[0])
                size_gb = total_bytes / (1024 ** 3)
                logger.debug(f"Directory {directory} size: {size_gb:.2f} GB")
                return size_gb
            else:
                logger.warning(f"du command failed, falling back to Python method")
                return _get_directory_size_gb_fallback(directory, max_size_gb)
        else:
            # For other systems (Mac, etc), fall back to Python method
            logger.debug(f"Unknown OS, using Python fallback method")
            return _get_directory_size_gb_fallback(directory, max_size_gb)
        
    except Exception as e:
        logger.warning(f"Error with OS-native size calculation, using fallback: {e}")
        return _get_directory_size_gb_fallback(directory, max_size_gb)


def _get_directory_size_gb_fallback(directory: str, max_size_gb: float = None) -> float:
    """
    Fallback method using Python's os.walk (slower but works everywhere)
    Can stop early if max_size_gb is exceeded
    
    Args:
        directory: Path to directory
        max_size_gb: Optional maximum size threshold. If exceeded, stops calculation and returns current size
        
    Returns:
        Size in GB (or current size if max_size_gb was exceeded)
    """
    try:
        total_size = 0
        max_size_bytes = max_size_gb * (1024 ** 3) if max_size_gb else None
        
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                try:
                    # Skip symbolic links
                    if not os.path.islink(filepath):
                        total_size += os.path.getsize(filepath)
                        
                        # Early exit if we've exceeded the maximum
                        if max_size_bytes and total_size > max_size_bytes:
                            size_gb = total_size / (1024 ** 3)
                            logger.debug(f"Directory {directory} exceeded {max_size_gb} GB threshold, stopping calculation at {size_gb:.2f} GB")
                            return size_gb
                            
                except (OSError, FileNotFoundError) as e:
                    logger.warning(f"Could not get size of {filepath}: {e}")
                    continue
        
        # Convert bytes to GB
        size_gb = total_size / (1024 ** 3)
        logger.debug(f"Directory {directory} size: {size_gb:.2f} GB")
        return size_gb
        
    except Exception as e:
        logger.error(f"Error calculating directory size: {e}")
        return 0.0

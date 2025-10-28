"""
Main script for GIS data ingestion from FGDB to PostgreSQL
"""

import os
from typing import Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil

# Import ArcPy
import arcpy

# Import local modules
from config import SDE_CONNECTION, ROOT_PATH, FOLDER_PREFIX, GIS_FOLDER_NAME, REGION_NAME, region_MAPPING
from logger_setup import setup_logger
from database import (
    connect_to_gis,
    get_next_batch_id,
    normalize_geom_type_for_table,
    get_column_set_from_fields,
    get_or_create_version,
    get_ingestion_id_for_gdb,
    create_versioned_table_from_gdb_fields,
    import_features_to_versioned_table,
    update_All_regions_Excavations_header,
    initialize_ingestion_id_from_db
    )
from file_scanner import (
    scan_root_directory,
    find_gis_resources,
    extract_archive,
    get_source_directory_name,
    get_extraction_user,
    get_extracted_files_dir,
    get_gis_resources_size_gb,
    organize_gdbs_in_source_directory,
    find_all_gdbs_recursively
)
from gdb_handler import (
    open_fgdb,
    get_gdb_layers,
    get_layer_info,
    normalize_geometry_type
)

# Initialize logger
logger = setup_logger()


def clean_extracted_files():
    """Clean extracted_files directories - keep only .gdb directories"""
    extracted_files_dir = get_extracted_files_dir()
    
    for source_dir in os.listdir(extracted_files_dir):
        source_path = os.path.join(extracted_files_dir, source_dir)
        
        if not os.path.isdir(source_path):
            continue
        
        for item in os.listdir(source_path):
            item_path = os.path.join(source_path, item)
            
            if os.path.isdir(item_path) and (item.lower().endswith('.gdb') or item.lower().endswith('.gitkeep')):
                continue
            
            try:
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            except:
                pass


def process_gdb(
    gdb_path: str,
    source_directory: str,
    sde_connection: str,
    batch_id: int,
    from_compressed: bool = False
) -> bool:
    """
    Process a File Geodatabase using new versioned table structure
    
    Args:
        gdb_path: Path to .gdb folder
        source_directory: Source directory path (up to A- folder)
        sde_connection: SDE connection path
        batch_id: Batch ID for this ingestion run
        from_compressed: Whether the GDB came from a compressed file
        
    Returns:
        True if processing was successful
    """
    logger.info(f"Processing GDB: {gdb_path}")
    
    # Get ingestion ID for this GDB
    ingestion_id = get_ingestion_id_for_gdb(gdb_path)
    logger.info(f"Using ingestion ID: {ingestion_id} for GDB: {gdb_path}")
    
    # Get user info
    try:
        from config import CURRENT_USER
        current_user = CURRENT_USER
    except:
        current_user = get_extraction_user(gdb_path)
    
    # Extract FGDB name
    fgdb_name = os.path.basename(gdb_path)
    
    # Open/validate the GDB
    validated_gdb = open_fgdb(gdb_path)
    if validated_gdb is None:
        logger.error(f"Failed to open GDB: {gdb_path}")
        return False
    
    # Track layer statistics for summary table
    layer_stats = {}  # {geom_type: {'version': 'verA', 'count': 10}}
    
    try:
        # Get all layers
        layers = get_gdb_layers(gdb_path)
        
        if not layers:
            logger.warning(f"No layers found in GDB: {gdb_path}")
            return False
        
        # Process each layer
        for layer_name in layers:
            logger.info(f"Processing layer: {layer_name}")
            
            # Get layer information
            layer_info = get_layer_info(gdb_path, layer_name)
            if not layer_info:
                logger.error(f"Failed to get layer info: {layer_name}")
                continue
            
            # Get normalized geometry type for table naming
            geom_type_arcpy = normalize_geometry_type(layer_info['geometry_type'])
            geom_type_norm = normalize_geom_type_for_table(geom_type_arcpy or 'POLYGON')
            
            # Get column set from fields
            column_set = get_column_set_from_fields(layer_info['fields'])
            
            # Get or create version for this geometry + column combination
            version = get_or_create_version(geom_type_norm, column_set, sde_connection, gdb_path, source_directory)
            
            # Build table name: All_Excavations_header_rows_{geom}_{ver}
            table_name = f"All_Excavations_header_rows_{geom_type_norm}_{version}"
            
            logger.info(f"Layer '{layer_name}' -> Table '{table_name}' (ingestion_id: {ingestion_id})")
            
            # Get spatial reference from layer
            arcpy.env.workspace = gdb_path
            desc = arcpy.Describe(layer_name)
            spatial_ref = desc.spatialReference if hasattr(desc, 'spatialReference') else None
            
            # Create table (function will handle "already exists" gracefully)
            success = create_versioned_table_from_gdb_fields(
                sde_connection=sde_connection,
                table_name=table_name,
                gdb_fields=layer_info['fields'],
                geometry_type=geom_type_arcpy,
                spatial_reference=spatial_ref
            )
            
            if not success:
                logger.error(f"Failed to create/access table '{table_name}'")
                continue
            
            # Import data to table
            success, feature_count = import_features_to_versioned_table(
                sde_connection=sde_connection,
                source_gdb_path=gdb_path,
                source_layer_name=layer_name,
                target_table_name=table_name,
                ingestion_id=ingestion_id,
                creation_user=current_user,
                region=region_MAPPING[REGION_NAME],
                is_new_table=True  # Always treat as new since we just created/verified it
            )
            
            if success:
                logger.info(f"Successfully imported {feature_count} features to '{table_name}'")
                
                # Update layer statistics
                if geom_type_norm not in layer_stats:
                    layer_stats[geom_type_norm] = {'version': version, 'count': 0}
                layer_stats[geom_type_norm]['count'] += feature_count
            else:
                logger.error(f"Failed to import data to '{table_name}'")
        
        # Update summary table
        if layer_stats:
            logger.info(f"Updating All_regions_Excavations_header for ingestion_id {ingestion_id} in the region {REGION_NAME}")
            update_All_regions_Excavations_header(
                sde_connection=sde_connection,
                ingestion_id=ingestion_id,
                gdb_path=gdb_path,
                source_directory=source_directory,
                layer_stats=layer_stats,
                creation_user=current_user,
                from_compressed=from_compressed,
                region=region_MAPPING[REGION_NAME]
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing GDB '{gdb_path}': {e}")
        return False


def process_folder(folder_path: str, sde_connection: str, batch_id: int) -> bool:
    """
    Process a single folder (looking for GIS resources)
    
    Args:
        folder_path: Path to folder
        sde_connection: SDE connection path
        batch_id: Batch ID for this ingestion run
        
    Returns:
        True if any GDB was processed successfully
    """
    logger.info(f"Processing folder: {folder_path}")
    
    # Get source directory name (up to A- folder)
    source_directory = get_source_directory_name(folder_path, FOLDER_PREFIX)
    
    # Find GIS resources
    gis_folder, gdb_path, compressed_files = find_gis_resources(folder_path)
    
    # If GIS folder exists, search inside it
    if gis_folder:
        logger.info(f"Found GIS folder: {gis_folder}")
        _, gdb_path_in_gis, compressed_files_in_gis = find_gis_resources(gis_folder)
        
        if gdb_path_in_gis:
            gdb_path = gdb_path_in_gis
        if compressed_files_in_gis:
            compressed_files = compressed_files_in_gis
    
    # Determine which folder to copy GDBs from
    check_folder = gis_folder if gis_folder else folder_path
    
    # If we have GIS resources (GDB or compressed files), process them
    if gis_folder or gdb_path or compressed_files:
        # Get source directory base name for organizing extracted files
        source_dir_basename = os.path.basename(source_directory)
        extraction_subdir = os.path.join(get_extracted_files_dir(), source_dir_basename)
        
        # Create source-specific subdirectory in extracted_files
        if not os.path.exists(extraction_subdir):
            os.makedirs(extraction_subdir)
            logger.info(f"Created extraction subdirectory: {extraction_subdir}")
        
        # Track which GDBs existed before extraction (directly copied, not from compressed)
        gdbs_before_extraction = set()
        
        # Copy only GDB files to extraction subdirectory (not everything)
        logger.info(f"Copying GDB files to {extraction_subdir}...")
        # Copy GDBs from check_folder to extraction_subdir
        try:
            for item in os.listdir(check_folder):
                source_path = os.path.join(check_folder, item)
                if os.path.isdir(source_path) and item.lower().endswith('.gdb'):
                    dest_path = os.path.join(extraction_subdir, item)
                    if not os.path.exists(dest_path):
                        try:
                            shutil.copytree(source_path, dest_path)
                            logger.info(f"Copied GDB: {item}")
                        except Exception as e:
                            logger.warning(f"Could not copy {item}: {e}")
        except Exception as e:
            logger.warning(f"Error listing directory {check_folder}: {e}")
        
        # Get list of GDBs that were directly copied (not from compressed files)
        gdbs_before_extraction = set(find_all_gdbs_recursively(extraction_subdir))
        logger.info(f"Found {len(gdbs_before_extraction)} GDB(s) directly from source (not compressed)")
        
        # Extract compressed files if found
        if compressed_files:
            logger.info(f"Found {len(compressed_files)} compressed file(s) - extracting...")
            for archive_path in compressed_files:
                # Extract to source-specific subdirectory
                extract_archive(archive_path, source_directory_name=source_dir_basename)
            
            # Organize extracted GDBs: find them recursively, copy to root
            logger.info("Organizing extracted GDB files...")
            organize_gdbs_in_source_directory(source_dir_basename)
        
        # Find all GDB files in the source-specific extraction directory
        all_gdb_paths = find_all_gdbs_recursively(extraction_subdir)
        
        if not all_gdb_paths:
            logger.warning(f"No GDB files found after extraction in {folder_path}")
            return False
        
        # Determine which GDBs came from compressed files
        gdbs_from_compressed = set(all_gdb_paths) - gdbs_before_extraction
        logger.info(f"Found {len(gdbs_from_compressed)} GDB(s) from compressed files")
        logger.info(f"Total GDBs to process: {len(all_gdb_paths)}")
        
        # Process all GDB files found
        success_count = 0
        for gdb_path in all_gdb_paths:
            is_from_compressed = gdb_path in gdbs_from_compressed
            logger.info(f"Processing GDB: {gdb_path} (from_compressed: {is_from_compressed})")
            if process_gdb(gdb_path, source_directory, sde_connection, batch_id, from_compressed=is_from_compressed):
                success_count += 1
                logger.info(f"Successfully processed GDB: {gdb_path}")
            else:
                logger.error(f"Failed to process GDB: {gdb_path}")
        
        return success_count > 0
    
    else:
        logger.info(f"No GIS resources found in folder: {folder_path}")
        return False


def main():
    """
    Main entry point for the script
    """
    logger.info("=" * 80)
    logger.info("Starting GIS Data Ingestion Process")
    logger.info("=" * 80)
    
    # Check if root path exists
    if not os.path.exists(ROOT_PATH):
        logger.error(f"Root path does not exist: {ROOT_PATH}")
        logger.error("Please update ROOT_PATH in config.py")
        return
    
    # Connect to database via SDE
    try:
        sde_conn = connect_to_gis(SDE_CONNECTION)
        logger.info("Successfully connected to Enterprise Geodatabase")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.error("Please update SDE_CONNECTION path in config.py")
        return
    
    try:
        # Initialize ingestion ID from database (to avoid duplicates on restart)
        initialize_ingestion_id_from_db(sde_conn)
        
        # Get next batch ID
        batch_id = get_next_batch_id(sde_conn)
        logger.info(f"Using batch ID: {batch_id}")
        
        # Scan root directory for matching folders
        matching_folders = scan_root_directory(ROOT_PATH, FOLDER_PREFIX)
        
        if not matching_folders:
            logger.warning(f"No folders found starting with '{FOLDER_PREFIX}' in {ROOT_PATH}")
            return
        
        # Filter out directories already in extracted_files/
        extract_dir = get_extracted_files_dir()
        if os.path.exists(extract_dir):
            already_extracted = set()
            for item in os.listdir(extract_dir):
                if os.path.isdir(os.path.join(extract_dir, item)):
                    already_extracted.add(item)
            
            original_count = len(matching_folders)
            matching_folders = [f for f in matching_folders if os.path.basename(f) not in already_extracted]
            skipped_count = original_count - len(matching_folders)
            
            if skipped_count > 0:
                logger.info(f"Skipped {skipped_count} directories already extracted (found in extracted_files/)")
        
        if not matching_folders:
            logger.warning(f"No folders to process after filtering already extracted directories")
            return
        
        # Filter out directories listed in huge_dirs.txt
        huge_dirs_file = "huge_dirs.txt"
        if os.path.exists(huge_dirs_file):
            try:
                with open(huge_dirs_file, 'r', encoding='utf-8') as f:
                    huge_dirs = set()
                    for line in f:
                        line = line.strip()
                        if line:
                            # Extract just the directory name (not full path)
                            huge_dirs.add(os.path.basename(line))
                
                original_count = len(matching_folders)
                matching_folders = [f for f in matching_folders if os.path.basename(f) not in huge_dirs]
                skipped_count = original_count - len(matching_folders)
                
                if skipped_count > 0:
                    logger.info(f"Skipped {skipped_count} directories listed in {huge_dirs_file}")
            except Exception as e:
                logger.warning(f"Could not read {huge_dirs_file}: {e}")
        
        if not matching_folders:
            logger.warning(f"No folders to process after filtering huge directories")
            return
        
        # Filter folders by GIS resources size (keep only those 20GB or under) - using parallel threads
        logger.info(f"Checking GIS resources size (compressed files + .gdb files at first level) for {len(matching_folders)} source directories in parallel...")
        folders_to_process = []
        skipped_folders = []
        
        # Use ThreadPoolExecutor to check sizes in parallel
        max_workers = min(10, len(matching_folders))  # Use up to 10 threads
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all size calculation tasks with 20GB threshold for early exit
            future_to_folder = {
                executor.submit(get_gis_resources_size_gb, folder_path, 20): folder_path 
                for folder_path in matching_folders
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_folder):
                folder_path = future_to_folder[future]
                try:
                    folder_size_gb = future.result()
                    if folder_size_gb <= 20:
                        folders_to_process.append(folder_path)
                    else:
                        skipped_folders.append((folder_path, folder_size_gb))
                except Exception as e:
                    logger.error(f"Error calculating size for {folder_path}: {e}")
                    # Skip this folder if we can't calculate its size
                    skipped_folders.append((folder_path, 0))
        
        # Log summary
        logger.info("=" * 80)
        logger.info(f"{len(folders_to_process)} from {len(matching_folders)} source directories will be processed (20GB or under)")
        if skipped_folders:
            logger.info(f"{len(skipped_folders)} directories skipped due to size > 20GB:")
            for skipped_path, size_gb in skipped_folders:
                logger.info(f"  - {os.path.basename(skipped_path)}: {size_gb:.2f} GB")
        logger.info("=" * 80)
        
        if not folders_to_process:
            logger.warning("No folders to process after size filtering")
            return
        
        # Process each folder
        success_count = 0
        for idx, folder_path in enumerate(folders_to_process, 1):
            try:
                if process_folder(folder_path, sde_conn, batch_id):
                    success_count += 1
            except Exception as e:
                logger.error(f"Error processing folder '{folder_path}': {e}")
                continue
            
            # Clean up every 5 directories
            if idx % 5 == 0:
                clean_extracted_files()
        
        # Final cleanup
        clean_extracted_files()
        
        logger.info("=" * 80)
        logger.info(f"Processing complete. Successfully processed {success_count}/{len(folders_to_process)} folders")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        
    finally:
        # SDE connections don't need explicit closing
        logger.info("")
        logger.info("Process complete")


if __name__ == "__main__":
    main()

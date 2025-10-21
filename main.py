"""
Main script for GIS data ingestion from FGDB to PostgreSQL
"""

import os
import sys
from typing import Optional
from datetime import datetime

# Import ArcPy
import arcpy

# Import local modules
from config import SDE_CONNECTION, ROOT_PATH, FOLDER_PREFIX, GIS_FOLDER_NAME
from logger_setup import setup_logger
from database import (
    connect_to_gis, 
    table_exists, 
    get_table_columns,
    get_table_geometry_type,
    check_data_already_imported,
    create_table_from_gdb_fields,
    get_next_batch_id,
    import_features_to_table,
    # New versioned functions
    normalize_geom_type_for_table,
    get_column_set_from_fields,
    get_or_create_version,
    get_ingestion_id_for_gdb,
    create_versioned_table_from_gdb_fields,
    import_features_to_versioned_table,
    update_excavationcenter_header,
    debug_table_existence
)
from file_scanner import (
    scan_root_directory,
    find_gis_resources,
    extract_archive,
    get_source_directory_name,
    get_extracted_gdb_path,
    get_extraction_user,
    cleanup_extracted_files_dir,
    find_all_gdbs_in_extracted_dir,
    get_extracted_files_dir,
    check_if_directory_already_processed,
    copy_gdb_files_only
)
from gdb_handler import (
    open_fgdb,
    get_gdb_layers,
    get_layer_info,
    normalize_geometry_type,
    compare_layer_fields_with_table
)

# Initialize logger
logger = setup_logger()

# Global data structure to collect all layer schemas
# Format: {(gdb_name, layer_name, geom_type): set(column_names)}
LAYER_SCHEMAS = {}


def process_gdb(
    gdb_path: str,
    source_directory: str,
    sde_connection: str,
    batch_id: int
) -> bool:
    """
    Process a File Geodatabase using new versioned table structure
    
    Args:
        gdb_path: Path to .gdb folder
        source_directory: Source directory path (up to A- folder)
        sde_connection: SDE connection path
        batch_id: Batch ID for this ingestion run
        
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
            
            # Build table name: excavationcenter_header_rows_{geom}_{ver}
            table_name = f"excavationcenter_header_rows_{geom_type_norm}_{version}"
            
            logger.info(f"Layer '{layer_name}' -> Table '{table_name}' (ingestion_id: {ingestion_id})")
            
            # Always try to create new table (will handle "already exists" error)
            logger.info(f"Creating or using existing table: {table_name}")
            
            # Get spatial reference from layer
            import arcpy
            arcpy.env.workspace = gdb_path
            desc = arcpy.Describe(layer_name)
            spatial_ref = desc.spatialReference if hasattr(desc, 'spatialReference') else None
            
            # Create table (function will handle "already exists" gracefully)
            success = create_versioned_table_from_gdb_fields(
                sde_connection=sde_connection,
                table_name=table_name,
                gdb_fields=layer_info['fields'],
                geometry_type=geom_type_arcpy,
                spatial_reference=spatial_ref,
                creation_user=current_user
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
            logger.info(f"Updating excavationcenter_header for ingestion_id {ingestion_id}")
            update_excavationcenter_header(
                sde_connection=sde_connection,
                ingestion_id=ingestion_id,
                gdb_path=gdb_path,
                source_directory=source_directory,
                layer_stats=layer_stats,
                creation_user=current_user
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing GDB '{gdb_path}': {e}")
        import traceback
        logger.error(traceback.format_exc())
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
    
    # Check if this directory has already been processed
    check_folder = gis_folder if gis_folder else folder_path
    if check_if_directory_already_processed(check_folder):
        logger.info(f"Directory already processed, skipping: {check_folder}")
        return True
    
    # If we have GIS resources (GDB or compressed files), process them
    if gis_folder or gdb_path or compressed_files:
        # Copy only GDB files to extraction directory (not everything)
        logger.info("Copying GDB files to extraction directory...")
        copy_gdb_files_only(check_folder)
        
        # Extract compressed files if found
        if compressed_files:
            logger.info(f"Found {len(compressed_files)} compressed file(s) - extracting...")
            for archive_path in compressed_files:
                # Extract directly to extracted_files directory
                extract_archive(archive_path)
        
        # Clean up - remove all non-GDB files
        logger.info("Cleaning up non-GDB files from extraction directory...")
        cleanup_extracted_files_dir()
        
        # Find all GDB files in extraction directory
        gdb_paths = find_all_gdbs_in_extracted_dir()
        
        if not gdb_paths:
            logger.warning(f"No GDB files found after extraction in {folder_path}")
            return False
        
        # Process all GDB files found
        success_count = 0
        for gdb_path in gdb_paths:
            logger.info(f"Processing GDB: {gdb_path}")
            if process_gdb(gdb_path, source_directory, sde_connection, batch_id):
                success_count += 1
                logger.info(f"Successfully processed GDB: {gdb_path}")
            else:
                logger.error(f"Failed to process GDB: {gdb_path}")
        
        # Final cleanup after processing all GDBs from this folder
        logger.info("Final cleanup of extraction directory...")
        cleanup_extracted_files_dir()
        
        return success_count > 0
    
    else:
        logger.info(f"No GIS resources found in folder: {folder_path}")
        return False


def analyze_and_print_schemas():
    """
    Analyze collected layer schemas and print summary of column differences
    """
    if not LAYER_SCHEMAS:
        logger.info("No schemas collected for analysis")
        return
    
    # Get all unique columns across all layers
    all_columns = set()
    for columns in LAYER_SCHEMAS.values():
        all_columns.update(columns)
    
    if not all_columns:
        logger.info("No columns found in any layer")
        return
    
    # Group layers by their column sets
    # Format: {frozenset(columns): [(gdb_name, layer_name, geom_type), ...]}
    column_groups = {}
    for (gdb_name, layer_name, geom_type), columns in LAYER_SCHEMAS.items():
        columns_key = frozenset(columns)
        if columns_key not in column_groups:
            column_groups[columns_key] = []
        column_groups[columns_key].append((gdb_name, layer_name, geom_type))
    
    # Sort groups by number of columns (ascending) and then by missing columns count
    sorted_groups = sorted(
        column_groups.items(),
        key=lambda x: (len(x[0]), len(all_columns - x[0]))
    )
    
    # Print schema analysis
    logger.info("=" * 80)
    logger.info("SCHEMA ANALYSIS - Column Distribution Across Layers")
    logger.info("=" * 80)
    logger.info(f"Total unique columns found: {len(all_columns)}")
    logger.info(f"All columns: {', '.join(sorted(all_columns))}")
    logger.info("")
    
    for columns_set, layer_list in sorted_groups:
        columns = set(columns_set)
        missing_columns = all_columns - columns
        
        # Format layer names
        layer_names = []
        for gdb_name, layer_name, geom_type in sorted(layer_list):
            layer_names.append(f"{gdb_name}_{layer_name}")
        
        # Build the output string
        layers_str = ", ".join(layer_names)
        columns_str = f"[{', '.join(sorted(columns))}]" if columns else "[]"
        missing_str = f"[{', '.join(sorted(missing_columns))}]" if missing_columns else "[]"
        
        logger.info(f"{layers_str} columns are: {columns_str} (missing {missing_str})")
    
    logger.info("=" * 80)


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
        # Get next batch ID
        batch_id = get_next_batch_id(sde_conn)
        logger.info(f"Using batch ID: {batch_id}")
        
        # Scan root directory for matching folders
        matching_folders = scan_root_directory(ROOT_PATH, FOLDER_PREFIX)
        
        if not matching_folders:
            logger.warning(f"No folders found starting with '{FOLDER_PREFIX}' in {ROOT_PATH}")
            return
        
        # Process each folder
        success_count = 0
        for folder_path in matching_folders:
            try:
                if process_folder(folder_path, sde_conn, batch_id):
                    success_count += 1
            except Exception as e:
                logger.error(f"Error processing folder '{folder_path}': {e}")
                continue
        
        # Final cleanup of extraction directory (in case any files remain)
        logger.info("Final cleanup of extraction directory...")
        cleanup_extracted_files_dir()
        
        logger.info("=" * 80)
        logger.info(f"Processing complete. Successfully processed {success_count}/{len(matching_folders)} folders")
        logger.info("=" * 80)
        
        # Analyze and print schema differences
        logger.info("")
        analyze_and_print_schemas()
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        
    finally:
        # SDE connections don't need explicit closing
        logger.info("")
        logger.info("Process complete")


if __name__ == "__main__":
    main()

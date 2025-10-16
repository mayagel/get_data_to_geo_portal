"""
Main script for GIS data ingestion from FGDB to PostgreSQL
"""

import os
import sys
from typing import Optional
from datetime import datetime

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
    import_features_to_table
)
from file_scanner import (
    scan_root_directory,
    find_gis_resources,
    extract_archive,
    get_source_directory_name
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
    Process a File Geodatabase
    
    Args:
        gdb_path: Path to .gdb folder
        source_directory: Source directory path (up to A- folder)
        sde_connection: SDE connection path
        batch_id: Batch ID for this ingestion run
        
    Returns:
        True if processing was successful
    """
    logger.info(f"Processing GDB: {gdb_path}")
    
    # Extract FGDB name
    fgdb_name = os.path.basename(gdb_path)
    
    # Extract source directory name (the A-xxxx folder name)
    source_dir_name = os.path.basename(source_directory)
    
    # Extract GDB name without .gdb extension
    gdb_name_without_ext = os.path.splitext(fgdb_name)[0]
    
    # Open/validate the GDB
    validated_gdb = open_fgdb(gdb_path)
    if validated_gdb is None:
        logger.error(f"Failed to open GDB: {gdb_path}")
        return False
    
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
            
            # Collect schema information for analysis
            geom_type = normalize_geometry_type(layer_info['geometry_type']) or 'table'
            column_names = set(
                field['name'].lower() for field in layer_info['fields']
                if field['name'].lower() not in ['objectid', 'oid', 'shape', 'geometry', 'shape_length', 'shape_area']
            )
            schema_key = (fgdb_name, layer_name, geom_type)
            LAYER_SCHEMAS[schema_key] = column_names
            
            # Create table name: <source_dir>_<gdb_name>_<layer_name>
            # Normalize each part (lowercase, replace special chars with underscores)
            source_part = source_dir_name.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            gdb_part = gdb_name_without_ext.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            layer_part = layer_name.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            table_name = f"{source_part}_{gdb_part}_{layer_part}"
            
            # Check if table exists
            if table_exists(sde_connection, table_name):
                logger.info(f"Table '{table_name}' already exists")
                
                # Check if data already imported
                if check_data_already_imported(sde_connection, table_name, source_directory, fgdb_name):
                    logger.info(f"Data from '{source_directory}' / '{fgdb_name}' already imported to '{table_name}'. Skipping.")
                    continue
                
                # Get normalized geometry type from layer
                layer_geom_type = normalize_geometry_type(layer_info['geometry_type'])
                
                # Get table geometry type
                table_geom_type = get_table_geometry_type(sde_connection, table_name)
                
                # Check if geometry types match
                if table_geom_type and layer_geom_type:
                    if table_geom_type.upper() != layer_geom_type.upper():
                        logger.warning(
                            f"Geometry type mismatch for table '{table_name}'. "
                            f"Table has '{table_geom_type}', layer has '{layer_geom_type}'. Skipping layer."
                        )
                        continue
                
                # Check if fields match
                table_columns = get_table_columns(sde_connection, table_name)
                fields_match, layer_exclusive, table_exclusive = compare_layer_fields_with_table(
                    layer_info['fields'], 
                    table_columns,
                    fgdb_name,
                    source_directory
                )
                
                if not fields_match:
                    logger.warning(f"Schema mismatch for table '{table_name}'. Skipping layer.")
                    continue
                
                logger.info(f"Table '{table_name}' exists with matching schema. Importing data...")
                
                # Import data
                success = import_features_to_table(
                    sde_connection=sde_connection,
                    source_gdb_path=gdb_path,
                    source_layer_name=layer_name,
                    target_table_name=table_name,
                    source_directory=source_directory,
                    fgdb_name=fgdb_name,
                    batch_id=batch_id
                )
                
                if success:
                    logger.info(f"Successfully imported data to '{table_name}'")
                else:
                    logger.error(f"Failed to import data to '{table_name}'")
                
            else:
                # Create new table
                logger.info(f"Creating new table: {table_name}")
                
                # Get normalized geometry type
                geom_type = normalize_geometry_type(layer_info['geometry_type'])
                
                # Get spatial reference from layer
                import arcpy
                arcpy.env.workspace = gdb_path
                desc = arcpy.Describe(layer_name)
                spatial_ref = desc.spatialReference if hasattr(desc, 'spatialReference') else None
                
                # Create table
                success = create_table_from_gdb_fields(
                    sde_connection=sde_connection,
                    table_name=table_name,
                    gdb_fields=layer_info['fields'],
                    geometry_type=geom_type,
                    spatial_reference=spatial_ref
                )
                
                if success:
                    logger.info(f"Successfully created table '{table_name}'. Importing data...")
                    
                    # Import data
                    import_success = import_features_to_table(
                        sde_connection=sde_connection,
                        source_gdb_path=gdb_path,
                        source_layer_name=layer_name,
                        target_table_name=table_name,
                        source_directory=source_directory,
                        fgdb_name=fgdb_name,
                        batch_id=batch_id
                    )
                    
                    if import_success:
                        logger.info(f"Successfully imported data to '{table_name}'")
                    else:
                        logger.error(f"Failed to import data to '{table_name}'")
                else:
                    logger.error(f"Failed to create table '{table_name}'")
                    continue
        
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
    
    # Extract compressed files if found
    if compressed_files:
        logger.info(f"Found {len(compressed_files)} compressed file(s)")
        for archive_path in compressed_files:
            success = extract_archive(archive_path)
            
            # After successful extraction, search again for GDB
            if success:
                extract_dir = os.path.dirname(archive_path)
                _, new_gdb_path, _ = find_gis_resources(extract_dir)
                if new_gdb_path:
                    gdb_path = new_gdb_path
            else:
                logger.warning(f"Skipping archive '{archive_path}' - extraction failed. Continuing with other files.")
    
    # Process GDB if found
    if gdb_path:
        logger.info(f"Found GDB: {gdb_path}")
        return process_gdb(gdb_path, source_directory, sde_connection, batch_id)
    else:
        logger.info(f"No GDB found in folder: {folder_path}")
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

"""
Main script for GIS data ingestion from FGDB to PostgreSQL
"""

import os
import sys
from typing import Optional
from datetime import datetime

# Import local modules
from config import POSTGRES_CONFIG, ROOT_PATH, FOLDER_PREFIX, GIS_FOLDER_NAME
from logger_setup import setup_logger
from database import (
    connect_to_gis, 
    table_exists, 
    get_table_columns,
    check_data_already_imported,
    create_table_from_gdb_fields,
    get_next_batch_id
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


def process_gdb(
    gdb_path: str,
    source_directory: str,
    conn,
    batch_id: int
) -> bool:
    """
    Process a File Geodatabase
    
    Args:
        gdb_path: Path to .gdb folder
        source_directory: Source directory path (up to A- folder)
        conn: Database connection
        batch_id: Batch ID for this ingestion run
        
    Returns:
        True if processing was successful
    """
    logger.info(f"Processing GDB: {gdb_path}")
    
    # Extract FGDB name
    fgdb_name = os.path.basename(gdb_path)
    
    # Open the GDB
    datasource = open_fgdb(gdb_path)
    if datasource is None:
        logger.error(f"Failed to open GDB: {gdb_path}")
        return False
    
    try:
        # Get all layers
        layers = get_gdb_layers(datasource)
        
        if not layers:
            logger.warning(f"No layers found in GDB: {gdb_path}")
            return False
        
        # Process each layer
        for layer_name in layers:
            logger.info(f"Processing layer: {layer_name}")
            
            layer = datasource.GetLayerByName(layer_name)
            if layer is None:
                logger.error(f"Failed to get layer: {layer_name}")
                continue
            
            # Get layer information
            layer_info = get_layer_info(layer)
            if not layer_info:
                logger.error(f"Failed to get layer info: {layer_name}")
                continue
            
            # Normalize table name (lowercase, replace spaces with underscores)
            table_name = layer_name.lower().replace(' ', '_').replace('-', '_')
            
            # Check if table exists
            if table_exists(conn, table_name):
                logger.info(f"Table '{table_name}' already exists")
                
                # Check if data already imported
                if check_data_already_imported(conn, table_name, source_directory, fgdb_name):
                    logger.info(f"Data from '{source_directory}' / '{fgdb_name}' already imported to '{table_name}'. Skipping.")
                    continue
                
                # Check if fields match
                table_columns = get_table_columns(conn, table_name)
                if not compare_layer_fields_with_table(layer_info['fields'], table_columns):
                    logger.warning(f"Schema mismatch for table '{table_name}'. Fields do not match exactly. Skipping layer.")
                    continue
                
                logger.info(f"Table '{table_name}' exists with matching schema. Ready for data import.")
                # TODO: Implement data import logic here
                
            else:
                # Create new table
                logger.info(f"Creating new table: {table_name}")
                
                # Get normalized geometry type
                geom_type = normalize_geometry_type(layer_info['geometry_type'])
                
                # Create table
                success = create_table_from_gdb_fields(
                    conn,
                    table_name,
                    layer_info['fields'],
                    geom_type
                )
                
                if success:
                    logger.info(f"Successfully created table '{table_name}'. Ready for data import.")
                    # TODO: Implement data import logic here
                else:
                    logger.error(f"Failed to create table '{table_name}'")
                    continue
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing GDB '{gdb_path}': {e}")
        return False
        
    finally:
        datasource = None  # Close datasource


def process_folder(folder_path: str, conn, batch_id: int) -> bool:
    """
    Process a single folder (looking for GIS resources)
    
    Args:
        folder_path: Path to folder
        conn: Database connection
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
            extract_archive(archive_path)
            
            # After extraction, search again for GDB
            extract_dir = os.path.dirname(archive_path)
            _, new_gdb_path, _ = find_gis_resources(extract_dir)
            if new_gdb_path:
                gdb_path = new_gdb_path
    
    # Process GDB if found
    if gdb_path:
        logger.info(f"Found GDB: {gdb_path}")
        return process_gdb(gdb_path, source_directory, conn, batch_id)
    else:
        logger.info(f"No GDB found in folder: {folder_path}")
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
    
    # Connect to database
    try:
        conn = connect_to_gis(POSTGRES_CONFIG)
        logger.info("Successfully connected to PostgreSQL database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.error("Please update database credentials in config.py")
        return
    
    try:
        # Get next batch ID
        batch_id = get_next_batch_id(conn)
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
                if process_folder(folder_path, conn, batch_id):
                    success_count += 1
            except Exception as e:
                logger.error(f"Error processing folder '{folder_path}': {e}")
                continue
        
        logger.info("=" * 80)
        logger.info(f"Processing complete. Successfully processed {success_count}/{len(matching_folders)} folders")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        
    finally:
        # Close database connection
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()

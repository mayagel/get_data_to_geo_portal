"""
Database connection and operations using ArcPy with SDE connection
"""

import arcpy
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime
import logging
import json

logger = logging.getLogger("GISIngestion.database")

# Global tracking for versions and ingestion IDs
VERSION_TRACKER = {}  # {(geom_type, frozenset(columns)): version_id}
NEXT_VERSION_IDS = {'poly': 'A', 'line': 'A', 'point': 'A'}  # Track next version letter per geometry type
CURRENT_INGESTION_ID = 1  # Global ingestion ID counter
GDB_INGESTION_IDS = {}  # {gdb_path: ingestion_id} to track same ID for layers from same GDB


def connect_to_gis(sde_connection_path: str) -> Optional[str]:
    """
    Connect to PostgreSQL/Enterprise Geodatabase using SDE connection
    
    Args:
        sde_connection_path: Path to .sde connection file
        
    Returns:
        SDE connection path if successful, None otherwise
        
    Raises:
        Exception: If connection fails
    """
    try:
        # Test the connection by describing the workspace
        desc = arcpy.Describe(sde_connection_path)
        logger.debug(f"Connected to {desc.workspaceType} workspace: {desc.connectionString}")
        logger.info(f"Successfully connected to enterprise geodatabase via SDE")
        return sde_connection_path
    except Exception as e:
        logger.exception(f"Error while connecting to GIS database: {e}")
        raise


def normalize_geom_type_for_table(geom_type: str) -> str:
    """
    Normalize geometry type to table naming convention (poly/line/point)
    
    Args:
        geom_type: Geometry type from ArcPy
        
    Returns:
        Normalized type: 'poly', 'line', or 'point'
    """
    geom_upper = geom_type.upper() if geom_type else ''
    
    if 'POLYGON' in geom_upper or 'MULTIPATCH' in geom_upper:
        return 'poly'
    elif 'LINE' in geom_upper or 'POLYLINE' in geom_upper:
        return 'line'
    elif 'POINT' in geom_upper:
        return 'point'
    else:
        return 'poly'  # Default to poly


def get_column_set_from_fields(fields: List[Dict]) -> frozenset:
    """
    Get a normalized set of column names from field list (excluding system fields)
    
    Args:
        fields: List of field dictionaries
        
    Returns:
        Frozen set of column names
    """
    column_names = set()
    for field in fields:
        field_name = field['name'].lower()
        # Exclude system fields
        if field_name not in ['objectid', 'oid', 'shape', 'geometry', 'fid', 'shape_length', 'shape_area']:
            column_names.add(field_name)
    
    return frozenset(column_names)


def write_version_to_file(version_id: str, geom_type: str, source_directory: str, gdb_filename: str, columns: list) -> None:
    """
    Write new version information to layers_version.txt file
    
    Args:
        version_id: Version ID (e.g., 'verA')
        geom_type: Geometry type (poly/line/point)
        source_directory: Source directory path
        gdb_filename: GDB file name
        columns: List of column names
    """
    try:
        version_file = "layers_version.txt"
        columns_str = ', '.join(sorted(columns))
        
        with open(version_file, 'a', encoding='utf-8') as f:
            f.write(f"{geom_type}_{version_id}: {source_directory}, {gdb_filename}, [{columns_str}]\n")
        
        logger.debug(f"Wrote version info to {version_file}")
    except Exception as e:
        logger.warning(f"Could not write to layers_version.txt: {e}")


def get_or_create_version(
    geom_type_norm: str, 
    column_set: frozenset, 
    sde_connection: str,
    gdb_path: str = None,
    source_directory: str = None
) -> str:
    """
    Get existing version ID or create new one for a geometry type + column combination
    
    Args:
        geom_type_norm: Normalized geometry type ('poly', 'line', 'point')
        column_set: Frozen set of column names
        sde_connection: SDE connection path
        gdb_path: Path to GDB file (optional, for logging)
        source_directory: Source directory path (optional, for logging)
        
    Returns:
        Version ID (e.g., 'verA', 'verB', 'verC')
    """
    import os
    global VERSION_TRACKER, NEXT_VERSION_IDS
    
    key = (geom_type_norm, column_set)
    
    # Check if we already have this version
    if key in VERSION_TRACKER:
        return VERSION_TRACKER[key]
    
    # Load existing versions from database if this is first time
    if not VERSION_TRACKER:
        load_existing_versions_from_db(sde_connection)
        # Check again after loading
        if key in VERSION_TRACKER:
            return VERSION_TRACKER[key]
    
    # Create new version
    next_letter = NEXT_VERSION_IDS[geom_type_norm]
    version_id = f'ver{next_letter}'
    VERSION_TRACKER[key] = version_id
    
    # Increment version letter
    NEXT_VERSION_IDS[geom_type_norm] = chr(ord(next_letter) + 1)
    
    # Get info for logging
    columns_list = sorted(column_set)
    columns_str = ', '.join(columns_list)
    
    # Log the new version discovery
    if gdb_path and source_directory:
        gdb_filename = os.path.basename(gdb_path)
        logger.info(f"{version_id} found with the columns [{columns_str}] in the gdb {gdb_filename} in the source {source_directory}.")
        
        # Write to file
        write_version_to_file(version_id, geom_type_norm, source_directory, gdb_filename, columns_list)
    else:
        logger.info(f"Created new version {version_id} for {geom_type_norm} with columns: {columns_str}")
    
    return version_id


def load_existing_versions_from_db(sde_connection: str) -> None:
    """
    Load existing version mappings from database tables
    
    Args:
        sde_connection: SDE connection path
    """
    global VERSION_TRACKER, NEXT_VERSION_IDS
    
    try:
        arcpy.env.workspace = sde_connection
        
        # List all tables starting with Center_Excavations_header_rows_
        all_tables = arcpy.ListTables("Center_Excavations_header_rows_*")
        all_fcs = arcpy.ListFeatureClasses("Center_Excavations_header_rows_*")
        
        tables = (all_tables or []) + (all_fcs or [])
        
        for table_name in tables:
            # Parse table name: Center_Excavations_header_rows_{geom}_{ver}
            parts = table_name.split('_')
            if len(parts) >= 6:
                geom_type = parts[4]  # poly/line/point
                version = parts[5]  # verA/verB/etc
                
                # Get columns from this table
                table_path = f"{sde_connection}\\{table_name}"
                fields = arcpy.ListFields(table_path)
                
                column_set = set()
                for field in fields:
                    field_name = field.name.lower()
                    # Exclude system and metadata fields
                    if field_name not in ['objectid', 'oid', 'shape', 'geometry', 'fid', 
                                          'creation_date', 'update_date', 'creation_user', 
                                          'update_user', 'ingestion_id', 'shape_length', 'shape_area']:
                        column_set.add(field_name)
                
                key = (geom_type, frozenset(column_set))
                VERSION_TRACKER[key] = version
                
                # Update next version letter if needed
                version_letter = version.replace('ver', '')
                if version_letter >= NEXT_VERSION_IDS[geom_type]:
                    NEXT_VERSION_IDS[geom_type] = chr(ord(version_letter) + 1)
                
                logger.debug(f"Loaded existing version {version} for {geom_type}")
        
        logger.info(f"Loaded {len(VERSION_TRACKER)} existing versions from database")
        
    except Exception as e:
        logger.warning(f"Could not load existing versions from database: {e}")


def get_ingestion_id_for_gdb(gdb_path: str) -> int:
    """
    Get or create ingestion ID for a GDB file
    Same GDB gets same ingestion ID for all its layers
    
    Args:
        gdb_path: Path to GDB file
        
    Returns:
        Ingestion ID
    """
    global CURRENT_INGESTION_ID, GDB_INGESTION_IDS
    
    if gdb_path in GDB_INGESTION_IDS:
        return GDB_INGESTION_IDS[gdb_path]
    
    # Assign new ingestion ID
    ingestion_id = CURRENT_INGESTION_ID
    GDB_INGESTION_IDS[gdb_path] = ingestion_id
    CURRENT_INGESTION_ID += 1
    
    logger.debug(f"Assigned ingestion ID {ingestion_id} to GDB: {gdb_path}")
    
    return ingestion_id


def force_delete_table(sde_connection: str, table_name: str) -> bool:
    """
    Force delete a table that might be stuck in SDE metadata
    
    Args:
        sde_connection: SDE connection path
        table_name: Name of the table to delete
        
    Returns:
        True if deletion successful or table doesn't exist
    """
    try:
        arcpy.env.workspace = sde_connection
        table_path = f"{sde_connection}\\{table_name}"
        
        # Try to delete if it exists
        if arcpy.Exists(table_path):
            logger.info(f"Attempting to force delete table: {table_name}")
            try:
                arcpy.Delete_management(table_path)
                logger.info(f"Successfully deleted table: {table_name}")
            except Exception as e:
                logger.warning(f"Could not delete table (might not actually exist): {e}")
        
        # Clear all caches
        arcpy.ClearWorkspaceCache_management()
        arcpy.RefreshCatalog(sde_connection)
        
        # Verify it's gone
        if arcpy.Exists(table_path):
            logger.warning(f"Table still appears to exist after deletion: {table_name}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error during force delete: {e}")
        return False


def table_exists(sde_connection: str, table_name: str) -> bool:
    """
    Check if a table/feature class exists in the database
    
    Args:
        sde_connection: SDE connection path
        table_name: Name of the table to check
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        arcpy.env.workspace = sde_connection
        
        # Clear cache to get fresh data
        arcpy.ClearWorkspaceCache_management()
        
        # Check using arcpy.Exists (most reliable)
        table_path = f"{sde_connection}\\{table_name}"
        exists = arcpy.Exists(table_path)
        
        if exists:
            logger.debug(f"Table/Feature class '{table_name}' exists")
        else:
            logger.debug(f"Table/Feature class '{table_name}' does not exist")
            
        return exists
        
    except Exception as e:
        logger.error(f"Error checking if table exists: {e}")
        return False


def debug_table_existence(sde_connection: str, table_name: str) -> None:
    """
    Debug function to check table existence using multiple methods
    
    Args:
        sde_connection: SDE connection path
        table_name: Name of the table to check
    """
    try:
        arcpy.env.workspace = sde_connection
        arcpy.ClearWorkspaceCache_management()
        
        table_path = f"{sde_connection}\\{table_name}"
        
        logger.info(f"=== DEBUG: Checking table existence for '{table_name}' ===")
        
        # Method 1: arcpy.Exists
        exists1 = arcpy.Exists(table_path)
        logger.info(f"arcpy.Exists: {exists1}")
        
        # Method 2: ListFeatureClasses
        try:
            fcs = arcpy.ListFeatureClasses(table_name)
            exists2 = fcs and table_name in [fc.lower() for fc in fcs]
            logger.info(f"ListFeatureClasses: {exists2} (found: {fcs})")
        except Exception as e:
            logger.info(f"ListFeatureClasses error: {e}")
        
        # Method 3: ListTables
        try:
            tables = arcpy.ListTables(table_name)
            exists3 = tables and table_name in [tbl.lower() for tbl in tables]
            logger.info(f"ListTables: {exists3} (found: {tables})")
        except Exception as e:
            logger.info(f"ListTables error: {e}")
        
        # Method 4: List all tables
        try:
            all_fcs = arcpy.ListFeatureClasses()
            all_tables = arcpy.ListTables()
            all_items = (all_fcs or []) + (all_tables or [])
            matching_items = [item for item in all_items if item.lower() == table_name.lower()]
            logger.info(f"All tables/FCs containing '{table_name}': {matching_items}")
        except Exception as e:
            logger.info(f"List all items error: {e}")
        
        logger.info("=== END DEBUG ===")
        
    except Exception as e:
        logger.error(f"Debug function error: {e}")


def get_table_columns(sde_connection: str, table_name: str) -> List[Tuple[str, str]]:
    """
    Get column names and types from a table
    
    Args:
        sde_connection: SDE connection path
        table_name: Name of the table
        
    Returns:
        List of tuples (column_name, data_type)
    """
    try:
        arcpy.env.workspace = sde_connection
        
        # Get full path to the table
        table_path = f"{sde_connection}\\{table_name}"
        
        fields = arcpy.ListFields(table_path)
        columns = []
        
        for field in fields:
            columns.append((field.name.lower(), field.type))
        
        logger.debug(f"Table '{table_name}' has {len(columns)} columns")
        return columns
        
    except Exception as e:
        logger.error(f"Error getting table columns: {e}")
        return []


def get_table_geometry_type(sde_connection: str, table_name: str) -> Optional[str]:
    """
    Get the geometry type from a feature class
    
    Args:
        sde_connection: SDE connection path
        table_name: Name of the table
        
    Returns:
        Geometry type (e.g., 'POINT', 'POLYLINE', 'POLYGON') or None if no geometry column
    """
    try:
        arcpy.env.workspace = sde_connection
        table_path = f"{sde_connection}\\{table_name}"
        
        desc = arcpy.Describe(table_path)
        
        if hasattr(desc, 'shapeType'):
            geom_type = desc.shapeType.upper()
            logger.debug(f"Table '{table_name}' has geometry type: {geom_type}")
            return geom_type
        else:
            logger.debug(f"Table '{table_name}' has no geometry column")
            return None
            
    except Exception as e:
        logger.error(f"Error getting table geometry type: {e}")
        return None


def check_data_already_imported(
    sde_connection: str,
    table_name: str,
    source_directory: str,
    fgdb_name: str
) -> bool:
    """
    Check if data from this source has already been imported
    
    Args:
        sde_connection: SDE connection path
        table_name: Name of the table
        source_directory: Source directory path
        fgdb_name: Name of the FGDB
        
    Returns:
        True if data already exists, False otherwise
    """
    try:
        arcpy.env.workspace = sde_connection
        table_path = f"{sde_connection}\\{table_name}"
        
        # Build where clause to check for existing data
        where_clause = f"source_directory = '{source_directory}' AND fgdb_name = '{fgdb_name}'"
        
        # Count matching records
        count = 0
        with arcpy.da.SearchCursor(table_path, ["OBJECTID"], where_clause=where_clause) as cursor:
            for row in cursor:
                count += 1
        
        if count > 0:
            logger.info(f"Data from '{source_directory}' / '{fgdb_name}' already imported ({count} records)")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Error checking if data already imported: {e}")
        # If columns don't exist, data hasn't been imported with this structure
        return False


def create_versioned_table_from_gdb_fields(
    sde_connection: str,
    table_name: str,
    gdb_fields: List[Dict],
    geometry_type: Optional[str] = None,
    spatial_reference: Optional[arcpy.SpatialReference] = None,
    creation_user: str = 'unknown'
) -> bool:
    """
    Create a new feature class/table with versioned naming and new metadata fields
    
    Args:
        sde_connection: SDE connection path
        table_name: Name of the table to create (e.g., Center_Excavations_header_rows_poly_verA)
        gdb_fields: List of field definitions from GDB
        geometry_type: Geometry type (Point, Polyline, Polygon, etc.)
        spatial_reference: Spatial reference object
        creation_user: User creating the table
        
    Returns:
        True if successful, False otherwise
    """
    try:
        arcpy.env.workspace = sde_connection
        
        # Force refresh the workspace to clear any cached metadata
        arcpy.ClearWorkspaceCache_management()
        
        # Check if table already exists
        table_path = f"{sde_connection}\\{table_name}"
        if arcpy.Exists(table_path):
            logger.info(f"Table '{table_name}' already exists, skipping creation")
            return True
        
        # Default spatial reference if not provided (Israel TM Grid - EPSG:2039)
        if spatial_reference is None:
            spatial_reference = arcpy.SpatialReference(2039)
        
        # Create feature class or table
        try:
            if geometry_type:
                # Create feature class
                output_fc = arcpy.CreateFeatureclass_management(
                    out_path=sde_connection,
                    out_name=table_name,
                    geometry_type=geometry_type,
                    spatial_reference=spatial_reference
                )[0]
                logger.info(f"Created feature class: {table_name}")
            else:
                # Create table (no geometry)
                output_fc = arcpy.CreateTable_management(
                    out_path=sde_connection,
                    out_name=table_name
                )[0]
                logger.info(f"Created table: {table_name}")
        except arcpy.ExecuteError as e:
            error_msg = str(e)
            if "already exists" in error_msg.lower() or "000258" in error_msg:
                logger.error(f"Table '{table_name}' already exists: {error_msg}")
                logger.error(f"ERROR 000258: Table exists in ArcGIS metadata but may not exist in PostgreSQL database")
                logger.error(f"MANUAL FIX REQUIRED:")
                logger.error(f"  1. Open ArcGIS Pro")
                logger.error(f"  2. Open Catalog pane")
                logger.error(f"  3. Navigate to your SDE connection")
                logger.error(f"  4. Find table '{table_name}' and delete it")
                logger.error(f"  5. Restart this script")
                return False
            else:
                logger.error(f"Error creating table '{table_name}': {e}")
                return False
        
        # Add metadata fields FIRST (so they appear at the beginning)
        arcpy.AddField_management(output_fc, "creation_date", "DATE")
        arcpy.AddField_management(output_fc, "update_date", "DATE")
        arcpy.AddField_management(output_fc, "creation_user", "TEXT", field_length=100)
        arcpy.AddField_management(output_fc, "update_user", "TEXT", field_length=100)
        
        # Add fields from GDB
        for field in gdb_fields:
            field_name = field['name']
            field_type = map_gdb_type_to_arcpy(field['type'])
            
            # Skip system fields
            if field_name.upper() in ['OBJECTID', 'OID', 'SHAPE', 'GEOMETRY', 'FID', 'SHAPE_LENGTH', 'SHAPE_AREA']:
                continue
            
            # Add the field
            try:
                field_length = field.get('width', 255) if field_type == 'TEXT' else None
                arcpy.AddField_management(
                    in_table=output_fc,
                    field_name=field_name,
                    field_type=field_type,
                    field_length=field_length
                )
                logger.debug(f"Added field: {field_name} ({field_type})")
            except Exception as e:
                logger.warning(f"Could not add field '{field_name}': {e}")
        
        # Add ingestion_id field at the end
        arcpy.AddField_management(output_fc, "ingestion_id", "LONG")
        
        logger.info(f"Successfully created {'feature class' if geometry_type else 'table'} '{table_name}' with new metadata structure")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        return False


def create_table_from_gdb_fields(
    sde_connection: str,
    table_name: str,
    gdb_fields: List[Dict],
    geometry_type: Optional[str] = None,
    spatial_reference: Optional[arcpy.SpatialReference] = None
) -> bool:
    """
    DEPRECATED: Use create_versioned_table_from_gdb_fields instead
    Create a new feature class/table based on GDB fields with additional metadata fields
    
    Args:
        sde_connection: SDE connection path
        table_name: Name of the table to create
        gdb_fields: List of field definitions from GDB
        geometry_type: Geometry type (Point, Polyline, Polygon, etc.)
        spatial_reference: Spatial reference object
        
    Returns:
        True if successful, False otherwise
    """
    try:
        arcpy.env.workspace = sde_connection
        
        # Default spatial reference if not provided (Israel TM Grid - EPSG:2039)
        if spatial_reference is None:
            spatial_reference = arcpy.SpatialReference(2039)
        
        # Create feature class or table
        if geometry_type:
            # Create feature class
            output_fc = arcpy.CreateFeatureclass_management(
                out_path=sde_connection,
                out_name=table_name,
                geometry_type=geometry_type,
                spatial_reference=spatial_reference
            )[0]
            logger.info(f"Created feature class: {table_name}")
        else:
            # Create table (no geometry)
            output_fc = arcpy.CreateTable_management(
                out_path=sde_connection,
                out_name=table_name
            )[0]
            logger.info(f"Created table: {table_name}")
        
        # Add fields from GDB
        for field in gdb_fields:
            field_name = field['name']
            field_type = map_gdb_type_to_arcpy(field['type'])
            
            # Skip system fields
            if field_name.upper() in ['OBJECTID', 'OID', 'SHAPE', 'GEOMETRY', 'FID', 'SHAPE_LENGTH', 'SHAPE_AREA']:
                continue
            
            # Add the field
            try:
                field_length = field.get('width', 255) if field_type == 'TEXT' else None
                arcpy.AddField_management(
                    in_table=output_fc,
                    field_name=field_name,
                    field_type=field_type,
                    field_length=field_length
                )
                logger.debug(f"Added field: {field_name} ({field_type})")
            except Exception as e:
                logger.warning(f"Could not add field '{field_name}': {e}")
        
        # Add metadata fields
        arcpy.AddField_management(output_fc, "source_directory", "TEXT", field_length=400)
        arcpy.AddField_management(output_fc, "ingestion_datetime", "DATE")
        arcpy.AddField_management(output_fc, "ingestion_batch_id", "LONG")
        arcpy.AddField_management(output_fc, "fgdb_name", "TEXT", field_length=255)
        
        logger.info(f"Successfully created {'feature class' if geometry_type else 'table'} '{table_name}' with metadata fields")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        return False


def map_gdb_type_to_arcpy(gdb_type: str) -> str:
    """
    Map GDB field types to ArcPy field types
    
    Args:
        gdb_type: GDB field type
        
    Returns:
        Corresponding ArcPy field type
    """
    type_mapping = {
        'Integer': 'LONG',
        'SmallInteger': 'SHORT',
        'Double': 'DOUBLE',
        'Single': 'FLOAT',
        'String': 'TEXT',
        'Date': 'DATE',
        'OID': 'LONG',
        'Geometry': 'GEOMETRY',
        'Blob': 'BLOB',
        'Raster': 'RASTER',
        'GUID': 'GUID',
        'GlobalID': 'GUID',
    }
    
    return type_mapping.get(gdb_type, 'TEXT')


def get_next_batch_id(sde_connection: str) -> int:
    """
    Get the next batch ID for ingestion
    Creates a simple table to track batch IDs if it doesn't exist
    
    Args:
        sde_connection: SDE connection path
        
    Returns:
        Next batch ID
    """
    try:
        arcpy.env.workspace = sde_connection
        batch_table = "ingestion_batch_tracker"
        batch_table_path = f"{sde_connection}\\{batch_table}"
        
        # Create batch tracker table if it doesn't exist
        if not arcpy.Exists(batch_table_path):
            arcpy.CreateTable_management(sde_connection, batch_table)
            arcpy.AddField_management(batch_table_path, "batch_id", "LONG")
            arcpy.AddField_management(batch_table_path, "created_date", "DATE")
            logger.info("Created batch tracker table")
        
        # Get the highest batch_id
        max_batch_id = 0
        with arcpy.da.SearchCursor(batch_table_path, ["batch_id"]) as cursor:
            for row in cursor:
                if row[0] and row[0] > max_batch_id:
                    max_batch_id = row[0]
        
        # Increment for new batch
        new_batch_id = max_batch_id + 1
        
        # Insert new batch record
        with arcpy.da.InsertCursor(batch_table_path, ["batch_id", "created_date"]) as cursor:
            cursor.insertRow([new_batch_id, datetime.now()])
        
        logger.debug(f"Generated batch ID: {new_batch_id}")
        return new_batch_id
        
    except Exception as e:
        logger.error(f"Error getting next batch ID: {e}")
        # Return a timestamp-based ID as fallback
        return int(datetime.now().timestamp())


def import_features_to_versioned_table(
    sde_connection: str,
    source_gdb_path: str,
    source_layer_name: str,
    target_table_name: str,
    ingestion_id: int,
    creation_user: str = 'unknown',
    is_new_table: bool = False
) -> Tuple[bool, int]:
    """
    Import features from source GDB layer to versioned target table
    
    Args:
        sde_connection: SDE connection path
        source_gdb_path: Path to source GDB
        source_layer_name: Name of source layer
        target_table_name: Name of target table
        ingestion_id: Ingestion ID for this import
        creation_user: User importing the data
        is_new_table: Whether this is a new table (affects creation_date)
        
    Returns:
        Tuple of (success, feature_count)
    """
    try:
        arcpy.env.workspace = sde_connection
        
        # Clear workspace cache to ensure fresh data
        arcpy.ClearWorkspaceCache_management()
        
        target_path = f"{sde_connection}\\{target_table_name}"
        
        source_path = f"{source_gdb_path}\\{source_layer_name}"
        
        # Get source fields (excluding system fields)
        source_fields = []
        for field in arcpy.ListFields(source_path):
            if field.name.upper() not in ['OBJECTID', 'OID', 'FID', 'SHAPE']:
                source_fields.append(field.name)
        
        # Get target fields
        target_fields = []
        for field in arcpy.ListFields(target_path):
            if field.name.upper() not in ['OBJECTID', 'OID', 'FID']:
                target_fields.append(field.name)
        
        # Find common fields (case-insensitive)
        source_field_map = {f.upper(): f for f in source_fields}
        target_field_map = {f.upper(): f for f in target_fields}
        
        common_field_keys = set(source_field_map.keys()) & set(target_field_map.keys())
        
        # Build field lists for cursors
        read_fields = [source_field_map[key] for key in common_field_keys]
        
        # Add SHAPE if geometry exists
        desc = arcpy.Describe(source_path)
        has_geometry = hasattr(desc, 'shapeType')
        if has_geometry:
            read_fields.append('SHAPE@')
        
        # Prepare write fields (matching order + metadata fields)
        write_fields = [target_field_map[key] for key in common_field_keys]
        if has_geometry:
            write_fields.append('SHAPE@')
        
        # Add new metadata fields
        write_fields.extend(['creation_date', 'update_date', 'creation_user', 'update_user', 'ingestion_id'])
        
        # Start editing session
        edit = arcpy.da.Editor(sde_connection)
        edit.startEditing(with_undo=False, multiuser_mode=False)
        edit.startOperation()
        
        try:
            # Insert features
            inserted_count = 0
            current_time = datetime.now()
            
            with arcpy.da.SearchCursor(source_path, read_fields) as search_cursor:
                with arcpy.da.InsertCursor(target_path, write_fields) as insert_cursor:
                    for row in search_cursor:
                        # Build new row with metadata
                        # creation_date: now if new table, else now (for this specific row)
                        # update_date: now
                        # creation_user and update_user: current user
                        # ingestion_id: from parameter
                        new_row = list(row) + [current_time, current_time, creation_user, creation_user, ingestion_id]
                        insert_cursor.insertRow(new_row)
                        inserted_count += 1
            
            # Save edits
            edit.stopOperation()
            edit.stopEditing(save_changes=True)
            
            logger.info(f"Successfully imported {inserted_count} features from '{source_layer_name}' to '{target_table_name}'")
            return True, inserted_count
            
        except Exception as e:
            # Abort edits on error
            edit.stopOperation()
            edit.stopEditing(save_changes=False)
            raise e
            
    except Exception as e:
        logger.error(f"Error importing features from '{source_layer_name}' to '{target_table_name}': {e}")
        return False, 0


def import_features_to_table(
    sde_connection: str,
    source_gdb_path: str,
    source_layer_name: str,
    target_table_name: str,
    source_directory: str,
    fgdb_name: str,
    batch_id: int
) -> bool:
    """
    DEPRECATED: Use import_features_to_versioned_table instead
    Import features from source GDB layer to target table using ArcPy Editor
    
    Args:
        sde_connection: SDE connection path
        source_gdb_path: Path to source GDB
        source_layer_name: Name of source layer
        target_table_name: Name of target table
        source_directory: Source directory path
        fgdb_name: FGDB file name
        batch_id: Batch ID for this import
        
    Returns:
        True if successful, False otherwise
    """
    try:
        arcpy.env.workspace = sde_connection
        target_path = f"{sde_connection}\\{target_table_name}"
        source_path = f"{source_gdb_path}\\{source_layer_name}"
        
        # Get source fields (excluding system fields)
        source_fields = []
        for field in arcpy.ListFields(source_path):
            if field.name.upper() not in ['OBJECTID', 'OID', 'FID', 'SHAPE']:
                source_fields.append(field.name)
        
        # Get target fields
        target_fields = []
        for field in arcpy.ListFields(target_path):
            if field.name.upper() not in ['OBJECTID', 'OID', 'FID']:
                target_fields.append(field.name)
        
        # Find common fields (case-insensitive)
        source_field_map = {f.upper(): f for f in source_fields}
        target_field_map = {f.upper(): f for f in target_fields}
        
        common_field_keys = set(source_field_map.keys()) & set(target_field_map.keys())
        
        # Build field lists for cursors
        read_fields = [source_field_map[key] for key in common_field_keys]
        
        # Add SHAPE if geometry exists
        desc = arcpy.Describe(source_path)
        has_geometry = hasattr(desc, 'shapeType')
        if has_geometry:
            read_fields.append('SHAPE@')
        
        # Prepare write fields (matching order + metadata fields)
        write_fields = [target_field_map[key] for key in common_field_keys]
        if has_geometry:
            write_fields.append('SHAPE@')
        write_fields.extend(['source_directory', 'ingestion_datetime', 'ingestion_batch_id', 'fgdb_name'])
        
        # Start editing session
        edit = arcpy.da.Editor(sde_connection)
        edit.startEditing(with_undo=False, multiuser_mode=False)
        edit.startOperation()
        
        try:
            # Insert features
            inserted_count = 0
            with arcpy.da.SearchCursor(source_path, read_fields) as search_cursor:
                with arcpy.da.InsertCursor(target_path, write_fields) as insert_cursor:
                    for row in search_cursor:
                        # Build new row with metadata
                        new_row = list(row) + [source_directory, datetime.now(), batch_id, fgdb_name]
                        insert_cursor.insertRow(new_row)
                        inserted_count += 1
            
            # Save edits
            edit.stopOperation()
            edit.stopEditing(save_changes=True)
            
            logger.info(f"Successfully imported {inserted_count} features from '{source_layer_name}' to '{target_table_name}'")
            return True
            
        except Exception as e:
            # Abort edits on error
            edit.stopOperation()
            edit.stopEditing(save_changes=False)
            raise e
            
    except Exception as e:
        logger.error(f"Error importing features from '{source_layer_name}' to '{target_table_name}': {e}")
        return False


def ensure_Center_Excavations_header_table(sde_connection: str) -> bool:
    """
    Ensure the Center_Excavations_header summary table exists
    
    Table structure:
    - Oid (auto)
    - creation_date
    - update_date
    - creation_user
    - update_user
    - poly_ver (version of polygon layers, or None)
    - line_ver (version of line layers, or None)
    - point_ver (version of point layers, or None)
    - ingestion_id (unique ID for this GDB)
    - line_count (number of line features)
    - poly_count (number of polygon features)
    - point_count (number of point features)
    - f_name (GDB file name)
    - s_dir (source directory)
    - from_compressed (1 if GDB came from compressed file, 0 otherwise)
    
    Args:
        sde_connection: SDE connection path
        
    Returns:
        True if table exists or was created successfully
    """
    try:
        arcpy.env.workspace = sde_connection
        table_name = "Center_Excavations_header"
        table_path = f"{sde_connection}\\{table_name}"
        
        if arcpy.Exists(table_path):
            logger.debug(f"Summary table '{table_name}' already exists")
            return True
        
        # Create the table
        arcpy.CreateTable_management(sde_connection, table_name)
        logger.info(f"Created summary table: {table_name}")
        
        # Add fields
        arcpy.AddField_management(table_path, "creation_date", "DATE")
        arcpy.AddField_management(table_path, "update_date", "DATE")
        arcpy.AddField_management(table_path, "creation_user", "TEXT", field_length=100)
        arcpy.AddField_management(table_path, "update_user", "TEXT", field_length=100)
        arcpy.AddField_management(table_path, "poly_ver", "TEXT", field_length=50)
        arcpy.AddField_management(table_path, "line_ver", "TEXT", field_length=50)
        arcpy.AddField_management(table_path, "point_ver", "TEXT", field_length=50)
        arcpy.AddField_management(table_path, "ingestion_id", "LONG")
        arcpy.AddField_management(table_path, "line_count", "LONG")
        arcpy.AddField_management(table_path, "poly_count", "LONG")
        arcpy.AddField_management(table_path, "point_count", "LONG")
        arcpy.AddField_management(table_path, "f_name", "TEXT", field_length=255)
        arcpy.AddField_management(table_path, "s_dir", "TEXT", field_length=500)
        arcpy.AddField_management(table_path, "from_compressed", "SHORT")
        
        logger.info(f"Successfully created summary table '{table_name}' with all fields")
        return True
        
    except Exception as e:
        logger.error(f"Error creating summary table: {e}")
        return False


def update_Center_Excavations_header(
    sde_connection: str,
    ingestion_id: int,
    gdb_path: str,
    source_directory: str,
    layer_stats: Dict[str, Dict],
    creation_user: str = 'unknown',
    from_compressed: bool = False
) -> bool:
    """
    Update or insert a row in the Center_Excavations_header summary table
    
    Args:
        sde_connection: SDE connection path
        ingestion_id: Ingestion ID for this GDB
        gdb_path: Path to GDB file
        source_directory: Source directory path
        layer_stats: Dictionary with stats per geometry type
                     Format: {'poly': {'version': 'verA', 'count': 10}, 'line': {...}, 'point': {...}}
        creation_user: User creating/updating the record
        from_compressed: Whether the GDB came from a compressed file (True) or not (False)
        
    Returns:
        True if successful
    """
    try:
        import os
        
        # Ensure table exists
        if not ensure_Center_Excavations_header_table(sde_connection):
            return False
        
        arcpy.env.workspace = sde_connection
        table_name = "Center_Excavations_header"
        table_path = f"{sde_connection}\\{table_name}"
        
        # Extract GDB file name
        gdb_filename = os.path.basename(gdb_path)
        
        # Prepare values
        poly_ver = layer_stats.get('poly', {}).get('version', None)
        line_ver = layer_stats.get('line', {}).get('version', None)
        point_ver = layer_stats.get('point', {}).get('version', None)
        poly_count = layer_stats.get('poly', {}).get('count', 0)
        line_count = layer_stats.get('line', {}).get('count', 0)
        point_count = layer_stats.get('point', {}).get('count', 0)
        from_compressed_value = 1 if from_compressed else 0
        
        current_time = datetime.now()
        
        # Check if record already exists for this ingestion_id
        where_clause = f"ingestion_id = {ingestion_id}"
        existing_count = 0
        
        with arcpy.da.SearchCursor(table_path, ["OBJECTID"], where_clause=where_clause) as cursor:
            for row in cursor:
                existing_count += 1
        
        if existing_count > 0:
            # Update existing record
            update_fields = ['update_date', 'update_user', 'poly_ver', 'line_ver', 'point_ver',
                           'poly_count', 'line_count', 'point_count', 'from_compressed']
            
            with arcpy.da.UpdateCursor(table_path, update_fields, where_clause=where_clause) as cursor:
                for row in cursor:
                    cursor.updateRow([current_time, creation_user, poly_ver, line_ver, point_ver,
                                     poly_count, line_count, point_count, from_compressed_value])
            
            logger.info(f"Updated summary record for ingestion_id {ingestion_id}")
        else:
            # Insert new record
            insert_fields = ['creation_date', 'update_date', 'creation_user', 'update_user',
                           'poly_ver', 'line_ver', 'point_ver', 'ingestion_id',
                           'poly_count', 'line_count', 'point_count', 'f_name', 's_dir', 'from_compressed']
            
            with arcpy.da.InsertCursor(table_path, insert_fields) as cursor:
                cursor.insertRow([current_time, current_time, creation_user, creation_user,
                                 poly_ver, line_ver, point_ver, ingestion_id,
                                 poly_count, line_count, point_count, gdb_filename, source_directory, from_compressed_value])
            
            logger.info(f"Inserted new summary record for ingestion_id {ingestion_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating summary table: {e}")
        return False

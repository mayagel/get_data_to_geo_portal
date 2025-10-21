"""
Database connection and operations using ArcPy with SDE connection
"""

import arcpy
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime
import logging

logger = logging.getLogger("GISIngestion.database")

# Global tracking for versions and ingestion IDs
VERSION_TRACKER = {}  # {(geom_type, frozenset(columns)): version_id}
NEXT_VERSION_IDS = {'poly': 'A', 'line': 'A', 'point': 'A'}  # Track next version letter per geometry type
CURRENT_INGESTION_ID = 1  # Global ingestion ID counter
GDB_INGESTION_IDS = {}  # {gdb_path: ingestion_id} to track same ID for layers from same GDB


def _increment_version(current_version: str) -> str:
    """
    Increment version string: A->B->...->Z->AA->AB->...->AZ
    
    Args:
        current_version: Current version letter(s)
        
    Returns:
        Next version letter(s)
    """
    if len(current_version) == 1:
        # Single letter: A-Z
        if current_version == 'Z':
            return 'AA'
        else:
            return chr(ord(current_version) + 1)
    elif len(current_version) == 2:
        # Double letter: AA-AZ
        if current_version[1] == 'Z':
            # Reached AZ, can't go further
            logger.warning(f"Reached maximum version AZ for geometry type!")
            return 'AZ'
        else:
            return current_version[0] + chr(ord(current_version[1]) + 1)
    else:
        # Should not happen
        return current_version


def _compare_versions(version1: str, version2: str) -> int:
    """
    Compare two version strings
    
    Args:
        version1: First version (e.g., 'A', 'Z', 'AA')
        version2: Second version
        
    Returns:
        -1 if version1 < version2, 0 if equal, 1 if version1 > version2
    """
    # Single letter versions come before double letter
    if len(version1) == 1 and len(version2) == 2:
        return -1
    elif len(version1) == 2 and len(version2) == 1:
        return 1
    elif len(version1) == len(version2):
        if version1 < version2:
            return -1
        elif version1 > version2:
            return 1
        else:
            return 0
    else:
        return 0


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
    
    # Increment version letter (A->B->...->Z->AA->AB->...->AZ)
    NEXT_VERSION_IDS[geom_type_norm] = _increment_version(next_letter)
    
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
                if _compare_versions(version_letter, NEXT_VERSION_IDS[geom_type]) >= 0:
                    NEXT_VERSION_IDS[geom_type] = _increment_version(version_letter)
        
        logger.info(f"Loaded {len(VERSION_TRACKER)} existing versions from database")
        
    except Exception as e:
        logger.warning(f"Could not load existing versions from database: {e}")


def initialize_ingestion_id_from_db(sde_connection: str) -> None:
    """
    Initialize the global ingestion ID counter from the database
    Gets the maximum ingestion_id from Center_Excavations_header and continues from there
    
    Args:
        sde_connection: SDE connection path
    """
    global CURRENT_INGESTION_ID
    
    try:
        # Ensure the table exists first
        if not ensure_Center_Excavations_header_table(sde_connection):
            logger.warning("Could not ensure Center_Excavations_header table exists, starting from ID 1")
            return
        
        arcpy.env.workspace = sde_connection
        table_name = "Center_Excavations_header"
        table_path = f"{sde_connection}\\{table_name}"
        
        # Get maximum ingestion_id from the table
        max_ingestion_id = 0
        try:
            with arcpy.da.SearchCursor(table_path, ["ingestion_id"]) as cursor:
                for row in cursor:
                    if row[0] and row[0] > max_ingestion_id:
                        max_ingestion_id = row[0]
        except Exception as e:
            logger.warning(f"Could not query ingestion_id from database: {e}")
        
        # Set current ingestion ID to continue from the maximum
        if max_ingestion_id > 0:
            CURRENT_INGESTION_ID = max_ingestion_id + 1
            logger.info(f"Continuing from ingestion_id: {CURRENT_INGESTION_ID} (max in DB: {max_ingestion_id})")
        else:
            logger.info(f"No existing ingestion_ids found, starting from: {CURRENT_INGESTION_ID}")
        
    except Exception as e:
        logger.warning(f"Error initializing ingestion ID from database: {e}")
        logger.info(f"Starting from default ingestion_id: {CURRENT_INGESTION_ID}")


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
    
    return ingestion_id




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
            except Exception as e:
                logger.warning(f"Could not add field '{field_name}': {e}")
        
        # Add ingestion_id field at the end
        arcpy.AddField_management(output_fc, "ingestion_id", "LONG")
        
        logger.info(f"Successfully created {'feature class' if geometry_type else 'table'} '{table_name}' with new metadata structure")
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
    - main_folder_name (name of the source directory, e.g., A-8569_Darchmon_20191030)
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
        arcpy.AddField_management(table_path, "main_folder_name", "TEXT", field_length=255)
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
        
        # Extract main folder name from source directory
        main_folder_name = os.path.basename(source_directory.rstrip('\\').rstrip('/'))
        
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
                           'poly_count', 'line_count', 'point_count', 'f_name', 's_dir', 'main_folder_name', 'from_compressed']
            
            with arcpy.da.InsertCursor(table_path, insert_fields) as cursor:
                cursor.insertRow([current_time, current_time, creation_user, creation_user,
                                 poly_ver, line_ver, point_ver, ingestion_id,
                                 poly_count, line_count, point_count, gdb_filename, source_directory, main_folder_name, from_compressed_value])
            
            logger.info(f"Inserted new summary record for ingestion_id {ingestion_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating summary table: {e}")
        return False

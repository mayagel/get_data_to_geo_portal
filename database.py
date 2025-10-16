"""
Database connection and operations using ArcPy with SDE connection
"""

import arcpy
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime
import logging

logger = logging.getLogger("GISIngestion.database")


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
        
        # Check in feature classes
        feature_classes = arcpy.ListFeatureClasses(table_name)
        if feature_classes and table_name in [fc.lower() for fc in feature_classes]:
            logger.debug(f"Feature class '{table_name}' exists")
            return True
        
        # Check in tables
        tables = arcpy.ListTables(table_name)
        if tables and table_name in [tbl.lower() for tbl in tables]:
            logger.debug(f"Table '{table_name}' exists")
            return True
            
        logger.debug(f"Table/Feature class '{table_name}' does not exist")
        return False
        
    except Exception as e:
        logger.error(f"Error checking if table exists: {e}")
        return False


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


def create_table_from_gdb_fields(
    sde_connection: str,
    table_name: str,
    gdb_fields: List[Dict],
    geometry_type: Optional[str] = None,
    spatial_reference: Optional[arcpy.SpatialReference] = None
) -> bool:
    """
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

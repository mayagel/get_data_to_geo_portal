"""
File Geodatabase (FGDB) handling utilities using ArcPy
"""

import logging
from typing import List, Dict, Optional, Tuple, Set
import arcpy

logger = logging.getLogger("GISIngestion.gdb")


def open_fgdb(gdb_path: str) -> Optional[str]:
    """
    Open a File Geodatabase (validate it exists and can list layers)

    Args:
        gdb_path: Path to .gdb folder

    Returns:
        The path if successful, None otherwise
    """
    try:
        arcpy.env.workspace = gdb_path
        layers = arcpy.ListFeatureClasses()
        if layers is None:
            logger.error(f"No layers found in GDB: {gdb_path}")
            return None
        logger.debug(f"Successfully opened GDB: {gdb_path}")
        return gdb_path
    except Exception as e:
        logger.error(f"Error opening GDB '{gdb_path}': {e}")
        return None


def get_gdb_layers(gdb_path: str) -> List[str]:
    """
    Get list of layer names from GDB

    Args:
        gdb_path: Path to .gdb folder

    Returns:
        List of layer names
    """
    try:
        arcpy.env.workspace = gdb_path
        layers = arcpy.ListFeatureClasses()
        if layers:
            logger.info(f"Found {len(layers)} layers in GDB")
            return layers
        else:
            logger.info("No layers found in GDB")
            return []
    except Exception as e:
        logger.error(f"Error getting GDB layers: {e}")
        return []


def get_layer_info(gdb_path: str, layer_name: str) -> Optional[Dict]:
    """
    Get information about a layer (fields, geometry type, etc.)

    Args:
        gdb_path: Path to .gdb folder
        layer_name: Layer name inside the GDB

    Returns:
        Dictionary with layer information, or None if error
    """
    try:
        arcpy.env.workspace = gdb_path
        desc = arcpy.Describe(layer_name)

        # Fields
        fields = []
        for field in arcpy.ListFields(layer_name):
            fields.append({
                'name': field.name,
                'type': field.type,
                'width': field.length,
                'precision': getattr(field, 'precision', None)
            })

        # Geometry type
        geom_type = desc.shapeType

        # EPSG code
        epsg_code = None
        if desc.spatialReference and desc.spatialReference.factoryCode:
            epsg_code = desc.spatialReference.factoryCode

        # Feature count
        feature_count = int(arcpy.GetCount_management(layer_name)[0])

        info = {
            'name': desc.name,
            'fields': fields,
            'geometry_type': geom_type,
            'epsg_code': epsg_code,
            'feature_count': feature_count
        }

        logger.debug(f"Layer '{desc.name}': {feature_count} features, type: {geom_type}")
        return info

    except Exception as e:
        logger.error(f"Error getting layer info for '{layer_name}': {e}")
        return None


def normalize_geometry_type(geom_type: str) -> Optional[str]:
    """
    Normalize geometry type name for ArcPy

    Args:
        geom_type: ArcPy geometry type name

    Returns:
        Normalized geometry type for ArcPy (uppercase)
    """
    # Remove 3D/Z/M modifiers
    geom_type = geom_type.replace('3D ', '').replace('ZM', '').replace('Z', '').replace('M', '').strip()

    type_mapping = {
        'Point': 'POINT',
        'Multipoint': 'MULTIPOINT',
        'Polyline': 'POLYLINE',  # Changed from LINESTRING to POLYLINE for ArcPy
        'Polygon': 'POLYGON',
        'Multipatch': 'MULTIPATCH',  # Changed from GEOMETRYCOLLECTION to MULTIPATCH for ArcPy
    }

    return type_mapping.get(geom_type)


def compare_layer_fields_with_table(
    layer_fields: List[Dict], 
    table_columns: List[tuple],
    fgdb_name: str,
    source_directory: str
) -> Tuple[bool, Set[str], Set[str]]:
    """
    Compare layer fields with existing table columns

    Args:
        layer_fields: List of field definitions from layer
        table_columns: List of (column_name, data_type) from table
        fgdb_name: Name of the FGDB file
        source_directory: Source directory path

    Returns:
        Tuple of (fields_match, layer_exclusive_fields, table_exclusive_fields)
    """
    layer_field_names = set(
        field['name'].lower() for field in layer_fields
        if field['name'].lower() not in ['objectid', 'oid', 'shape', 'geometry']
    )

    metadata_columns = {'id', 'source_directory', 'ingestion_datetime', 'ingestion_batch_id', 'fgdb_name', 'geometry'}
    table_field_names = set(
        col[0].lower() for col in table_columns
        if col[0].lower() not in metadata_columns
    )

    layer_exclusive = layer_field_names - table_field_names
    table_exclusive = table_field_names - layer_field_names

    if layer_field_names == table_field_names:
        logger.debug("Layer fields match table columns")
        return True, set(), set()
    else:
        # Log warnings in the required format
        if layer_exclusive:
            exclusive_list = ', '.join(sorted(layer_exclusive))
            logger.warning(
                f"{fgdb_name} from {source_directory} have exclusive columns [{exclusive_list}]"
            )
        
        if table_exclusive:
            exclusive_list = ', '.join(sorted(table_exclusive))
            logger.warning(
                f"Table has exclusive columns not in {fgdb_name} from {source_directory}: [{exclusive_list}]"
            )
        
        return False, layer_exclusive, table_exclusive

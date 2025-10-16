"""
File Geodatabase (FGDB) handling utilities using ArcPy
"""

import logging
from typing import List, Dict, Optional
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
        logger.info(f"Found {len(layers)} layers in GDB")
        return layers if layers else []
    except Exception as e:
        logger.error(f"Error getting GDB layers: {e}")
        return []


def get_layer_info(gdb_path: str, layer_name: str) -> Dict:
    """
    Get information about a layer (fields, geometry type, etc.)

    Args:
        gdb_path: Path to .gdb folder
        layer_name: Layer name inside the GDB

    Returns:
        Dictionary with layer information
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
        return {}


def normalize_geometry_type(geom_type: str) -> Optional[str]:
    """
    Normalize geometry type name for PostgreSQL/PostGIS

    Args:
        geom_type: ArcPy geometry type name

    Returns:
        Normalized geometry type for PostGIS
    """
    geom_type = geom_type.replace('3D ', '').replace('ZM', '').replace('Z', '').replace('M', '').strip()

    type_mapping = {
        'Point': 'POINT',
        'Multipoint': 'MULTIPOINT',
        'Polyline': 'LINESTRING',
        'Polygon': 'POLYGON',
        'Multipatch': 'GEOMETRYCOLLECTION',
    }

    return type_mapping.get(geom_type)


def compare_layer_fields_with_table(layer_fields: List[Dict], table_columns: List[tuple]) -> bool:
    """
    Compare layer fields with existing table columns

    Args:
        layer_fields: List of field definitions from layer
        table_columns: List of (column_name, data_type) from table

    Returns:
        True if fields match, False otherwise
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

    if layer_field_names == table_field_names:
        logger.debug("Layer fields match table columns")
        return True
    else:
        logger.warning(f"Field mismatch - Layer: {layer_field_names}, Table: {table_field_names}")
        logger.warning(f"Missing in table: {layer_field_names - table_field_names}")
        logger.warning(f"Extra in table: {table_field_names - layer_field_names}")
        return False

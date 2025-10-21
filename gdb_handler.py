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



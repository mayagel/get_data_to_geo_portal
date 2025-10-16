"""
Database connection and operations for PostgreSQL
"""

import psycopg2
import psycopg2.extensions
from typing import List, Dict, Optional, Tuple
from config import DBConfig, SCHEMA_NAME, ADDITIONAL_FIELDS
import logging

logger = logging.getLogger("GISIngestion.database")


def connect_to_gis(config: DBConfig) -> psycopg2.extensions.connection:
    """
    Connect to PostgreSQL database
    
    Args:
        config: Database configuration
        
    Returns:
        Database connection object
        
    Raises:
        Exception: If connection fails
    """
    try:
        conn = psycopg2.connect(
            host=config.host,
            user=config.user,
            dbname=config.db_name,
            password=config.password,
            port=config.port
        )
        logger.debug(f"Connected to postgresql {config.host}.{config.db_name} successfully")
        return conn
    except Exception as e:
        logger.exception(f"Error while connecting to GIS db {e}")
        raise


def table_exists(conn: psycopg2.extensions.connection, table_name: str) -> bool:
    """
    Check if a table exists in the database
    
    Args:
        conn: Database connection
        table_name: Name of the table to check
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """
        cursor.execute(query, (SCHEMA_NAME, table_name.lower()))
        exists = cursor.fetchone()[0]
        cursor.close()
        logger.debug(f"Table '{table_name}' exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error checking if table exists: {e}")
        raise


def get_table_columns(conn: psycopg2.extensions.connection, table_name: str) -> List[Tuple[str, str]]:
    """
    Get column names and types from a table
    
    Args:
        conn: Database connection
        table_name: Name of the table
        
    Returns:
        List of tuples (column_name, data_type)
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s
            ORDER BY ordinal_position;
        """
        cursor.execute(query, (SCHEMA_NAME, table_name.lower()))
        columns = cursor.fetchall()
        cursor.close()
        logger.debug(f"Table '{table_name}' has {len(columns)} columns")
        return columns
    except Exception as e:
        logger.error(f"Error getting table columns: {e}")
        raise


def check_data_already_imported(
    conn: psycopg2.extensions.connection,
    table_name: str,
    source_directory: str,
    fgdb_name: str
) -> bool:
    """
    Check if data from this source has already been imported
    
    Args:
        conn: Database connection
        table_name: Name of the table
        source_directory: Source directory path
        fgdb_name: Name of the FGDB
        
    Returns:
        True if data already exists, False otherwise
    """
    try:
        cursor = conn.cursor()
        query = f"""
            SELECT COUNT(*) 
            FROM {SCHEMA_NAME}.{table_name} 
            WHERE source_directory = %s 
            AND fgdb_name = %s;
        """
        cursor.execute(query, (source_directory, fgdb_name))
        count = cursor.fetchone()[0]
        cursor.close()
        
        if count > 0:
            logger.info(f"Data from '{source_directory}' / '{fgdb_name}' already imported ({count} records)")
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking if data already imported: {e}")
        # If columns don't exist, data hasn't been imported with this structure
        return False


def create_table_from_gdb_fields(
    conn: psycopg2.extensions.connection,
    table_name: str,
    gdb_fields: List[Dict],
    geometry_type: Optional[str] = None
) -> bool:
    """
    Create a new table based on GDB fields with additional metadata fields
    
    Args:
        conn: Database connection
        table_name: Name of the table to create
        gdb_fields: List of field definitions from GDB
        geometry_type: Geometry type (Point, LineString, Polygon, etc.)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        cursor = conn.cursor()
        
        # Start building CREATE TABLE statement
        field_definitions = []
        
        # Add primary key
        field_definitions.append("id SERIAL PRIMARY KEY")
        
        # Add GDB fields
        for field in gdb_fields:
            field_name = field['name'].lower()
            field_type = map_gdb_type_to_postgres(field['type'])
            
            # Skip OID and Shape fields (we'll handle geometry separately)
            if field_name in ['objectid', 'oid', 'shape', 'geometry']:
                continue
                
            field_definitions.append(f"{field_name} {field_type}")
        
        # Add geometry field if geometry type is specified
        if geometry_type:
            field_definitions.append(f"geometry GEOMETRY({geometry_type}, 2039)")  # EPSG:2039 - Israel TM Grid
        
        # Add additional metadata fields
        field_definitions.append("source_directory VARCHAR(400)")
        field_definitions.append("ingestion_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        field_definitions.append("ingestion_batch_id INTEGER")
        field_definitions.append("fgdb_name VARCHAR(255)")
        
        # Create the table
        create_query = f"""
            CREATE TABLE {SCHEMA_NAME}.{table_name} (
                {', '.join(field_definitions)}
            );
        """
        
        cursor.execute(create_query)
        
        # Create spatial index if geometry exists
        if geometry_type:
            index_query = f"""
                CREATE INDEX idx_{table_name}_geometry 
                ON {SCHEMA_NAME}.{table_name} 
                USING GIST (geometry);
            """
            cursor.execute(index_query)
        
        conn.commit()
        cursor.close()
        
        logger.info(f"Successfully created table '{table_name}' with {len(field_definitions)} fields")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        conn.rollback()
        return False


def map_gdb_type_to_postgres(gdb_type: str) -> str:
    """
    Map GDB field types to PostgreSQL types
    
    Args:
        gdb_type: GDB field type
        
    Returns:
        Corresponding PostgreSQL type
    """
    type_mapping = {
        'Integer': 'INTEGER',
        'SmallInteger': 'SMALLINT',
        'Double': 'DOUBLE PRECISION',
        'Single': 'REAL',
        'String': 'TEXT',
        'Date': 'TIMESTAMP',
        'OID': 'INTEGER',
        'Geometry': 'GEOMETRY',
        'Blob': 'BYTEA',
        'Raster': 'RASTER',
        'GUID': 'UUID',
        'GlobalID': 'UUID',
    }
    
    return type_mapping.get(gdb_type, 'TEXT')


def get_next_batch_id(conn: psycopg2.extensions.connection) -> int:
    """
    Get the next batch ID for ingestion
    
    Args:
        conn: Database connection
        
    Returns:
        Next batch ID
    """
    try:
        cursor = conn.cursor()
        # Create a sequence if it doesn't exist
        cursor.execute("""
            CREATE SEQUENCE IF NOT EXISTS ingestion_batch_id_seq;
        """)
        cursor.execute("SELECT nextval('ingestion_batch_id_seq');")
        batch_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        logger.debug(f"Generated batch ID: {batch_id}")
        return batch_id
    except Exception as e:
        logger.error(f"Error getting next batch ID: {e}")
        raise

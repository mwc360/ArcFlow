"""
Table utilities for arcflow framework

Common utilities for building table references, paths, and identifiers
"""
from typing import Optional


def build_table_reference(
    catalog_name: Optional[str],
    schema_name: str,
    table_name: str
) -> str:
    """
    Build fully qualified table reference for Delta/Spark tables
    
    Args:
        catalog_name: Optional catalog/lakehouse name (e.g., 'my_lakehouse')
        schema_name: Schema/database name (e.g., 'bronze', 'silver', 'gold')
        table_name: Table name (e.g., 'shipment', 'customer')
        
    Returns:
        Fully qualified table reference with proper escaping
        
    Examples:
        >>> build_table_reference(None, 'bronze', 'shipment')
        '`bronze`.`shipment`'
        
        >>> build_table_reference('my_lakehouse', 'silver', 'customer')
        '`my_lakehouse`.`silver`.`customer`'
        
    Notes:
        - Uses backtick escaping for identifier safety
        - Supports 2-level (schema.table) and 3-level (catalog.schema.table) namespaces
        - Compatible with Microsoft Fabric, Databricks, and local Spark catalogs
    """
    if catalog_name:
        return f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
    else:
        return f"`{schema_name}`.`{table_name}`"


def parse_table_reference(table_reference: str) -> tuple[Optional[str], str, str]:
    """
    Parse a table reference into its components
    
    Args:
        table_reference: Fully qualified table reference (e.g., '`catalog`.`schema`.`table`')
        
    Returns:
        Tuple of (catalog_name, schema_name, table_name)
        
    Examples:
        >>> parse_table_reference('`bronze`.`shipment`')
        (None, 'bronze', 'shipment')
        
        >>> parse_table_reference('`my_lakehouse`.`silver`.`customer`')
        ('my_lakehouse', 'silver', 'customer')
    """
    # Remove backticks and split
    parts = table_reference.replace('`', '').split('.')
    
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return None, parts[0], parts[1]
    else:
        raise ValueError(f"Invalid table reference: {table_reference}")


def get_table_identifier(zone: str, table_name: str) -> str:
    """
    Get a simple identifier for a table (used in logging, checkpoints, etc.)
    
    Args:
        zone: Zone name (e.g., 'bronze', 'silver')
        table_name: Table name (e.g., 'shipment')
        
    Returns:
        Simple identifier string
        
    Examples:
        >>> get_table_identifier('bronze', 'shipment')
        'bronze_shipment'
    """
    return f"{zone}_{table_name}"

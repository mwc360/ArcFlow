"""
Common transformations applied across all zones

These are universal transformations that every table gets
"""
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, ArrayType
import re
from typing import Optional


def to_snake_case(name: str) -> str:
    """
    Convert string to snake_case
    
    Handles:
    - CamelCase/PascalCase
    - Punctuation (except underscores)
    - Numbers
    - Preserves existing underscores
    
    Args:
        name: Input string
        
    Returns:
        snake_case string
    """
    # Replace punctuation with space (preserve underscores)
    name = re.sub(r"[^A-Za-z0-9_]+", " ", name)
    
    # Split camelCase/PascalCase
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name)
    
    # Add space before numbers
    name = re.sub(r"([a-z])(\d+)", r"\1 \2", name)
    
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    
    # Convert to lowercase and replace spaces with underscores
    result = name.lower().replace(" ", "_")
    
    # Clean up multiple consecutive underscores
    result = re.sub(r"_+", "_", result)
    
    return result


def normalize_columns_to_snake_case(
    df: DataFrame,
    schema: Optional[StructType] = None
) -> DataFrame:
    """
    Normalize all column names to snake_case recursively
    
    Handles:
    - Top-level columns
    - Nested struct fields (any depth)
    - Arrays of structs
    
    Args:
        df: Input DataFrame
        schema: Optional schema (defaults to df.schema)
        
    Returns:
        DataFrame with all columns renamed to snake_case
    """
    from pyspark.sql import Column as SparkColumn
    
    def build_struct_from_lambda(lambda_var: SparkColumn, struct_type: StructType) -> SparkColumn:
        """Build struct from lambda variable with snake_case field names"""
        return sf.struct(*[
            lambda_var[field.name].alias(to_snake_case(field.name))
            for field in struct_type.fields
        ])
    
    def transform_struct_fields(col_path: str, struct_type: StructType) -> SparkColumn:
        """Recursively transform struct fields to snake_case"""
        return sf.struct(*[
            _transform_field(f"{col_path}.{field.name}", field.name, field.dataType)
            for field in struct_type.fields
        ])
    
    def _transform_field(col_path: str, field_name: str, field_type) -> SparkColumn:
        """Transform a single field based on its type"""
        new_name = to_snake_case(field_name)
        
        if isinstance(field_type, StructType):
            # Nested struct - recursively transform
            return transform_struct_fields(col_path, field_type).alias(new_name)
        elif isinstance(field_type, ArrayType) and isinstance(field_type.elementType, StructType):
            # Array of structs - transform each element
            return sf.transform(
                sf.col(col_path),
                lambda x: build_struct_from_lambda(x, field_type.elementType)
            ).alias(new_name)
        else:
            # Simple field or array of primitives
            return sf.col(col_path).alias(new_name)
    
    if schema is None:
        schema = df.schema
    
    select_exprs = [
        _transform_field(field.name, field.name, field.dataType)
        for field in schema.fields
    ]
    
    return df.select(*select_exprs)


def apply_processing_timestamp(df: DataFrame) -> DataFrame:
    """
    Add processing timestamp to DataFrame
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with _processing_timestamp column
    """
    return df.withColumn("_processing_timestamp", sf.current_timestamp())


def add_zone_metadata(df: DataFrame, zone: str, table_name: str) -> DataFrame:
    """
    Add zone-specific metadata columns
    
    Universal transformation applied across all zones
    
    Args:
        df: Input DataFrame
        zone: Zone name (e.g., 'bronze', 'silver')
        table_name: Table name
        
    Returns:
        DataFrame with metadata columns
    """
    return (df
        .withColumn('_zone', sf.lit(zone))
        .withColumn('_table_name', sf.lit(table_name))
        .withColumn('_zone_timestamp', sf.current_timestamp())
    )

"""
Zone-specific custom transformations

Registry of custom transformation functions that can be applied per table per zone
"""
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
from typing import Callable, Dict
import logging


logger = logging.getLogger(__name__)


# ============================================================================
# Example Custom Transformations
# ============================================================================
# Add your custom transformations here and register them in ZONE_TRANSFORMERS

def example_bronze_validation(df: DataFrame) -> DataFrame:
    """
    Example bronze zone validation
    
    Args:
        df: Input DataFrame
        table_config: SourceConfig or DimensionConfig
        zone_config: ZoneConfig
        
    Returns:
        Transformed DataFrame
    """
    return df.withColumn('_is_valid', sf.lit(True))


def example_silver_enrichment(df: DataFrame) -> DataFrame:
    """
    Example silver zone enrichment
    
    Args:
        df: Input DataFrame
        table_config: SourceConfig or DimensionConfig
        zone_config: ZoneConfig
        
    Returns:
        Transformed DataFrame
    """
    return df.withColumn('_enriched', sf.current_timestamp())


# ============================================================================
# Transformation Registry
# ============================================================================

ZONE_TRANSFORMERS: Dict[str, Callable[[DataFrame, object, object], DataFrame]] = {
    # Example transformations
    'example_bronze_validation': example_bronze_validation,
    'example_silver_enrichment': example_silver_enrichment,
    
    # Add your custom transformations here:
    # 'transform_shipment': transform_shipment,
    # 'transform_facility': transform_facility,
    # etc.
}


def get_zone_transformer(name: str) -> Callable:
    """
    Get zone transformation function by name
    
    Args:
        name: Name of transformation
        
    Returns:
        Transformation function
        
    Raises:
        ValueError: If transformation not found
    """
    if name not in ZONE_TRANSFORMERS:
        raise ValueError(
            f"Zone transformation '{name}' not found. "
            f"Available: {list(ZONE_TRANSFORMERS.keys())}"
        )
    return ZONE_TRANSFORMERS[name]


def has_zone_transformer(name: str) -> bool:
    """Check if a zone transformer exists"""
    return name in ZONE_TRANSFORMERS


def register_zone_transformer(name_or_func=None, func=None):
    """
    Register a custom zone transformer
    
    Can be used as a decorator or a regular function:
    
    As a decorator with name:
        @register_zone_transformer('my_transform')
        def my_transform(df):
            return df
    
    As a regular function:
        register_zone_transformer('my_transform', my_transform_func)
    
    Args:
        name_or_func: Either the name (str) or the function itself
        func: The transformation function (when name is provided)
    """
    def _register(name: str, transform_func: Callable):
        """Internal registration logic"""
        if name in ZONE_TRANSFORMERS:
            logger.warning(f"Overwriting existing transformer: {name}")
        
        ZONE_TRANSFORMERS[name] = transform_func
        logger.info(f"Registered zone transformer: {name}")
        return transform_func
    
    # Case 1: Used as @register_zone_transformer('name')
    if isinstance(name_or_func, str):
        def decorator(f):
            return _register(name_or_func, f)
        return decorator
    
    # Case 2: Used as register_zone_transformer('name', func)
    elif isinstance(name_or_func, str) and func is not None:
        return _register(name_or_func, func)
    
    # Case 3: Used as @register_zone_transformer (no args - use function name)
    elif callable(name_or_func):
        return _register(name_or_func.__name__, name_or_func)
    
    else:
        raise ValueError(
            "register_zone_transformer must be called with a name and function, "
            "or used as a decorator"
        )

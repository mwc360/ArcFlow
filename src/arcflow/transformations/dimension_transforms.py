"""
Dimension transformations - combine multiple tables into dimensional models

These transformations read from multiple source tables and create:
- Enriched dimensions (denormalized)
- Fact tables (aggregated metrics)
- Bridge tables (many-to-many relationships)

Note: Source table reading is now handled by DimensionPipeline using catalog references.
Dimension transformers receive pre-loaded DataFrames as a dictionary.
"""
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
from typing import Dict, Callable
import logging


logger = logging.getLogger(__name__)


# ============================================================================
# Example Dimension Builders
# ============================================================================

def example_dimension_join(
    sources: Dict[str, DataFrame],
    dimension_config
) -> DataFrame:
    """
    Example dimension builder - joins two tables
    
    Args:
        sources: Dict of source DataFrames
        dimension_config: DimensionConfig instance
        
    Returns:
        Enriched DataFrame
    """
    # Example: join table1 and table2
    if len(sources) < 2:
        raise ValueError("example_dimension_join requires at least 2 source tables")
    
    table_names = list(sources.keys())
    df1 = sources[table_names[0]]
    df2 = sources[table_names[1]]
    
    # Simple join example (customize based on your needs)
    result = df1.join(df2, on='id', how='left')
    
    return result


def example_fact_aggregation(
    sources: Dict[str, DataFrame],
    dimension_config
) -> DataFrame:
    """
    Example fact table builder - aggregates metrics
    
    Args:
        sources: Dict of source DataFrames
        dimension_config: DimensionConfig instance
        
    Returns:
        Aggregated DataFrame
    """
    if len(sources) == 0:
        raise ValueError("example_fact_aggregation requires at least 1 source table")
    
    df = sources[list(sources.keys())[0]]
    
    # Example aggregation (customize based on your needs)
    result = (df
        .groupBy('date', 'category')
        .agg(
            sf.count('*').alias('record_count'),
            sf.sum('amount').alias('total_amount'),
            sf.avg('value').alias('avg_value')
        )
    )
    
    return result


# ============================================================================
# Dimension Transformer Registry
# ============================================================================

DIMENSION_TRANSFORMERS: Dict[str, Callable[[Dict[str, DataFrame], object], DataFrame]] = {
    # Example transformers
    'example_dimension_join': example_dimension_join,
    'example_fact_aggregation': example_fact_aggregation,
    
    # Add your custom dimension builders here:
    # 'build_dim_shipment_enriched': build_dim_shipment_enriched,
    # 'build_fact_daily_performance': build_fact_daily_performance,
    # etc.
}


def get_dimension_transformer(name: str) -> Callable:
    """
    Get dimension transformation function by name
    
    Args:
        name: Name of transformation
        
    Returns:
        Transformation function
        
    Raises:
        ValueError: If transformation not found
    """
    if name not in DIMENSION_TRANSFORMERS:
        raise ValueError(
            f"Dimension transformer '{name}' not found. "
            f"Available: {list(DIMENSION_TRANSFORMERS.keys())}"
        )
    return DIMENSION_TRANSFORMERS[name]


def has_dimension_transformer(name: str) -> bool:
    """Check if a dimension transformer exists"""
    return name in DIMENSION_TRANSFORMERS


def register_dimension_transformer(
    name: str,
    func: Callable[[Dict[str, DataFrame], object], DataFrame]
):
    """
    Register a custom dimension transformer
    
    Args:
        name: Name of the transformer
        func: Transformation function
    """
    if name in DIMENSION_TRANSFORMERS:
        logger.warning(f"Overwriting existing transformer: {name}")
    
    DIMENSION_TRANSFORMERS[name] = func
    logger.info(f"Registered dimension transformer: {name}")

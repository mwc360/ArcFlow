"""
Spark session factory with arcflow optimizations
"""
from pyspark.sql import SparkSession
from typing import Dict, Any
import logging


def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    Create or get Spark session with arcflow configurations
    
    Args:
        config: Configuration dictionary with optional keys:
            - app_name: Application name
            - spark_configs: Dict of Spark configurations
            - delta_configs: Enable Delta Lake optimizations
    
    Returns:
        SparkSession instance
    """
    logger = logging.getLogger(__name__)
    
    app_name = config.get('app_name', 'ArcFlow')
    spark_configs = config.get('spark_configs', {})
    delta_configs = config.get('delta_configs', True)
    
    logger.info(f"Creating Spark session: {app_name}")
    
    # Start with builder
    builder = SparkSession.builder.appName(app_name)
    
    # Apply custom Spark configurations
    for key, value in spark_configs.items():
        builder = builder.config(key, value)
    
    # Apply Delta Lake configurations if enabled
    if delta_configs:
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        builder = builder.config("spark.databricks.delta.optimizeWrite.enabled", "true")
        builder = builder.config("spark.databricks.delta.autoCompact.enabled", "true")
    
    # Get or create session
    spark = builder.getOrCreate()
    
    logger.info(f"Spark session created: {spark.version}")
    logger.info(f"Delta Lake enabled: {delta_configs}")
    
    return spark


def get_spark_session() -> SparkSession:
    """
    Get existing Spark session
    
    Returns:
        SparkSession instance
    
    Raises:
        RuntimeError: If no active Spark session exists
    """
    try:
        return SparkSession.getActiveSession()
    except Exception as e:
        raise RuntimeError(
            "No active Spark session found. "
            "Create one using create_spark_session() first."
        ) from e

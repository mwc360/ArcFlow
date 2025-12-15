"""
Writer factory pattern for arcflow framework
"""
from pyspark.sql import SparkSession
from typing import Dict, Any
from .base_writer import BaseWriter
from .delta_writer import DeltaWriter


class WriterFactory:
    """
    Factory to create appropriate writer
    
    Currently supports Delta Lake format only
    """
    
    def __init__(
        self,
        spark: SparkSession,
        is_streaming: bool,
        config: Dict[str, Any]
    ):
        """
        Initialize writer factory
        
        Args:
            spark: SparkSession instance
            is_streaming: True for streaming, False for batch
            config: Global configuration dict
        """
        self.spark = spark
        self.is_streaming = is_streaming
        self.config = config
    
    def create_writer(self, table_config=None, zone_config=None) -> BaseWriter:
        """
        Create Delta writer
        
        Args:
            table_config: Optional SourceConfig or DimensionConfig
            zone_config: Optional ZoneConfig
            
        Returns:
            BaseWriter instance
        """
        # For now, always use DeltaWriter
        # Can be extended to support other formats
        return DeltaWriter(self.spark, self.is_streaming, self.config)
    
    def create_writer_for_dimension(self, dimension_config) -> BaseWriter:
        """
        Create writer for dimensional table
        
        Args:
            dimension_config: DimensionConfig instance
            
        Returns:
            BaseWriter instance
        """
        return DeltaWriter(self.spark, self.is_streaming, self.config)

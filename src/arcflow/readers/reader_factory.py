"""
Reader factory pattern for arcflow framework
"""
from pyspark.sql import SparkSession
from typing import Dict, Any
from .base_reader import BaseReader
from .parquet_reader import ParquetReader
from .json_reader import JsonReader
from .kafka_reader import KafkaReader
from .eventhub_reader import EventHubReader


class ReaderFactory:
    """
    Factory to create appropriate reader based on source format
    """
    
    def __init__(
        self,
        spark: SparkSession,
        is_streaming: bool,
        config: Dict[str, Any]
    ):
        """
        Initialize reader factory
        
        Args:
            spark: SparkSession instance
            is_streaming: True for streaming, False for batch
            config: Global configuration dict
        """
        self.spark = spark
        self.is_streaming = is_streaming
        self.config = config
    
    def create_reader(self, table_config) -> BaseReader:
        """
        Create appropriate reader based on source format
        
        Args:
            table_config: FlowConfig instance
            
        Returns:
            BaseReader subclass instance
            
        Raises:
            ValueError: If format is not supported
        """
        readers = {
            'parquet': ParquetReader,
            'json': JsonReader,
            'kafka': KafkaReader,
            'eventhub': EventHubReader,
            # 'csv': CsvReader,  # Can be added later
        }
        
        reader_class = readers.get(table_config.format)
        
        if not reader_class:
            raise ValueError(
                f"Unsupported format: {table_config.format}. "
                f"Supported formats: {list(readers.keys())}"
            )
        
        return reader_class(self.spark, self.is_streaming, self.config)

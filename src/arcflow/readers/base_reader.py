"""
Base reader interface for arcflow framework
"""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
from ..config import Defaults
import posixpath


class BaseReader(ABC):
    """
    Abstract base class for all readers
    
    Supports both batch and streaming reads with a simple toggle
    """
    
    def __init__(
        self, 
        spark: SparkSession, 
        is_streaming: bool,
        config: Dict[str, Any]
    ):
        """
        Initialize reader
        
        Args:
            spark: SparkSession instance
            is_streaming: True for streaming, False for batch
            config: Global configuration dict
        """
        self.spark = spark
        self.is_streaming = is_streaming
        self.config = config
    
    @abstractmethod
    def read(self, table_config) -> DataFrame:
        """
        Read data from source
        
        Args:
            table_config: SourceConfig instance
            
        Returns:
            DataFrame
        """
        pass
    
    def get_reader(self):
        """
        Get appropriate Spark reader (stream or batch)
        
        Returns:
            DataStreamReader or DataFrameReader
        """
        return self.spark.readStream if self.is_streaming else self.spark.read
    
    def get_landing_path(self, table_name: str) -> str:
        """
        Get landing zone path for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Full path to landing zone table
        """
        landing_base = self.config.get('landing_uri', Defaults.LANDING_URI)
        return posixpath.join(landing_base, table_name)

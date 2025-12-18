"""
Base writer interface for arcflow framework
"""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from typing import Optional, Dict, Any
from ..config import Defaults
import posixpath


class BaseWriter(ABC):
    """
    Abstract base class for all writers
    
    Supports both batch and streaming writes with a simple toggle
    """
    
    def __init__(
        self,
        spark: SparkSession,
        is_streaming: bool,
        config: Dict[str, Any]
    ):
        """
        Initialize writer
        
        Args:
            spark: SparkSession instance
            is_streaming: True for streaming, False for batch
            config: Global configuration dict
        """
        self.spark = spark
        self.is_streaming = is_streaming
        self.config = config
    
    @abstractmethod
    def write(
        self,
        df: DataFrame,
        table_config,
        zone_config,
        zone: str
    ) -> Optional[StreamingQuery]:
        """
        Write DataFrame to target
        
        Args:
            df: DataFrame to write
            table_config: FlowConfig or DimensionConfig
            zone_config: StageConfig instance
            zone: Target zone name
            
        Returns:
            StreamingQuery if streaming, None if batch
        """
        pass
    
    def get_checkpoint_path(self, zone: str, table_name: str) -> str:
        """
        Get checkpoint path for a streaming query
        
        Args:
            zone: Zone name
            table_name: Name of the table
            
        Returns:
            Full checkpoint path
        """
        checkpoint_base = self.config.get('checkpoint_uri', Defaults.CHECKPOINT_URI)
        return posixpath.join(checkpoint_base, zone, table_name)

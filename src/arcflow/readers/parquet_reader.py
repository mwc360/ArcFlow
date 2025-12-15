"""
Parquet file reader for arcflow framework
"""
from pyspark.sql import DataFrame
from .base_reader import BaseReader
from ..config import Defaults
import posixpath


class ParquetReader(BaseReader):
    """
    Parquet file reader supporting both batch and streaming
    """
    
    def read(self, table_config) -> DataFrame:
        """
        Read Parquet files from landing zone
        
        Args:
            table_config: SourceConfig instance
            
        Returns:
            DataFrame
        """
        path = table_config.source_uri or self.get_landing_path(table_config.name)
        
        reader = self.get_reader()
        
        # Apply schema
        df = reader.schema(table_config.schema)
        
        # Handle cleanSource/archive
        if self.is_streaming and table_config.clean_source:
            archive_base = self.config.get('archive_uri', Defaults.ARCHIVE_URI)
            
            archive_dir = posixpath.join(archive_base, table_config.name)
            
            df = df.option('cleanSource', 'archive')
            df = df.option('sourceArchiveDir', archive_dir)
        
        # Apply any custom reader options
        for key, value in table_config.reader_options.items():
            df = df.option(key, value)
        
        # Read parquet
        df = df.parquet(path)
        
        return df

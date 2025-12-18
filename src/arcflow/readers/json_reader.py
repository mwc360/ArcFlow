"""
JSON file reader for arcflow framework
"""
from pyspark.sql import DataFrame
from .base_reader import BaseReader
from ..config import Defaults
import posixpath


class JsonReader(BaseReader):
    """
    JSON file reader with support for:
    - Multi-line JSON
    - Array exploding
    - Archive on read (cleanSource)
    """
    
    def read(self, table_config) -> DataFrame:
        """
        Read JSON files from landing zone
        
        Args:
            table_config: FlowConfig instance
            
        Returns:
            DataFrame
        """
        path = table_config.source_uri or self.get_landing_path(table_config.name)
        
        reader = self.get_reader()
        
        # Apply schema
        df = reader.schema(table_config.schema)
        
        # JSON-specific options
        df = df.option('multiline', 'true')
        
        # Handle cleanSource/archive
        if self.is_streaming and table_config.clean_source:
            archive_base = self.config.get('archive_uri', Defaults.ARCHIVE_URI)
            
            archive_dir = posixpath.join(archive_base, table_config.name)
            
            df = df.option('cleanSource', 'archive')
            df = df.option('sourceArchiveDir', archive_dir)
        
        # Apply any custom reader options
        for key, value in table_config.reader_options.items():
            df = df.option(key, value)
        
        # Read JSON
        df = df.json(path)
        
        # Handle array explosion if configured
        if table_config.explode_column:
            alias = table_config.explode_alias or f"{table_config.explode_column}_item"
            
            # Keep metadata and explode array
            df = df.selectExpr(
                "_metadata",
                f"explode({table_config.explode_column}) as {alias}"
            )
        
        return df

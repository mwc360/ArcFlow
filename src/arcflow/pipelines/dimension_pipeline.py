"""
Dimensional pipeline for multi-source dimensional modeling

Handles:
- Multi-source table joins (fact tables, dimensions, bridge tables)
- Stream-stream joins or batch joins
- Custom dimension builders
"""
import logging
from typing import List, Optional, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from ..models import DimensionConfig, SourceConfig
from ..transformations.common import (
    apply_processing_timestamp,
    add_zone_metadata
)
from ..transformations.dimension_transforms import (
    get_dimension_transformer,
    has_dimension_transformer
)
from ..writers.writer_factory import WriterFactory
from ..utils.table_utils import build_table_reference


class DimensionPipeline:
    """
    Multi-source dimensional pipeline
    
    Combines multiple source tables to create:
    - Fact tables (transactions, events)
    - Dimension tables (customers, products, locations)
    - Bridge tables (many-to-many relationships)
    """
    
    def __init__(
        self,
        spark: SparkSession,
        zone: str,
        config: dict
    ):
        """
        Initialize dimensional pipeline
        
        Args:
            spark: SparkSession
            zone: Target zone for dimensional table
            config: Pipeline configuration
        """
        self.spark = spark
        self.zone = zone
        self.config = config
        self.is_streaming = config.get('streaming_enabled', True)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(
            f"Initialized dimension pipeline for {zone} "
            f"(streaming: {self.is_streaming})"
        )
    
    def read_source_tables_for_dimension(
        self,
        dimension_config: DimensionConfig,
        table_registry: Dict[str, SourceConfig]
    ) -> Dict[str, DataFrame]:
        """
        Read all source tables required for this dimension
        
        Args:
            dimension_config: DimensionConfig instance
            table_registry: Dict mapping table names to SourceConfig
            
        Returns:
            Dict of table_name -> DataFrame
        """
        self.logger.info(
            f"Reading {len(dimension_config.source_tables)} source tables "
            f"for {dimension_config.name}"
        )
        
        dataframes = {}
        for source_def in dimension_config.source_tables:
            table_name = source_def['table']
            source_zone = source_def['zone']
            
            # Build catalog reference for source table
            catalog_name = self.config.get('catalog_name')
            schema_name = self.config.get('schema_name', source_zone)
            table_ref = build_table_reference(catalog_name, schema_name, table_name)
            
            self.logger.info(f"Reading {table_name} from {source_zone} using reference: {table_ref}")
            
            # Read as stream or batch based on mode
            if self.is_streaming:
                df = self.spark.readStream.format('delta').table(table_ref)
            else:
                df = self.spark.read.format('delta').table(table_ref)
            
            # Store with alias if provided, otherwise use table name
            alias = source_def.get('alias', table_name)
            dataframes[alias] = df
        
        return dataframes
    
    def apply_dimension_builder(
        self,
        source_tables: Dict[str, DataFrame],
        dimension_config: DimensionConfig
    ) -> DataFrame:
        """
        Apply custom dimension builder transformation
        
        Args:
            source_tables: Dict of table_name -> DataFrame
            dimension_config: DimensionConfig instance
            
        Returns:
            Dimensional DataFrame
        """
        builder_name = dimension_config.builder_transform
        
        if not has_dimension_transformer(builder_name):
            raise ValueError(
                f"Dimension builder '{builder_name}' not found. "
                f"Available: {list(dimension_config.available_builders())}"
            )
        
        self.logger.info(f"Applying dimension builder: {builder_name}")
        builder = get_dimension_transformer(builder_name)
        
        # Call builder with source tables and config
        df = builder(source_tables, dimension_config)
        
        return df
    
    def apply_metadata(
        self,
        df: DataFrame,
        dimension_config: DimensionConfig
    ) -> DataFrame:
        """
        Add zone metadata to dimensional table
        
        Args:
            df: Input DataFrame
            dimension_config: DimensionConfig instance
            
        Returns:
            DataFrame with metadata
        """
        df = add_zone_metadata(df, self.zone, dimension_config.name)
        df = apply_processing_timestamp(df)
        return df
    
    def write_dimension(
        self,
        df: DataFrame,
        dimension_config: DimensionConfig
    ) -> Optional[StreamingQuery]:
        """
        Write dimensional table to target zone
        
        Args:
            df: Dimensional DataFrame
            dimension_config: DimensionConfig instance
            
        Returns:
            StreamingQuery if streaming, None if batch
        """
        writer_factory = WriterFactory(self.spark, self.is_streaming, self.config)
        writer = writer_factory.create_writer_for_dimension(dimension_config)
        return writer.write_dimension(df, dimension_config, self.zone)
    
    def process_dimension(
        self,
        dimension_config: DimensionConfig,
        table_registry: Dict[str, SourceConfig]
    ) -> Optional[StreamingQuery]:
        """
        Full pipeline for one dimensional table
        
        Args:
            dimension_config: DimensionConfig instance
            table_registry: Dict mapping table names to SourceConfig
            
        Returns:
            StreamingQuery if streaming, None if batch
        """
        if not dimension_config.enabled:
            self.logger.info(f"Dimension {dimension_config.name} not enabled")
            return None
        
        self.logger.info(f"Processing dimension: {dimension_config.name}")
        
        try:
            # Read source tables
            source_tables = self.read_source_tables_for_dimension(
                dimension_config,
                table_registry
            )
            
            # Apply dimension builder (joins, aggregations, etc.)
            df = self.apply_dimension_builder(source_tables, dimension_config)
            
            # Add metadata
            df = self.apply_metadata(df, dimension_config)
            
            # Write
            query = self.write_dimension(df, dimension_config)
            
            self.logger.info(f"Successfully set up dimension pipeline for {dimension_config.name}")
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to process dimension {dimension_config.name}: {e}")
            raise
    
    def process_all(
        self,
        dimension_configs: List[DimensionConfig],
        table_registry: Dict[str, SourceConfig]
    ) -> List[StreamingQuery]:
        """
        Process all dimensional tables
        
        Args:
            dimension_configs: List of DimensionConfig instances
            table_registry: Dict mapping table names to SourceConfig
            
        Returns:
            List of StreamingQuery instances
        """
        queries = []
        for config in dimension_configs:
            query = self.process_dimension(config, table_registry)
            if query:
                queries.append(query)
        return queries

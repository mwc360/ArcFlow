"""
Zone-agnostic pipeline - works for any zone (bronze, silver, gold, etc.)

Replaces separate BronzePipeline, SilverPipeline, GoldPipeline with one flexible class
"""
import logging
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from ..models import FlowConfig, StageConfig
from ..transformations.common import (
    normalize_columns_to_snake_case,
    apply_processing_timestamp
)
from ..transformations.zone_transforms import (
    get_zone_transformer,
    has_zone_transformer
)
from ..readers.reader_factory import ReaderFactory
from ..writers.writer_factory import WriterFactory
from ..utils.table_utils import build_table_reference


class ZonePipeline:
    """
    Zone-agnostic pipeline that works for any zone
    
    Handles:
    - Single-source processing (1 input -> 1 output)
    - Zone-to-zone processing (reading from previous zone)
    - Universal and custom transformations
    - Batch and streaming modes
    """
    
    def __init__(
        self,
        spark: SparkSession,
        zone: str,
        config: dict
    ):
        """
        Initialize pipeline for a specific zone
        
        Args:
            spark: SparkSession
            zone: Target zone name (e.g., 'bronze', 'silver', 'gold')
            config: Pipeline configuration
        
        Note:
            source_zone is calculated dynamically per table using _get_source_zone()
        """
        self.spark = spark
        self.zone = zone
        self.config = config
        self.is_streaming = config.get('streaming_enabled', True)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(
            f"Initialized {zone} pipeline (streaming: {self.is_streaming})"
        )

    def _get_source_zone(self, table_config: FlowConfig) -> Optional[str]:
        """
        Calculate the source zone for a given target zone.
        Returns the previous zone in the zones dict, or None if it's the first zone.

        Args:
            table_config: FlowConfig with zones defined
            current_zone: The target zone name (e.g., 'silver')

        Returns:
            Name of source zone (e.g., 'bronze') or None if reading from landing

        Example:
            zones = {'bronze': ..., 'silver': ..., 'gold': ...}
            get_source_zone(config, 'silver')  # Returns 'bronze'
            get_source_zone(config, 'bronze')  # Returns None (read from landing)
        """
        zone_names = list(table_config.zones.keys())

        if self.zone not in zone_names:
            raise ValueError(f"Zone '{self.zone}' not found in table config. Available: {zone_names}")

        current_index = zone_names.index(self.zone)

        if current_index == 0:
            # First zone - read from landing
            return None
        else:
            # Return previous zone
            return zone_names[current_index - 1]
        
    def _get_catalog(self, table_config: FlowConfig, zone) -> Optional[str]:
        """
        Get catalog name for a given zone from table config
        
        Args:
            table_config: FlowConfig instance
            zone: Zone name
        """
        zone_config = table_config.get_zone_config(zone)
        if zone_config and hasattr(zone_config, 'catalog_name'):
            return zone_config.catalog_name
        return None
    
    def _get_schema(self, table_config: FlowConfig, zone) -> Optional[str]:
        """
        Get schema name for a given zone from table config
        
        Args:
            table_config: FlowConfig instance
            zone: Zone name
        """
        zone_config = table_config.get_zone_config(zone)
        if zone_config and hasattr(zone_config, 'schema_name'):
            return zone_config.schema_name
        return zone
    
    def read_source(self, table_config: FlowConfig) -> DataFrame:
        """
        Read from source - either landing zone or previous zone
        
        Args:
            table_config: FlowConfig instance
            
        Returns:
            DataFrame
        """
        source_zone = self._get_source_zone(table_config)
        source_catalog = self._get_catalog(table_config, source_zone)
        source_schema = self._get_schema(table_config, source_zone)
        table_reference = build_table_reference(source_catalog, source_schema, table_config.name)

        if source_zone:
            # Read from previous zone (e.g., silver reads from bronze)
            self.logger.info(f"Reading {table_config.name} from {source_zone} zone")
            
            if self.is_streaming:
                df = self.spark.readStream.format('delta').table(table_reference)
            else:
                df = self.spark.read.format('delta').table(table_reference)
        else:
            # Read from landing (raw files)
            self.logger.info(f"Reading {table_config.name} from landing")
            reader_factory = ReaderFactory(self.spark, self.is_streaming, self.config)
            reader = reader_factory.create_reader(table_config)
            df = reader.read(table_config)
        
        return df
    
    def apply_transformations(
        self,
        df: DataFrame,
        table_config: FlowConfig,
        zone_config: StageConfig
    ) -> DataFrame:
        """
        Apply zone-specific transformations
        
        1. Universal transformations (all tables, all zones)
        2. Custom transformations (table-specific, zone-specific)
        
        Args:
            df: Input DataFrame
            table_config: FlowConfig instance
            zone_config: StageConfig instance
            
        Returns:
            Transformed DataFrame
        """
        # Custom table-specific, zone-specific transformations
        if zone_config.custom_transform:
            if has_zone_transformer(zone_config.custom_transform):
                self.logger.info(f"Applying custom transformation: {zone_config.custom_transform}")
                transformer = get_zone_transformer(zone_config.custom_transform)
                df = transformer(df)
            else:
                self.logger.warning(
                    f"Transformation '{zone_config.custom_transform}' not found "
                    f"for {table_config.name} in {self.zone} zone. Skipping."
                )

        # Universal transformations for all zones
        self.logger.info("Applying snake_case normalization")
        df = normalize_columns_to_snake_case(df)
        
        # Add zone metadata
        # df = add_zone_metadata(df, self.zone, table_config.name)
        df = apply_processing_timestamp(df)
        
        
        return df
    
    def test_input(self, table_config: FlowConfig) -> DataFrame:
        """
        Tests the input of the zone pipeline without writing.
        Returns a batch DataFrame for testing (even if pipeline is configured for streaming).
        
        Args:
            table_config: FlowConfig instance
            
        Returns:
            DataFrame (batch mode)
        """
        # Get zone-specific configuration
        zone_config = table_config.get_zone_config(self.zone)
        if not zone_config:
            self.logger.info(f"Table {table_config.name} not configured for {self.zone} zone")
            return None
        
        self.logger.info(f"Testing {table_config.name} input for {self.zone} zone")

        # Temporarily save streaming state
        original_streaming = self.is_streaming
        self.is_streaming = False
        
        try:
            # Read
            df = self.read_source(table_config)
            
            self.logger.info(f"Successfully generated test input for {table_config.name}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to test {table_config.name} input: {e}")
            raise
        finally:
            # Restore original streaming state
            self.is_streaming = original_streaming
    
    def test_output(self, table_config: FlowConfig) -> DataFrame:
        """
        Tests the output of the zone pipeline without writing.
        Returns a batch DataFrame for testing (even if pipeline is configured for streaming).
        
        Args:
            table_config: FlowConfig instance
            
        Returns:
            DataFrame (batch mode)
        """
        # Get zone-specific configuration
        zone_config = table_config.get_zone_config(self.zone)
        if not zone_config:
            self.logger.info(f"Table {table_config.name} not configured for {self.zone} zone")
            return None
        
        self.logger.info(f"Testing {table_config.name} output for {self.zone} zone")

        # Temporarily save streaming state
        original_streaming = self.is_streaming
        self.is_streaming = False
        
        try:
            # Read
            df = self.read_source(table_config)
            
            # Transform
            df = self.apply_transformations(df, table_config, zone_config)
            
            self.logger.info(f"Successfully generated test output for {table_config.name}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to test {table_config.name} output: {e}")
            raise
        finally:
            # Restore original streaming state
            self.is_streaming = original_streaming
    
    def write_target(
        self,
        df: DataFrame,
        table_config: FlowConfig,
        zone_config: StageConfig
    ) -> Optional[StreamingQuery]:
        """
        Write to target zone
        
        Args:
            df: DataFrame to write
            table_config: FlowConfig instance
            zone_config: StageConfig instance
            
        Returns:
            StreamingQuery if streaming, None if batch
        """
        writer_factory = WriterFactory(self.spark, self.is_streaming, self.config)
        writer = writer_factory.create_writer(table_config, zone_config)
        return writer.write(df, table_config, zone_config, self.zone)
    
    def process_table(self, table_config: FlowConfig) -> Optional[StreamingQuery]:
        """
        Full pipeline for one table in this zone
        
        Args:
            table_config: FlowConfig instance
            
        Returns:
            StreamingQuery if streaming, None if batch
        """
        # Get zone-specific configuration
        zone_config = table_config.get_zone_config(self.zone)
        if not zone_config or not zone_config.enabled:
            self.logger.info(f"Table {table_config.name} not enabled for {self.zone} zone")
            return None
        
        self.logger.info(f"Processing {table_config.name} for {self.zone} zone")
        
        try:
            # Read
            df = self.read_source(table_config)
            
            # Transform
            df = self.apply_transformations(df, table_config, zone_config)
            
            # Write
            query = self.write_target(df, table_config, zone_config)
            
            self.logger.info(f"Successfully set up pipeline for {table_config.name}")
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to process {table_config.name}: {e}")
            raise
    
    def process_all(self, table_configs: List[FlowConfig]) -> List[StreamingQuery]:
        """
        Process all tables for this zone
        
        Args:
            table_configs: List of FlowConfig instances
            
        Returns:
            List of StreamingQuery instances
        """
        queries = []
        for config in table_configs:
            query = self.process_table(config)
            if query:
                queries.append(query)
        return queries

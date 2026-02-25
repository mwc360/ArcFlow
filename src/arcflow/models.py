"""
Configuration models for arcflow framework

Zone-agnostic design supporting any lakehouse architecture
"""
from dataclasses import dataclass, field
from typing import Optional, Literal, Dict, Any, List
from pyspark.sql.types import StructType


@dataclass
class StageConfig:
    """
    Configuration for processing in a specific stage/zone
    
    Stage examples: landing -> bronze -> silver -> gold, or any custom stages/zones
    """
    mode: Literal['append', 'upsert'] = 'append'
    merge_keys: Optional[List[str]] = None  # Required if mode='upsert'
    partition_by: Optional[List[str]] = None
    custom_transform: Optional[str] = None  # Name of custom transformation function
    enabled: bool = True


@dataclass
class FlowConfig:
    """
    Complete configuration for a source table across all stages/zones
    
    Zone-agnostic design allows flexibility in pipeline architecture.
    Each table defines behavior per zone (bronze, silver, gold, etc.)
    """
    
    # Identifiers
    name: str
    schema: StructType
    
    # Source Configuration
    format: Literal['parquet', 'json', 'csv', 'kafka', 'eventhub'] = 'parquet'
    source_uri: Optional[str] = None
    
    # Zone Configurations - flexible for any zone architecture
    zones: Dict[str, StageConfig] = field(default_factory=dict)
    
    # Streaming Configuration
    trigger_mode: Literal['availableNow', 'processingTime', 'continuous'] = 'availableNow'
    trigger_interval: Optional[str] = None
    
    # Source Options
    clean_source: bool = False
    explode_column: Optional[str] = None
    explode_alias: Optional[str] = None
    
    # Additional Options
    reader_options: Dict[str, Any] = field(default_factory=dict)
    writer_options: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    owner: Optional[str] = None
    
    def get_zone_config(self, zone: str) -> Optional[StageConfig]:
        """Get configuration for a specific zone"""
        return self.zones.get(zone)
    
    def is_enabled_for_zone(self, zone: str) -> bool:
        """Check if table is enabled for a specific zone"""
        zone_config = self.get_zone_config(zone)
        return zone_config is not None and zone_config.enabled


@dataclass
class DimensionConfig:
    """
    Configuration for dimensional/fact tables built from multiple sources
    
    Unlike FlowConfig (1 source -> 1 table), DimensionConfig combines multiple sources
    to create enriched dimensions, facts, and bridge tables
    """
    
    # Identifiers
    name: str  # e.g., 'dim_shipment_enriched', 'fact_daily_performance'
    dimension_type: Literal['dimension', 'fact', 'bridge']
    
    # Source tables this dimension depends on
    source_tables: List[str]  # e.g., ['shipment', 'facility', 'service_level']
    source_zone: str  # Which zone to read from (e.g., 'silver')
    
    # Target zone configuration
    target_zone: str  # Where to write (e.g., 'gold')
    zone_config: StageConfig  # Mode, merge_keys, partitioning
    
    # Custom transformation
    transform: str  # Name of dimension builder function
    
    # Streaming config
    trigger_mode: Literal['availableNow', 'processingTime', 'continuous'] = 'availableNow'
    trigger_interval: Optional[str] = None
    
    # Watermarking for stream-stream joins
    watermark_column: Optional[str] = None
    watermark_delay: Optional[str] = None  # e.g., "10 minutes"
    
    # SCD Type (for dimensions)
    scd_type: Optional[Literal['type1', 'type2', 'type3']] = None
    scd_key_columns: Optional[List[str]] = None
    
    # Metadata
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    owner: Optional[str] = None

"""
ArcFlow Orchestrator

Coordinates all pipelines:
- Zone pipelines (single-source processing)
- Dimension pipelines (multi-source modeling)
- Stream lifecycle management
"""
import logging
from typing import List, Dict, Optional
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from .models import FlowConfig, DimensionConfig
from .core.stream_manager import StreamManager
from .pipelines.zone_pipeline import ZonePipeline
from .pipelines.dimension_pipeline import DimensionPipeline


class Controller:
    """
    Main controller for arcflow ELT framework
    
    Responsibilities:
    - Initialize zone pipelines in order (bronze -> silver -> gold)
    - Initialize dimension pipelines
    - Manage streaming queries
    - Handle graceful shutdown
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        table_registry: Dict[str, FlowConfig],
        dimension_registry: Optional[Dict[str, DimensionConfig]] = None
    ):
        """
        Initialize controller
        
        Args:
            spark: SparkSession
            config: Global pipeline configuration
            table_registry: Dict of table_name -> FlowConfig
            dimension_registry: Optional dict of dimension_name -> DimensionConfig
        """
        self.spark = spark
        self.config = config
        self.table_registry = table_registry
        self.dimension_registry = dimension_registry or {}
        self.is_streaming = config.get('streaming_enabled', True)
        
        # Stream lifecycle manager
        self.stream_manager = StreamManager()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initialized ArcFlowOrchestrator")
    
    def run_zone_pipeline(
        self,
        zone: str,
        source_zone: Optional[str] = None,
        table_subset: Optional[List[str]] = None
    ) -> List[StreamingQuery]:
        """
        Run pipeline for a specific zone
        
        Args:
            zone: Target zone (e.g., 'bronze', 'silver', 'gold')
            source_zone: Source zone to read from (None = landing)
            table_subset: Optional list of table names to process (None = all)
            
        Returns:
            List of StreamingQuery instances
        """
        self.logger.info(f"Starting {zone} pipeline (source: {source_zone or 'landing'})")
        
        # Initialize zone pipeline (source_zone calculated automatically per table)
        pipeline = ZonePipeline(
            spark=self.spark,
            zone=zone,
            config=self.config
        )
        
        # Filter tables
        if table_subset:
            configs = [self.table_registry[name] for name in table_subset]
        else:
            # Process all tables that have this zone configured
            configs = [
                config for config in self.table_registry.values()
                if config.is_enabled_for_zone(zone)
            ]
        
        self.logger.info(f"Processing {len(configs)} tables for {zone} zone")
        
        # Process all tables
        queries = pipeline.process_all(configs)
        
        # Register queries with stream manager
        for query in queries:
            self.stream_manager.register(query)
        
        self.logger.info(f"Started {len(queries)} streams for {zone} zone")
        return queries
    
    def run_dimension_pipeline(
        self,
        zone: str,
        dimension_subset: Optional[List[str]] = None
    ) -> List[StreamingQuery]:
        """
        Run dimension pipeline for multi-source dimensional tables
        
        Args:
            zone: Target zone for dimensional tables
            dimension_subset: Optional list of dimension names to process
            
        Returns:
            List of StreamingQuery instances
        """
        self.logger.info(f"Starting dimension pipeline for {zone} zone")
        
        # Initialize dimension pipeline
        pipeline = DimensionPipeline(
            spark=self.spark,
            zone=zone,
            config=self.config
        )
        
        # Filter dimensions
        if dimension_subset:
            configs = [self.dimension_registry[name] for name in dimension_subset]
        else:
            # Process all enabled dimensions
            configs = [
                config for config in self.dimension_registry.values()
                if config.enabled
            ]
        
        self.logger.info(f"Processing {len(configs)} dimensions for {zone} zone")
        
        # Process all dimensions
        queries = pipeline.process_all(configs, self.table_registry)
        
        # Register queries with stream manager
        for query in queries:
            self.stream_manager.register(query)
        
        self.logger.info(f"Started {len(queries)} dimension streams for {zone} zone")
        return queries
    
    def run_full_pipeline(
        self,
        zones: List[str] = None,
        include_dimensions: bool = True,
        await_termination: bool = None
    ):
        """
        Run complete ELT pipeline
        
        Default flow:
        1. Landing -> Bronze (raw ingestion)
        2. Bronze -> Silver (curation)
        3. Silver -> Gold (aggregations)
        4. Dimensions (multi-source modeling)
        
        Args:
            zones: List of zones to process (default: ['bronze', 'silver', 'gold'])
            include_dimensions: Whether to run dimension pipeline
            await_termination: Whether to block until all streams complete.
                - None (default): Auto-detect from config 'await_termination' (defaults to False for interactive use)
                - True: Block and wait (required for Spark Job Definitions with continuous streams)
                - False: Start streams and return immediately (better for notebooks)
                
        Note:
            In notebooks: Keep await_termination=False to avoid blocking the kernel
        """
        if zones is None:
            zones = ['bronze', 'silver', 'gold']
        
        self.logger.info(f"Starting full pipeline for zones: {zones}")
        
        # Process zones in order
        source_zone = None
        for zone in zones:
            self.run_zone_pipeline(zone, source_zone=source_zone)
            source_zone = zone  # Next zone reads from current zone
        
        # Process dimensions (typically in gold zone)
        if include_dimensions and self.dimension_registry:
            dimension_zone = zones[-1] if zones else 'gold'
            self.run_dimension_pipeline(dimension_zone)
        
        self.logger.info("Full pipeline started")
        
        # Determine if we should await termination
        if await_termination is None:
            await_termination = self.config.get('await_termination', False)
        
        # If streaming and await_termination is enabled, block until completion
        if self.is_streaming and await_termination:
            self.logger.info("Awaiting streaming termination (blocking)...")
            self.logger.info("Tip: Set await_termination=False for interactive notebooks")
            self.stream_manager.await_all()
        elif self.is_streaming:
            self.logger.info("Streams started (non-blocking). Use controller.stream_manager.await_all() to wait for completion.")
    
    def await_completion(self):
        """
        Block and wait for all streaming queries to complete.
        
        Useful for:
        - Spark Job Definitions with continuous/processingTime triggers
        - Waiting for availableNow triggers to finish in notebooks
        
        Not needed for:
        - Interactive notebook development (blocks the kernel)
        """
        if self.is_streaming:
            self.logger.info("Awaiting all streaming queries to complete...")
            self.stream_manager.await_all()
            self.logger.info("All streaming queries completed")
        else:
            self.logger.info("No streaming queries to await (batch mode)")
    
    def stop_all(self):
        """Stop all streaming queries gracefully"""
        self.logger.info("Stopping all streams...")
        self.stream_manager.stop_all()
        self.logger.info("All streams stopped")
    
    def get_status(self) -> Dict:
        """
        Get status of all streaming queries
        
        Returns:
            Dict with query statuses
        """
        return self.stream_manager.get_status()

"""
ArcFlow Orchestrator

Coordinates all pipelines:
- Zone pipelines (single-source processing)
- Dimension pipelines (multi-source modeling)
- Stream lifecycle management
"""
import logging
from typing import List, Dict, Optional, Tuple, Set
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from .models import FlowConfig, DimensionConfig, StageConfig
from .core.stream_manager import StreamManager
from .core.spark_configurator import SparkConfigurator
from .core.stage_chain_listener import StageChainListener
from .lock import JobLock
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
        self._chain_listener: Optional[StageChainListener] = None
        self._processed_stages: Set[Tuple[str, str]] = set()  # (flow_name, stage_name)
        
        # Job lock (opt-in via config)
        self._job_lock: Optional[JobLock] = None
        self._lock_held_by_full_pipeline = False
        if config.get('job_lock_enabled', False):
            job_id = config.get('job_id')
            if job_id:
                self._job_lock = JobLock(
                    job_id=job_id,
                    lock_path=config.get('job_lock_path', 'Files/locks/'),
                    timeout_seconds=config.get('job_lock_timeout_seconds', 3600),
                    poll_interval=config.get('job_lock_poll_interval'),
                    heartbeat_interval=config.get('job_lock_heartbeat_interval'),
                )
            else:
                self.logger.warning("job_lock_enabled=True but no job_id provided — lock disabled")
        
        self.logger = logging.getLogger(__name__)

        # Auto-apply best-practice Spark configs (opt-out via autoset_spark_configs=False)
        if config.get('autoset_spark_configs', True):
            overrides = config.get('spark_config_overrides')
            result = SparkConfigurator.apply(spark, overrides=overrides)
            self.logger.info(
                f"SparkConfigurator: {len(result['applied'])} configs applied, "
                f"{len(result['skipped'])} already set, "
                f"{len(result['unset'])} unset"
            )

        self.logger.info("Initialized ArcFlowOrchestrator")

    # ── Stage-input resolution & grouping ───────────────────────────

    @staticmethod
    def _resolve_stage_input(
        stage_name: str,
        stage_config: StageConfig,
        zones: Dict[str, StageConfig],
        flow_name: str,
    ) -> Tuple[str, Optional[str]]:
        """
        Resolve what a stage reads from.

        Returns:
            ('root', flow_name) or ('stage', source_stage_name)
        """
        if stage_config.stage_input is not None:
            if stage_config.stage_input == flow_name:
                return ('root', flow_name)
            if stage_config.stage_input in zones:
                return ('stage', stage_config.stage_input)
            raise ValueError(
                f"stage_input '{stage_config.stage_input}' for stage '{stage_name}' "
                f"is not a valid stage name or FlowConfig.name ('{flow_name}'). "
                f"Available stages: {list(zones.keys())}"
            )
        # Main chain: stages without stage_input, in dict order
        main_chain = [n for n, c in zones.items() if c.stage_input is None]
        idx = main_chain.index(stage_name)
        if idx == 0:
            return ('root', flow_name)
        return ('stage', main_chain[idx - 1])

    @staticmethod
    def _build_stage_groups(
        table_config: FlowConfig,
    ) -> List[List[Tuple[str, StageConfig]]]:
        """
        Group stages of a FlowConfig by their resolved input.

        Returns groups in dependency order (root-readers first, then
        stages that read from earlier stages).
        """
        zones = table_config.zones
        flow_name = table_config.name

        # Resolve all inputs
        resolved: Dict[str, Tuple[str, Optional[str]]] = {}
        for name, cfg in zones.items():
            if not cfg.enabled:
                continue
            resolved[name] = Controller._resolve_stage_input(
                name, cfg, zones, flow_name
            )

        # Group by resolved input
        groups_map: Dict[Tuple[str, Optional[str]], List[Tuple[str, StageConfig]]] = {}
        for name, res in resolved.items():
            groups_map.setdefault(res, []).append((name, zones[name]))

        # Order: root groups first, then by dependency depth
        root_groups = [g for key, g in groups_map.items() if key[0] == 'root']
        stage_groups = [g for key, g in groups_map.items() if key[0] == 'stage']
        return root_groups + stage_groups
    
    def run_zone_pipeline(
        self,
        zone: str,
        source_zone: Optional[str] = None,
        table_subset: Optional[List[str]] = None
    ) -> List[StreamingQuery]:
        """
        Run pipeline for a specific zone.

        When a FlowConfig contains multi-target groups (stages sharing the same
        resolved input), all grouped stages are processed together via
        ``foreachBatch``.  Stages already processed as part of an earlier group
        are skipped (tracked in ``_processed_stages``).
        
        Args:
            zone: Target zone (e.g., 'bronze', 'silver', 'gold')
            source_zone: Source zone to read from (None = landing)
            table_subset: Optional list of table names to process (None = all)
            
        Returns:
            List of StreamingQuery instances
        """
        # Acquire job lock if configured and not already held by run_full_pipeline
        lock_acquired_here = False
        if self._job_lock and not self._lock_held_by_full_pipeline:
            self._job_lock.acquire()
            lock_acquired_here = True

        try:
            return self._run_zone_pipeline_inner(zone, source_zone, table_subset)
        finally:
            if lock_acquired_here:
                self._job_lock.release()

    def _run_zone_pipeline_inner(
        self,
        zone: str,
        source_zone: Optional[str] = None,
        table_subset: Optional[List[str]] = None
    ) -> List[StreamingQuery]:
        """Inner implementation of run_zone_pipeline (no lock logic)."""
        self.logger.info(f"Starting {zone} pipeline (source: {source_zone or 'landing'})")
        
        # Filter tables
        if table_subset:
            configs = [self.table_registry[name] for name in table_subset]
        else:
            configs = [
                config for config in self.table_registry.values()
                if config.is_enabled_for_zone(zone)
            ]
        
        self.logger.info(f"Processing {len(configs)} tables for {zone} zone")
        
        # Log active streams before starting
        active_names = [q.name for q in self.spark.streams.active if q.isActive]
        if active_names:
            self.logger.info(f"Currently active streams: {active_names}")
        
        queries: List[StreamingQuery] = []

        for table_config in configs:
            # Skip if this stage was already processed as part of a multi-target group
            if (table_config.name, zone) in self._processed_stages:
                self.logger.info(
                    f"⏭️  Stage '{zone}' for {table_config.name} already processed in group"
                )
                continue

            # Build groups for this FlowConfig
            groups = self._build_stage_groups(table_config)

            # Find the group that contains the current zone
            target_group = None
            for group in groups:
                if any(s_name == zone for s_name, _ in group):
                    target_group = group
                    break

            if target_group is None:
                continue

            try:
                if len(target_group) == 1:
                    # Single-stage: use existing path
                    pipeline = ZonePipeline(
                        spark=self.spark, zone=zone, config=self.config
                    )
                    query = pipeline.process_table(table_config)
                    if query:
                        queries.append(query)
                else:
                    # Multi-stage group: use primary stage's zone for the read
                    primary_zone = target_group[0][0]
                    pipeline = ZonePipeline(
                        spark=self.spark, zone=primary_zone, config=self.config
                    )
                    query = pipeline.process_table_group(table_config, target_group)
                    if query:
                        queries.append(query)
            except Exception as e:
                if self.config.get('fail_fast', True):
                    raise
                self.logger.error(
                    f"Failed to start {table_config.name} for {zone} zone: {e}"
                )

            # Mark all stages in the group as processed
            for s_name, _ in target_group:
                self._processed_stages.add((table_config.name, s_name))

        # Categorize results: newly started vs already running
        started = []
        skipped = []
        for query in queries:
            if query.name in active_names:
                skipped.append(query.name)
            else:
                started.append(query.name)
        
        # Register queries with stream manager
        for query in queries:
            self.stream_manager.register(query, zone=zone)
        
        if started:
            self.logger.info(f"✅ Started {len(started)} streams for {zone}: {started}")
        if skipped:
            self.logger.info(f"⏭️  Skipped {len(skipped)} already-active streams for {zone}: {skipped}")
        if not started and not skipped:
            self.logger.info(f"No streams to start for {zone} zone")
        
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
            self.stream_manager.register(query, zone=zone)
        
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
            When event_driven_chaining is enabled (default), downstream zones are
            triggered automatically via StreamingQueryListener when upstream zones
            produce data. Set event_driven_chaining=False in config for the legacy
            sequential approach.
        """
        if zones is None:
            zones = ['bronze', 'silver', 'gold']
        
        # Acquire job lock if configured
        if self._job_lock:
            self._job_lock.acquire()
            self._lock_held_by_full_pipeline = True

        try:
            self.logger.info(f"Starting full pipeline for zones: {zones}")
            self._processed_stages.clear()
            
            use_chaining = self.config.get('event_driven_chaining', True) and self.is_streaming

            if use_chaining and len(zones) > 1:
                self._run_event_driven_pipeline(zones, include_dimensions, await_termination)
            else:
                self._run_sequential_pipeline(zones, include_dimensions, await_termination)
        except Exception:
            self._release_job_lock()
            raise
        else:
            # For non-streaming or availableNow, release immediately after completion.
            # For continuous streams, lock is released by stop_all().
            if not self.is_streaming or not self.stream_manager.get_active_queries():
                self._release_job_lock()
    
    def _run_sequential_pipeline(
        self,
        zones: List[str],
        include_dimensions: bool,
        await_termination: bool = None
    ):
        """Legacy sequential zone processing."""
        source_zone = None
        for zone in zones:
            self.run_zone_pipeline(zone, source_zone=source_zone)
            source_zone = zone
        
        if include_dimensions and self.dimension_registry:
            dimension_zone = zones[-1] if zones else 'gold'
            self.run_dimension_pipeline(dimension_zone)
        
        self.logger.info("Full pipeline started (sequential)")
        self._handle_await_termination(await_termination)
    
    def _run_event_driven_pipeline(
        self,
        zones: List[str],
        include_dimensions: bool,
        await_termination: bool = None
    ):
        """
        Event-driven pipeline: first zone runs with its configured trigger,
        downstream zones are spawned as availableNow via StreamingQueryListener.
        """
        self.logger.info(f"Starting event-driven pipeline: {' → '.join(zones)}")
        
        # Build the listener with spawn callback
        self._chain_listener = StageChainListener(
            zone_chain=zones,
            spawn_zone_fn=self._spawn_zone,
            logger=self.logger,
        )
        self.spark.streams.addListener(self._chain_listener)
        self.logger.info("StageChainListener registered with SparkSession")
        
        # Start the first zone with its configured trigger
        first_zone = zones[0]
        queries = self.run_zone_pipeline(first_zone)
        
        # Determine trigger mode for first zone queries
        for q in queries:
            # Find the matching FlowConfig to get trigger_mode
            trigger_mode = self._get_query_trigger_mode(q.name, first_zone)
            self._chain_listener.register_query(q, first_zone, trigger_mode)
        
        # Recovery: cascade all downstream zones as availableNow after the first
        # zone is running. Idempotent — if no pending data from a prior run,
        # streams process 0 rows and stop.
        for zone in zones[1:]:
            self.logger.info(f"Recovery: spawning {zone} as availableNow")
            recovery_queries = self._spawn_zone_internal(zone, recovery=True)
            for q in recovery_queries:
                self._chain_listener.register_query(q, zone, 'availableNow')
        
        # Dimensions: run after all zones (not event-driven)
        if include_dimensions and self.dimension_registry:
            dimension_zone = zones[-1] if zones else 'gold'
            self.run_dimension_pipeline(dimension_zone)
        
        self.logger.info("Full pipeline started (event-driven chaining)")
        self._handle_await_termination(await_termination)
    
    def _spawn_zone(self, zone: str) -> List[StreamingQuery]:
        """
        Callback for StageChainListener to spawn a zone as availableNow.
        Creates a ZonePipeline, processes all enabled tables, and registers
        queries with the StreamManager.
        """
        queries = self._spawn_zone_internal(zone)
        return queries
    
    def _spawn_zone_internal(self, zone: str, recovery: bool = False) -> List[StreamingQuery]:
        """Create and start zone pipeline queries, register with StreamManager.
        
        Args:
            zone: Target zone name
            recovery: If True, skip tables whose upstream doesn't exist yet
                (first run). The chain listener will trigger them once upstream
                produces data.
        """
        pipeline = ZonePipeline(
            spark=self.spark,
            zone=zone,
            config=self.config
        )
        
        configs = [
            config for config in self.table_registry.values()
            if config.is_enabled_for_zone(zone)
        ]
        
        if not configs:
            self.logger.info(f"No tables enabled for {zone} zone")
            return []
        
        self.logger.info(f"Spawning {len(configs)} tables for {zone} zone (availableNow)")
        queries = pipeline.process_all(configs, recovery=recovery)
        
        for query in queries:
            self.stream_manager.register(query, zone=zone)
        
        return queries
    
    def _get_query_trigger_mode(self, query_name: str, zone: str) -> str:
        """Resolve the trigger mode for a query based on its FlowConfig."""
        for config in self.table_registry.values():
            if config.is_enabled_for_zone(zone):
                # Query names typically follow the pattern: zone_tablename
                if config.name in query_name:
                    return config.trigger_mode
        return 'availableNow'
    
    def _handle_await_termination(self, await_termination: bool = None):
        """Common await_termination logic for both pipeline modes."""
        if await_termination is None:
            await_termination = self.config.get('await_termination', False)
        
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
        """Stop all streaming queries, remove the chain listener, release job lock, and reset state"""
        self.logger.info("Stopping all streams...")
        self.stream_manager.stop_all()
        try:
            self.spark.streams.resetTerminated()
        except Exception as e:
            self.logger.warning(f"Failed to reset terminated queries: {e}")
        if self._chain_listener is not None:
            try:
                self.spark.streams.removeListener(self._chain_listener)
                self.logger.info("StageChainListener removed")
            except Exception as e:
                self.logger.warning(f"Failed to remove StageChainListener: {e}")
            self._chain_listener = None
        self._processed_stages.clear()
        self._release_job_lock()
        self.logger.info("All streams stopped")
    
    def _release_job_lock(self):
        """Release the job lock if held."""
        if self._job_lock and self._job_lock.held:
            self._job_lock.release()
        self._lock_held_by_full_pipeline = False
    
    def get_status(self, as_dataframe: bool = False):
        """
        Get status of all streaming queries
        
        Args:
            as_dataframe: If True, return a pandas DataFrame with flattened
                metrics. Defaults to False (dict output).
        
        Returns:
            Dict with query statuses, or a pandas DataFrame
        """
        return self.stream_manager.get_status(as_dataframe=as_dataframe)

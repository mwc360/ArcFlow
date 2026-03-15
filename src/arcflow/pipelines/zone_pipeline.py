"""
Zone-agnostic pipeline - works for any zone (bronze, silver, gold, etc.)

Replaces separate BronzePipeline, SilverPipeline, GoldPipeline with one flexible class
"""
import logging
import threading
from typing import List, Optional, Tuple
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
from ..utils.endpoint_validator import StreamEndpointValidator

_STREAMING_FORMATS = ('kafka', 'eventhub')

# Tracks query names currently being set up (between "decided to start" and
# "stream is live in Spark").  Used with _query_start_lock to prevent two
# threads from starting the same (table, zone) stream concurrently.
_pending_queries: set = set()
_query_start_lock = threading.Lock()


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

        If the stage has ``stage_input`` set, it takes precedence:
        - stage_input == FlowConfig.name → root (returns None)
        - stage_input == another stage name → that stage's output

        Otherwise follows the **main chain** (stages without ``stage_input``,
        in dict order).  Branches are skipped so they don't shift the chain.

        Args:
            table_config: FlowConfig with zones defined

        Returns:
            Name of source zone (e.g., 'bronze') or None if reading from landing
        """
        zone_config = table_config.zones.get(self.zone)
        if zone_config is None:
            raise ValueError(
                f"Zone '{self.zone}' not found in table config. "
                f"Available: {list(table_config.zones.keys())}"
            )

        # Explicit stage_input overrides dict-order resolution
        if zone_config.stage_input is not None:
            if zone_config.stage_input == table_config.name:
                return None  # root source
            if zone_config.stage_input in table_config.zones:
                return zone_config.stage_input
            raise ValueError(
                f"stage_input '{zone_config.stage_input}' for stage '{self.zone}' "
                f"is not a valid stage name or FlowConfig.name ('{table_config.name}'). "
                f"Available stages: {list(table_config.zones.keys())}"
            )

        # Main chain: only stages without stage_input, in dict order
        main_chain = [
            name for name, cfg in table_config.zones.items()
            if cfg.stage_input is None
        ]

        if self.zone not in main_chain:
            # Should not happen — stage_input is None so it must be in main_chain
            raise ValueError(f"Zone '{self.zone}' not in main chain: {main_chain}")

        idx = main_chain.index(self.zone)
        if idx == 0:
            return None  # first in chain → root
        return main_chain[idx - 1]
        
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
        if zone_config and zone_config.schema_name is not None:
            return zone_config.schema_name
        return zone
    
    def _get_table_name(self, table_config: FlowConfig, zone) -> str:
        """
        Get target table name for a given zone from table config.

        Args:
            table_config: FlowConfig instance
            zone: Zone name
        """
        if zone is not None:
            zone_config = table_config.get_zone_config(zone)
            if zone_config and zone_config.table_name is not None:
                return zone_config.table_name
        return table_config.name

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
        source_table = self._get_table_name(table_config, source_zone)
        table_reference = build_table_reference(source_catalog, source_schema, source_table)

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
    
    def _get_upstream_zones(self, table_config: FlowConfig) -> List[str]:
        """
        Return the ordered list of zones in the main chain before this zone.

        For example, if the main chain is [bronze, silver, gold] and self.zone
        is 'silver', returns ['bronze'].  If self.zone is 'bronze', returns [].
        """
        main_chain = [
            name for name, cfg in table_config.zones.items()
            if cfg.stage_input is None
        ]
        if self.zone not in main_chain:
            return []
        idx = main_chain.index(self.zone)
        return main_chain[:idx]

    def _apply_upstream_transforms(
        self,
        df: DataFrame,
        table_config: FlowConfig,
    ) -> DataFrame:
        """
        Apply transformations for every zone upstream of this zone in the main chain.

        This allows ``test_input`` / ``test_output`` to read from the root source
        and replay the full transform chain without requiring persisted Delta tables.
        """
        for zone_name in self._get_upstream_zones(table_config):
            zone_config = table_config.get_zone_config(zone_name)
            if zone_config and zone_config.enabled:
                self.logger.info(f"Chaining upstream transforms for {zone_name}")
                df = self.apply_transformations(df, table_config, zone_config)
        return df

    def _stream_to_memory(
        self,
        streaming_df: DataFrame,
        view_name: str,
        limit: int,
        timeout_seconds: int,
    ) -> DataFrame:
        """
        Write a streaming DataFrame to a Spark memory sink and return a batch result.

        Uses ``trigger(availableNow=True)`` — reads all backlogged messages then stops
        automatically. No checkpoint is required (memory sink is stateless).
        The in-memory view persists in the Spark session for follow-up ad-hoc SQL queries.

        Args:
            streaming_df: Streaming DataFrame to materialise.
            view_name: Name of the in-memory temp view (prefixed with _arcflow_).
            limit: Maximum rows to return.
            timeout_seconds: Safety-net timeout for awaitTermination.

        Returns:
            Batch DataFrame with at most ``limit`` rows.
        """
        query = (
            streaming_df.writeStream
            .format('memory')
            .queryName(view_name)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination(timeout=timeout_seconds)
        self.logger.info(
            f"Memory view '{view_name}' ready — "
            f"run spark.sql(\"SELECT * FROM {view_name}\") for further queries."
        )
        return self.spark.sql(f"SELECT * FROM {view_name} LIMIT {limit}")

    def test_input(
        self,
        table_config: FlowConfig,
        limit: int = 20,
        timeout_seconds: int = 60,
        raw: bool = False,
    ) -> DataFrame:
        """
        Test the source read for this pipeline without writing to any zone.

        For Kafka / Event Hubs sources the data is streamed into a Spark memory view
        using ``trigger(availableNow=True)`` — no checkpoint, no Delta write, row-limited.
        For file-based sources a standard batch read is used.

        Args:
            table_config: FlowConfig instance.
            limit: Maximum number of rows to return (streaming sources only).
            timeout_seconds: awaitTermination timeout in seconds (streaming sources only).
            raw: If True (streaming sources only), return the raw message payload as a
                 string column named ``payload`` without applying JSON deserialization.
                 Useful for initial stream discovery before a schema is defined.
                 Ignored for file-based sources.

        Returns:
            DataFrame (batch) — schema columns + metadata columns when raw=False,
            payload string + metadata columns when raw=True.

        Notes:
            The in-memory view ``_arcflow_test_<name>`` persists in the Spark session
            after this call for further ad-hoc SQL exploration.
        """
        zone_config = table_config.get_zone_config(self.zone)
        if not zone_config:
            self.logger.info(f"Table {table_config.name} not configured for {self.zone} zone")
            return None

        self.logger.info(f"Testing {table_config.name} input for {self.zone} zone")

        if table_config.format in _STREAMING_FORMATS:
            validation = StreamEndpointValidator.validate(table_config)
            if not validation.valid:
                raise RuntimeError(
                    f"Endpoint validation failed for '{table_config.name}': {validation.error}"
                )
            reader = ReaderFactory(self.spark, True, self.config).create_reader(table_config)
            try:
                streaming_df = reader.read(table_config, raw=raw, max_records=limit)
                if not raw:
                    streaming_df = self._apply_upstream_transforms(streaming_df, table_config)
            except Exception as e:
                self.logger.error(f"Failed to read {table_config.name}: {e}")
                raise
            view_name = f"_arcflow_test_{table_config.name}"
            return self._stream_to_memory(streaming_df, view_name, limit, timeout_seconds)

        # File-based sources — batch path
        original_streaming = self.is_streaming
        self.is_streaming = False
        try:
            df = self.read_source(table_config)
            df = self._apply_upstream_transforms(df, table_config)
            self.logger.info(f"Successfully generated test input for {table_config.name}")
            return df.limit(limit)
        except Exception as e:
            self.logger.error(f"Failed to test {table_config.name} input: {e}")
            raise
        finally:
            self.is_streaming = original_streaming

    def test_output(
        self,
        table_config: FlowConfig,
        limit: int = 20,
        timeout_seconds: int = 60,
    ) -> DataFrame:
        """
        Test the full read + transform pipeline without writing to any zone.

        For Kafka / Event Hubs sources the data is streamed into a Spark memory view
        using ``trigger(availableNow=True)`` — no checkpoint, no Delta write, row-limited.
        Schema deserialization and all zone transformations (``custom_transform``,
        snake_case normalisation, processing timestamp) are applied before writing to memory.
        For file-based sources a standard batch read + transform is used.

        Args:
            table_config: FlowConfig instance.
            limit: Maximum number of rows to return (streaming sources only).
            timeout_seconds: awaitTermination timeout in seconds (streaming sources only).

        Returns:
            Transformed DataFrame (batch).

        Notes:
            The in-memory view ``_arcflow_test_out_<name>`` persists in the Spark session
            after this call for further ad-hoc SQL exploration.
        """
        zone_config = table_config.get_zone_config(self.zone)
        if not zone_config:
            self.logger.info(f"Table {table_config.name} not configured for {self.zone} zone")
            return None

        self.logger.info(f"Testing {table_config.name} output for {self.zone} zone")

        if table_config.format in _STREAMING_FORMATS:
            validation = StreamEndpointValidator.validate(table_config)
            if not validation.valid:
                raise RuntimeError(
                    f"Endpoint validation failed for '{table_config.name}': {validation.error}"
                )
            reader = ReaderFactory(self.spark, True, self.config).create_reader(table_config)
            try:
                streaming_df = reader.read(table_config, raw=False, max_records=limit)
                streaming_df = self._apply_upstream_transforms(streaming_df, table_config)
                streaming_df = self.apply_transformations(streaming_df, table_config, zone_config)
            except Exception as e:
                self.logger.error(f"Failed to build pipeline for {table_config.name}: {e}")
                raise
            view_name = f"_arcflow_test_out_{table_config.name}"
            return self._stream_to_memory(streaming_df, view_name, limit, timeout_seconds)

        # File-based sources — batch path
        original_streaming = self.is_streaming
        self.is_streaming = False
        try:
            df = self.read_source(table_config)
            df = self._apply_upstream_transforms(df, table_config)
            df = self.apply_transformations(df, table_config, zone_config)
            self.logger.info(f"Successfully generated test output for {table_config.name}")
            return df.limit(limit)
        except Exception as e:
            self.logger.error(f"Failed to test {table_config.name} output: {e}")
            raise
        finally:
            self.is_streaming = original_streaming
    
    def write_target(
        self,
        df: DataFrame,
        table_config: FlowConfig,
        zone_config: StageConfig
    ) -> Optional[StreamingQuery]:
        """
        Write to target zone
        
        Downstream zones (those reading from a prior zone's Delta output)
        always use availableNow trigger — only the root zone polls the
        external source with the configured trigger_mode.
        
        Args:
            df: DataFrame to write
            table_config: FlowConfig instance
            zone_config: StageConfig instance
            
        Returns:
            StreamingQuery if streaming, None if batch
        """
        # Force availableNow for downstream zones when using event-driven chaining.
        # When chaining is disabled, downstream zones use the table's own trigger
        # so they run as persistent streams alongside the root zone.
        source_zone = self._get_source_zone(table_config)
        use_chaining = self.config.get('event_driven_chaining', True)
        if source_zone is not None and use_chaining:
            config = {**self.config, '_trigger_mode_override': 'availableNow'}
        else:
            config = self.config

        writer_factory = WriterFactory(self.spark, self.is_streaming, config)
        writer = writer_factory.create_writer(table_config, zone_config)
        return writer.write(df, table_config, zone_config, self.zone)
    
    def _is_query_active(self, query_name: str) -> bool:
        """Check if a streaming query with this name is already active."""
        for active_query in self.spark.streams.active:
            if active_query.name == query_name and active_query.isActive:
                return True
        return False

    def _claim_query(self, query_name: str) -> bool:
        """Atomically check if a query is active or pending, and claim it if not.
        
        Returns True if this caller now owns the query name and should proceed.
        Returns False if the query is already active or being set up.
        """
        with _query_start_lock:
            if query_name in _pending_queries or self._is_query_active(query_name):
                return False
            _pending_queries.add(query_name)
            return True

    @staticmethod
    def _release_claim(query_name: str):
        """Release a claimed query name after the stream has started (or failed)."""
        with _query_start_lock:
            _pending_queries.discard(query_name)

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

        query_name = f"{self.zone}_{table_config.name}_stream"

        # For streaming: atomic check-and-claim prevents duplicate readStream setup
        if self.is_streaming and not self._claim_query(query_name):
            self.logger.info(f"⏭️  Query '{query_name}' is already active or pending, skipping")
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
        finally:
            if self.is_streaming:
                self._release_claim(query_name)
    
    def process_table_group(
        self,
        table_config: FlowConfig,
        stages: List[Tuple[str, StageConfig]],
    ) -> Optional[StreamingQuery]:
        """
        Process a group of stages that share the same input in one streaming query.

        Reads from the shared source once, then delegates to a multi-target writer
        that applies each stage's ``custom_transform`` inside a ``foreachBatch``.

        Args:
            table_config: FlowConfig instance
            stages: List of (stage_name, StageConfig) tuples sharing the same input

        Returns:
            StreamingQuery if streaming, None if batch
        """
        stage_names = [s[0] for s in stages]
        primary_name = stages[0][0]
        t_name = getattr(stages[0][1], 'table_name', None) or table_config.name
        query_name = f"{primary_name}_{t_name}_stream"

        if self.is_streaming and not self._claim_query(query_name):
            self.logger.info(f"⏭️  Query '{query_name}' is already active or pending, skipping")
            return None

        self.logger.info(
            f"Processing multi-target group for {table_config.name}: {stage_names}"
        )

        try:
            # Read once from the shared source (uses self.zone for resolution)
            df = self.read_source(table_config)

            # Force availableNow for downstream zones when using event-driven chaining.
            # When chaining is disabled, downstream zones use the table's own trigger
            # so they run as persistent streams alongside the root zone.
            source_zone = self._get_source_zone(table_config)
            use_chaining = self.config.get('event_driven_chaining', True)
            if source_zone is not None and use_chaining:
                config = {**self.config, '_trigger_mode_override': 'availableNow'}
            else:
                config = self.config

            # Write to all targets via multi-target writer
            writer_factory = WriterFactory(self.spark, self.is_streaming, config)
            # Use the first stage to create the writer (config-level settings)
            writer = writer_factory.create_writer(table_config, stages[0][1])
            query = writer.write_multi(
                df, table_config, stages, self.zone, config
            )

            self.logger.info(
                f"Successfully set up multi-target pipeline for "
                f"{table_config.name} → {stage_names}"
            )
            return query

        except Exception as e:
            self.logger.error(
                f"Failed to process multi-target group for {table_config.name}: {e}"
            )
            raise
        finally:
            if self.is_streaming:
                self._release_claim(query_name)

    def process_all(
        self,
        table_configs: List[FlowConfig],
        recovery: bool = False,
    ) -> List[StreamingQuery]:
        """
        Process all tables for this zone
        
        Args:
            table_configs: List of FlowConfig instances
            recovery: If True, log skipped tables at INFO level (upstream may
                not exist yet on first run). Otherwise respects the ``fail_fast``
                config: raise on failure when True, log at ERROR when False.
            
        Returns:
            List of StreamingQuery instances that started successfully
        """
        fail_fast = self.config.get('fail_fast', True) and not recovery
        queries = []
        for config in table_configs:
            try:
                query = self.process_table(config)
                if query:
                    queries.append(query)
            except Exception as e:
                if fail_fast:
                    raise
                if recovery:
                    self.logger.info(
                        f"Skipping {config.name} for {self.zone} zone — "
                        f"upstream not yet available, will be triggered by chain listener"
                    )
                else:
                    self.logger.error(
                        f"Failed to start {config.name} for {self.zone} zone: {e}"
                    )
        return queries

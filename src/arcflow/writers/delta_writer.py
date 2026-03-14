"""
Delta Lake writer for arcflow framework
"""
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from typing import Optional, List, Tuple
import logging
import threading
from .base_writer import BaseWriter
from ..utils.table_utils import build_table_reference
from ..transformations.common import (
    normalize_columns_to_snake_case,
    apply_processing_timestamp
)
from ..transformations.zone_transforms import (
    get_zone_transformer,
    has_zone_transformer
)
import tenacity

# Class-level lock shared across all DeltaWriter instances.  Makes the
# "check active queries → start stream" sequence atomic so that concurrent
# spawn paths (recovery + listener) cannot create duplicate streams for the
# same (table, zone) pair.
_stream_start_lock = threading.Lock()


class DeltaWriter(BaseWriter):
    """
    Delta Lake writer supporting:
    - Append and upsert modes
    - Batch and streaming writes
    - Partitioning
    - Optimizations (optimizeWrite, autoCompact)
    """
    
    def __init__(self, spark, is_streaming, config):
        super().__init__(spark, is_streaming, config)
        self.logger = logging.getLogger(__name__)

    @tenacity.retry(
        # retry due to intermittent errors on windows
        retry=tenacity.retry_if_exception(
            lambda e: "java.lang.UnsatisfiedLinkError" in str(e) and 
                     "NativeIO$POSIX.stat" in str(e)
        ),
        stop=tenacity.stop_after_attempt(2)
    )
    def create_schema_if_not_exists(self, schema_name: str):
        """
        Create schema if it does not exist
        
        Args:
            schema_name: Name of the schema to create
        """
        self.logger.info(f"Creating schema {schema_name} if not exists")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    
    def write(
        self,
        df: DataFrame,
        table_config,
        zone_config,
        zone: str
    ) -> Optional[StreamingQuery]:
        """
        Write DataFrame to Delta Lake
        
        Args:
            df: DataFrame to write
            table_config: FlowConfig or DimensionConfig
            zone_config: StageConfig instance
            zone: Target zone name
            
        Returns:
            StreamingQuery if streaming, None if batch
        """
        table_name = getattr(zone_config, 'table_name', None) or getattr(table_config, 'name')
        
        catalog_name = self.config.get('lakehouse_name', None)
        # Use zone_config.schema_name if provided, otherwise fall back to zone name
        schema_name = getattr(zone_config, 'schema_name', None) or zone
        
        # Build the full reference with proper escaping
        table_reference = build_table_reference(catalog_name, schema_name, table_name)
        
        # Create schema if it doesn't exist
        self.create_schema_if_not_exists(schema_name)
        
        self.logger.info(f"Writing {table_name} to {table_reference} (mode: {zone_config.mode})")
        
        if self.is_streaming:
            return self._write_stream(df, table_config, zone_config, zone, table_reference)
        else:
            self._write_batch(df, zone_config, table_reference)
            return None
    
    def _write_stream(
        self,
        df: DataFrame,
        table_config,
        zone_config,
        zone: str,
        table_reference: str
    ) -> StreamingQuery:
        """Write streaming DataFrame to Delta"""
        table_name = getattr(table_config, 'name')
        query_name = f"{zone}_{table_name}_stream"

        with _stream_start_lock:
            # Skip if a query with this name is truly active in the SparkSession
            for active_query in self.spark.streams.active:
                if active_query.name == query_name and active_query.isActive:
                    self.logger.info(
                        f"⏭️  Query '{query_name}' is already active, skipping start"
                    )
                    return active_query

            checkpoint_path = self.get_checkpoint_path(zone, table_name)
            
            writer = (df.writeStream
                .format('delta')
                .option('checkpointLocation', checkpoint_path)
                .queryName(f"{zone}_{table_name}_stream")
            )
            
            # Apply trigger configuration (override takes precedence for downstream zones)
            trigger_mode = self.config.get('_trigger_mode_override') or getattr(table_config, 'trigger_mode', 'availableNow')
            
            # Check table-level trigger_interval first, then fall back to global config
            trigger_interval = table_config.trigger_interval
            if trigger_interval is None:
                trigger_interval = self.config.get('trigger_interval', None)
            
            self.logger.debug(f"Trigger config for {table_name}: mode={trigger_mode}, interval={trigger_interval}")
            
            if trigger_mode == 'availableNow':
                writer = writer.trigger(availableNow=True)
            elif trigger_mode == 'processingTime':
                if trigger_interval is None:
                    raise ValueError(
                        f"`trigger_interval` not specified for {table_name}. "
                        f"Set it in table config or global config. "
                    )
                writer = writer.trigger(processingTime=trigger_interval)
            elif trigger_mode == 'continuous':
                interval = trigger_interval or '1 second'
                writer = writer.trigger(continuous=interval)
            
            # Apply partitioning
            if zone_config.partition_by:
                writer = writer.partitionBy(*zone_config.partition_by)
            
            # Handle append vs upsert
            if zone_config.mode == 'upsert':
                # Use foreachBatch for merge/upsert
                def upsert_batch(batch_df, batch_id):
                    self._merge_batch(batch_df, zone_config.merge_keys, table_reference)
                
                return writer.foreachBatch(upsert_batch).start()
            else:
                # Simple append
                writer = writer.outputMode('append')
                return writer.toTable(table_reference)
    
    def _write_batch(
        self,
        df: DataFrame,
        zone_config,
        table_reference: str
    ):
        """Write batch DataFrame to Delta"""
        if zone_config.mode == 'upsert':
            self._merge_batch(df, zone_config.merge_keys, table_reference)
        else:
            writer = df.write.format('delta').mode('append')
            
            if zone_config.partition_by:
                writer = writer.partitionBy(*zone_config.partition_by)
            
            writer.saveAsTable(table_reference)
    
    def _merge_batch(
        self,
        batch_df: DataFrame,
        merge_keys: list,
        table_reference: str
    ):
        """
        Perform merge/upsert operation
        
        Args:
            batch_df: DataFrame to merge
            merge_keys: List of columns to use for matching
            table_reference: Fully qualified table reference
        """
        from delta.tables import DeltaTable
        
        if not merge_keys:
            raise ValueError("merge_keys required for upsert mode")
        
        # Check if table exists
        if self.spark.catalog.tableExists(table_reference):
            delta_table = DeltaTable.forName(self.spark, table_reference)
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in merge_keys
            ])
            
            # Perform merge
            (delta_table.alias('target')
                .merge(
                    batch_df.alias('source'),
                    merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            
            self.logger.info(f"Merged batch to {table_reference}")
        else:
            # Table doesn't exist, create it
            batch_df.write.format('delta').saveAsTable(table_reference)
            self.logger.info(f"Created new table at {table_reference}")

    # ── Multi-target (foreachBatch) support ─────────────────────────

    def _resolve_target(self, stage_name, stage_config, table_config, zone):
        """Resolve table_reference and schema for a stage in a multi-target group."""
        t_name = getattr(stage_config, 'table_name', None) or table_config.name
        s_name = getattr(stage_config, 'schema_name', None) or stage_name
        catalog = self.config.get('lakehouse_name', None)
        return build_table_reference(catalog, s_name, t_name), s_name

    def _apply_stage_transforms(self, df, stage_config):
        """Apply per-stage custom_transform + universal transforms."""
        if stage_config.custom_transform:
            if has_zone_transformer(stage_config.custom_transform):
                transformer = get_zone_transformer(stage_config.custom_transform)
                df = transformer(df)
            else:
                self.logger.warning(
                    f"Transform '{stage_config.custom_transform}' not found, skipping"
                )
        df = normalize_columns_to_snake_case(df)
        df = apply_processing_timestamp(df)
        return df

    def _write_single_target(self, batch_df, stage_config, table_reference):
        """Write a single batch to one target (append or upsert)."""
        if stage_config.mode == 'upsert':
            self._merge_batch(batch_df, stage_config.merge_keys, table_reference)
        else:
            writer = batch_df.write.format('delta').mode('append')
            if stage_config.partition_by:
                writer = writer.partitionBy(*stage_config.partition_by)
            writer.saveAsTable(table_reference)

    def write_multi(
        self,
        df: DataFrame,
        table_config,
        stages: List[Tuple[str, 'StageConfig']],
        zone: str,
        config: dict,
    ) -> Optional[StreamingQuery]:
        """
        Write one input DataFrame to multiple targets.

        Args:
            df: Source DataFrame (streaming or batch)
            table_config: FlowConfig instance
            stages: List of (stage_name, StageConfig) tuples
            zone: Primary zone name (used for query naming / checkpoint)
            config: Global pipeline config

        Returns:
            StreamingQuery if streaming, None if batch
        """
        # Ensure all target schemas exist
        for stage_name, stage_config in stages:
            s_name = getattr(stage_config, 'schema_name', None) or stage_name
            self.create_schema_if_not_exists(s_name)

        if self.is_streaming:
            return self._write_stream_multi(df, table_config, stages, zone)
        else:
            self._write_batch_multi(df, table_config, stages)
            return None

    def _write_stream_multi(
        self,
        df: DataFrame,
        table_config,
        stages: List[Tuple[str, 'StageConfig']],
        zone: str,
    ) -> StreamingQuery:
        """Start one streaming query that writes to N targets via foreachBatch."""
        primary_name = stages[0][0]
        t_name = getattr(stages[0][1], 'table_name', None) or table_config.name
        query_name = f"{primary_name}_{t_name}_stream"

        with _stream_start_lock:
            # Skip if already active
            for active_query in self.spark.streams.active:
                if active_query.name == query_name and active_query.isActive:
                    self.logger.info(f"⏭️  Query '{query_name}' is already active, skipping")
                    return active_query

            checkpoint_path = self.get_checkpoint_path(zone, f"{t_name}_multi")

            # Capture references for the closure
            writer_ref = self
            stages_snapshot = list(stages)
            table_config_ref = table_config

            def multi_batch(batch_df, batch_id):
                if batch_df.isEmpty():
                    return
                # Cache if writing to multiple targets
                if len(stages_snapshot) > 1:
                    batch_df.cache()
                try:
                    for s_name, s_cfg in stages_snapshot:
                        target_df = writer_ref._apply_stage_transforms(batch_df, s_cfg)
                        ref, _ = writer_ref._resolve_target(
                            s_name, s_cfg, table_config_ref, s_name
                        )
                        writer_ref._write_single_target(target_df, s_cfg, ref)
                        writer_ref.logger.info(f"  → wrote batch {batch_id} to {ref}")
                finally:
                    if len(stages_snapshot) > 1:
                        batch_df.unpersist()

            writer = (
                df.writeStream
                .format('delta')
                .option('checkpointLocation', checkpoint_path)
                .queryName(query_name)
            )

            # Apply trigger (override takes precedence for downstream zones)
            trigger_mode = self.config.get('_trigger_mode_override') or getattr(table_config, 'trigger_mode', 'availableNow')
            trigger_interval = getattr(table_config, 'trigger_interval', None)
            if trigger_interval is None:
                trigger_interval = self.config.get('trigger_interval', None)

            if trigger_mode == 'availableNow':
                writer = writer.trigger(availableNow=True)
            elif trigger_mode == 'processingTime':
                if trigger_interval is None:
                    raise ValueError(
                        f"`trigger_interval` not specified for {table_config.name}."
                    )
                writer = writer.trigger(processingTime=trigger_interval)
            elif trigger_mode == 'continuous':
                writer = writer.trigger(continuous=trigger_interval or '1 second')

            self.logger.info(
                f"Starting multi-target query '{query_name}' → "
                f"{[s[0] for s in stages]}"
            )
            return writer.foreachBatch(multi_batch).start()

    def _write_batch_multi(
        self,
        df: DataFrame,
        table_config,
        stages: List[Tuple[str, 'StageConfig']],
    ):
        """Write one batch DataFrame to N targets."""
        if len(stages) > 1:
            df.cache()
        try:
            for stage_name, stage_config in stages:
                target_df = self._apply_stage_transforms(df, stage_config)
                ref, _ = self._resolve_target(
                    stage_name, stage_config, table_config, stage_name
                )
                self._write_single_target(target_df, stage_config, ref)
                self.logger.info(f"Wrote batch to {ref}")
        finally:
            if len(stages) > 1:
                df.unpersist()

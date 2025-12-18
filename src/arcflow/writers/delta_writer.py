"""
Delta Lake writer for arcflow framework
"""
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from typing import Optional
import logging
from .base_writer import BaseWriter
from ..utils.table_utils import build_table_reference
import tenacity


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
        table_name = getattr(table_config, 'name')
        
        catalog_name = self.config.get('lakehouse_name', None)
        # Use zone_config.schema_name if provided, otherwise fall back to zone name
        schema_name = getattr(zone_config, 'schema_name', zone)
        
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
        checkpoint_path = self.get_checkpoint_path(zone, table_name)
        
        writer = (df.writeStream
            .format('delta')
            .option('checkpointLocation', checkpoint_path)
            .queryName(f"{zone}_{table_name}_stream")
        )
        
        # Apply trigger configuration
        trigger_mode = getattr(table_config, 'trigger_mode', 'availableNow')
        
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

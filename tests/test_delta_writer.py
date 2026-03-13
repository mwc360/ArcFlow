"""Tests for DeltaWriter skip-active-query behavior."""
from unittest.mock import MagicMock, PropertyMock
import pytest

from arcflow.writers.delta_writer import DeltaWriter
from arcflow.models import FlowConfig, StageConfig
from pyspark.sql.types import StructType, StructField, StringType


def _make_mock_spark(active_queries=None):
    """Create a mock SparkSession with streams.active."""
    spark = MagicMock()
    spark.streams.active = active_queries or []
    return spark


def _make_table_config(name="orders", trigger_mode="availableNow"):
    schema = StructType([StructField("id", StringType())])
    return FlowConfig(
        name=name,
        schema=schema,
        format="parquet",
        zones={"bronze": StageConfig(enabled=True)},
        trigger_mode=trigger_mode,
    )


class TestSkipActiveQuery:
    def test_skips_start_when_query_already_active(self):
        existing_query = MagicMock()
        existing_query.name = "bronze_orders_stream"
        existing_query.isActive = True

        spark = _make_mock_spark(active_queries=[existing_query])
        writer = DeltaWriter(spark, is_streaming=True, config={"checkpoint_uri": "/tmp"})

        table_config = _make_table_config("orders")
        zone_config = StageConfig(enabled=True)
        df = MagicMock()

        result = writer._write_stream(df, table_config, zone_config, "bronze", "bronze.orders")

        # Should return the existing query, not start a new one
        assert result is existing_query
        df.writeStream.assert_not_called()

    def test_starts_new_query_when_none_active(self):
        spark = _make_mock_spark(active_queries=[])
        writer = DeltaWriter(spark, is_streaming=True, config={"checkpoint_uri": "/tmp"})

        table_config = _make_table_config("orders")
        zone_config = StageConfig(enabled=True)
        df = MagicMock()

        # Mock the chained writeStream builder
        mock_query = MagicMock()
        mock_query.name = "bronze_orders_stream"
        df.writeStream.format.return_value = df.writeStream
        df.writeStream.option.return_value = df.writeStream
        df.writeStream.queryName.return_value = df.writeStream
        df.writeStream.trigger.return_value = df.writeStream
        df.writeStream.outputMode.return_value = df.writeStream
        df.writeStream.toTable.return_value = mock_query

        result = writer._write_stream(df, table_config, zone_config, "bronze", "bronze.orders")

        # Should have called writeStream chain
        df.writeStream.format.assert_called_once_with('delta')
        assert result is mock_query

    def test_skips_only_matching_name(self):
        """A different active query should not prevent starting this one."""
        other_query = MagicMock()
        other_query.name = "bronze_customers_stream"
        other_query.isActive = True

        spark = _make_mock_spark(active_queries=[other_query])
        writer = DeltaWriter(spark, is_streaming=True, config={"checkpoint_uri": "/tmp"})

        table_config = _make_table_config("orders")
        zone_config = StageConfig(enabled=True)
        df = MagicMock()

        mock_query = MagicMock()
        df.writeStream.format.return_value = df.writeStream
        df.writeStream.option.return_value = df.writeStream
        df.writeStream.queryName.return_value = df.writeStream
        df.writeStream.trigger.return_value = df.writeStream
        df.writeStream.outputMode.return_value = df.writeStream
        df.writeStream.toTable.return_value = mock_query

        result = writer._write_stream(df, table_config, zone_config, "bronze", "bronze.orders")

        # Should start the new query since name doesn't match
        df.writeStream.format.assert_called_once()
        assert result is mock_query


class TestTriggerModeOverride:
    """Downstream zones should always use availableNow trigger."""

    def _make_writer_and_df(self, config):
        spark = _make_mock_spark(active_queries=[])
        writer = DeltaWriter(spark, is_streaming=True, config=config)
        df = MagicMock()
        mock_query = MagicMock()
        mock_query.name = "silver_orders_stream"
        df.writeStream.format.return_value = df.writeStream
        df.writeStream.option.return_value = df.writeStream
        df.writeStream.queryName.return_value = df.writeStream
        df.writeStream.trigger.return_value = df.writeStream
        df.writeStream.outputMode.return_value = df.writeStream
        df.writeStream.toTable.return_value = mock_query
        return writer, df

    def test_override_forces_available_now(self):
        """When _trigger_mode_override is set, use availableNow even if FlowConfig says processingTime."""
        config = {"checkpoint_uri": "/tmp", "_trigger_mode_override": "availableNow"}
        writer, df = self._make_writer_and_df(config)
        table_config = _make_table_config("orders", trigger_mode="processingTime")
        table_config.trigger_interval = "5 seconds"
        zone_config = StageConfig(enabled=True)

        writer._write_stream(df, table_config, zone_config, "silver", "silver.orders")

        df.writeStream.trigger.assert_called_once_with(availableNow=True)

    def test_no_override_uses_flow_config(self):
        """Without override, FlowConfig trigger_mode is used."""
        config = {"checkpoint_uri": "/tmp", "trigger_interval": "10 seconds"}
        writer, df = self._make_writer_and_df(config)
        table_config = _make_table_config("orders", trigger_mode="processingTime")
        table_config.trigger_interval = "10 seconds"
        zone_config = StageConfig(enabled=True)

        writer._write_stream(df, table_config, zone_config, "bronze", "bronze.orders")

        df.writeStream.trigger.assert_called_once_with(processingTime="10 seconds")

    def test_override_applies_to_write_stream_multi(self):
        """Multi-target writer also respects _trigger_mode_override."""
        config = {"checkpoint_uri": "/tmp", "_trigger_mode_override": "availableNow"}
        spark = _make_mock_spark(active_queries=[])
        writer = DeltaWriter(spark, is_streaming=True, config=config)

        df = MagicMock()
        mock_query = MagicMock()
        df.writeStream.format.return_value = df.writeStream
        df.writeStream.option.return_value = df.writeStream
        df.writeStream.queryName.return_value = df.writeStream
        df.writeStream.trigger.return_value = df.writeStream
        df.writeStream.foreachBatch.return_value = df.writeStream
        df.writeStream.start.return_value = mock_query

        table_config = _make_table_config("orders", trigger_mode="processingTime")
        table_config.trigger_interval = "5 seconds"
        stages = [("silver", StageConfig(enabled=True)), ("archive", StageConfig(enabled=True))]

        writer._write_stream_multi(df, table_config, stages, "silver")

        df.writeStream.trigger.assert_called_once_with(availableNow=True)


class TestStreamManagerClearOnStop:
    def test_stop_all_clears_queries_list(self):
        from arcflow.core.stream_manager import StreamManager

        sm = StreamManager()
        q1 = MagicMock()
        q1.isActive = True
        q1.name = "q1"
        q2 = MagicMock()
        q2.isActive = False
        q2.name = "q2"

        sm.register(q1, zone="bronze")
        sm.register(q2, zone="silver")

        assert len(sm.queries) == 2
        assert len(sm._zone_queries) == 2

        sm.stop_all()

        assert len(sm.queries) == 0
        assert len(sm._zone_queries) == 0
        q1.stop.assert_called_once()
        q2.stop.assert_not_called()  # was not active

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

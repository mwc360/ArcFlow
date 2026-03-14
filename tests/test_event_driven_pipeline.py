"""Integration tests for event-driven pipeline in Controller."""
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from arcflow.controller import Controller
from arcflow.models import FlowConfig, StageConfig
from arcflow.config import get_config
from pyspark.sql.types import StructType, StructField, StringType


def _make_mock_spark():
    """Create a mock SparkSession with streams manager."""
    spark = MagicMock()
    spark.streams = MagicMock()
    spark.streams.addListener = MagicMock()
    spark.streams.removeListener = MagicMock()
    return spark


def _make_table_registry():
    """Create a minimal table registry for testing."""
    schema = StructType([StructField("id", StringType())])
    return {
        "orders": FlowConfig(
            name="orders",
            schema=schema,
            format="parquet",
            zones={
                "bronze": StageConfig(enabled=True),
                "silver": StageConfig(enabled=True),
                "gold": StageConfig(enabled=True),
            },
            trigger_mode="availableNow",
        ),
    }


class TestRunFullPipelineDispatch:
    @patch.object(Controller, '_run_event_driven_pipeline')
    @patch.object(Controller, '_run_sequential_pipeline')
    def test_defaults_to_event_driven_when_streaming(self, seq_mock, ed_mock):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        controller.run_full_pipeline()

        ed_mock.assert_called_once()
        seq_mock.assert_not_called()

    @patch.object(Controller, '_run_event_driven_pipeline')
    @patch.object(Controller, '_run_sequential_pipeline')
    def test_falls_back_to_sequential_when_disabled(self, seq_mock, ed_mock):
        spark = _make_mock_spark()
        config = get_config({
            'streaming_enabled': True,
            'event_driven_chaining': False,
            'autoset_spark_configs': False,
        })
        controller = Controller(spark, config, _make_table_registry())

        controller.run_full_pipeline()

        seq_mock.assert_called_once()
        ed_mock.assert_not_called()

    @patch.object(Controller, '_run_event_driven_pipeline')
    @patch.object(Controller, '_run_sequential_pipeline')
    def test_falls_back_to_sequential_when_batch(self, seq_mock, ed_mock):
        spark = _make_mock_spark()
        config = get_config({
            'streaming_enabled': False,
            'autoset_spark_configs': False,
        })
        controller = Controller(spark, config, _make_table_registry())

        controller.run_full_pipeline()

        seq_mock.assert_called_once()
        ed_mock.assert_not_called()

    @patch.object(Controller, '_run_event_driven_pipeline')
    @patch.object(Controller, '_run_sequential_pipeline')
    def test_falls_back_to_sequential_for_single_zone(self, seq_mock, ed_mock):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        controller.run_full_pipeline(zones=['bronze'])

        seq_mock.assert_called_once()
        ed_mock.assert_not_called()


class TestEventDrivenPipelineSetup:
    @patch.object(Controller, 'run_zone_pipeline', return_value=[])
    def test_registers_listener_with_spark(self, rzp_mock):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        controller._run_event_driven_pipeline(['bronze', 'silver'], False, False)

        spark.streams.addListener.assert_called_once()
        assert controller._chain_listener is not None

    @patch.object(Controller, 'run_zone_pipeline', return_value=[])
    def test_no_recovery_spawn_calls(self, rzp_mock):
        """Event-driven pipeline relies on first-batch cascade, not recovery spawns."""
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        with patch.object(controller, '_spawn_zone_internal') as spawn_mock:
            controller._run_event_driven_pipeline(
                ['bronze', 'silver', 'gold'], False, False
            )
            spawn_mock.assert_not_called()

    @patch.object(Controller, 'run_zone_pipeline', return_value=[])
    def test_starts_first_zone_via_run_zone_pipeline(self, rzp_mock):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        controller._run_event_driven_pipeline(['bronze', 'silver'], False, False)

        rzp_mock.assert_called_once_with('bronze')

    @patch.object(Controller, 'run_zone_pipeline')
    def test_registers_queries_with_table_name(self, rzp_mock):
        """First-zone queries are registered with the listener including table_name."""
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        # Simulate run_zone_pipeline returning a query
        mock_query = MagicMock()
        mock_query.name = "bronze_orders_stream"
        mock_query.id = "q-1"
        rzp_mock.return_value = [mock_query]

        controller._run_event_driven_pipeline(['bronze', 'silver'], False, False)

        # Verify the listener has the query registered with table_name
        listener = controller._chain_listener
        assert listener._query_table.get("bronze_orders_stream") == "orders"


class TestStopAllCleansUpListener:
    def test_removes_listener_on_stop(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        # Simulate having a listener
        listener = MagicMock()
        controller._chain_listener = listener
        controller.stop_all()

        spark.streams.removeListener.assert_called_once_with(listener)

    def test_stop_all_without_listener_is_safe(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        # No listener set
        controller.stop_all()  # Should not raise

        spark.streams.removeListener.assert_not_called()

    def test_clears_listener_reference_on_stop(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        controller._chain_listener = MagicMock()
        controller.stop_all()

        assert controller._chain_listener is None

    def test_resets_terminated_queries(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        controller.stop_all()

        spark.streams.resetTerminated.assert_called_once()

    def test_clears_stream_manager_state(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        # Register a mock query
        mock_query = MagicMock()
        mock_query.isActive = False
        controller.stream_manager.register(mock_query, zone='bronze')
        assert len(controller.stream_manager.queries) == 1

        controller.stop_all()

        assert len(controller.stream_manager.queries) == 0
        assert len(controller.stream_manager._zone_queries) == 0


class TestSequentialPipelineFallback:
    @patch.object(Controller, 'run_zone_pipeline', return_value=[])
    def test_sequential_processes_zones_in_order(self, rzp_mock):
        spark = _make_mock_spark()
        config = get_config({
            'streaming_enabled': True,
            'event_driven_chaining': False,
            'autoset_spark_configs': False,
        })
        controller = Controller(spark, config, _make_table_registry())

        controller.run_full_pipeline(zones=['bronze', 'silver'])

        assert rzp_mock.call_count == 2
        calls = rzp_mock.call_args_list
        assert calls[0].kwargs.get('source_zone') is None
        assert calls[1].kwargs.get('source_zone') == 'bronze'


class TestConfigDefault:
    def test_event_driven_chaining_defaults_true(self):
        config = get_config()
        assert config['event_driven_chaining'] is True

    def test_event_driven_chaining_overrideable(self):
        config = get_config({'event_driven_chaining': False})
        assert config['event_driven_chaining'] is False


class TestSpawnTable:
    def test_spawn_table_creates_pipeline_and_processes(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        with patch('arcflow.controller.ZonePipeline') as MockPipeline:
            mock_query = MagicMock()
            mock_query.name = "silver_orders_stream"
            MockPipeline.return_value.process_table.return_value = mock_query

            result = controller._spawn_table('silver', 'orders')

            assert result == mock_query
            MockPipeline.assert_called_once_with(
                spark=spark, zone='silver', config=config,
            )

    def test_spawn_table_returns_none_for_unknown_table(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        result = controller._spawn_table('silver', 'nonexistent')
        assert result is None

    def test_spawn_table_returns_none_for_disabled_zone(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        schema = StructType([StructField("id", StringType())])
        registry = {
            "orders": FlowConfig(
                name="orders", schema=schema, format="parquet",
                zones={"bronze": StageConfig(enabled=True)},
            ),
        }
        controller = Controller(spark, config, registry)

        result = controller._spawn_table('silver', 'orders')
        assert result is None


class TestGetQueryTableName:
    def test_extracts_table_name(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        assert controller._get_query_table_name("bronze_orders_stream", "bronze") == "orders"

    def test_returns_none_for_unknown(self):
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        controller = Controller(spark, config, _make_table_registry())

        assert controller._get_query_table_name("bronze_unknown_stream", "bronze") is None

    def test_no_substring_collision(self):
        """'shipment' should not match 'bronze_shipment_scan_event_stream'."""
        spark = _make_mock_spark()
        config = get_config({'streaming_enabled': True, 'autoset_spark_configs': False})
        schema = StructType([StructField("id", StringType())])
        registry = {
            "shipment": FlowConfig(
                name="shipment", schema=schema, format="parquet",
                zones={"bronze": StageConfig(enabled=True)},
            ),
            "shipment_scan_event": FlowConfig(
                name="shipment_scan_event", schema=schema, format="parquet",
                zones={"bronze": StageConfig(enabled=True)},
            ),
        }
        controller = Controller(spark, config, registry)

        assert controller._get_query_table_name(
            "bronze_shipment_scan_event_stream", "bronze"
        ) == "shipment_scan_event"
        assert controller._get_query_table_name(
            "bronze_shipment_stream", "bronze"
        ) == "shipment"

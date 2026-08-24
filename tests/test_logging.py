import logging
from unittest.mock import MagicMock, patch

from pyspark.sql.types import StringType, StructField, StructType

from arcflow.config import get_config
from arcflow.controller import Controller
from arcflow.core.stage_chain_listener import StageChainListener
from arcflow.models import FlowConfig, StageConfig
from arcflow.pipelines.zone_pipeline import ZonePipeline


_SCHEMA = StructType([StructField("id", StringType())])


def _flow(trigger_mode="availableNow"):
    return FlowConfig(
        name="orders",
        schema=_SCHEMA,
        zones={
            "bronze": StageConfig(),
            "silver": StageConfig(),
        },
        trigger_mode=trigger_mode,
        trigger_interval="30 seconds" if trigger_mode == "processingTime" else None,
    )


def test_processing_time_root_stream_is_logged_at_info(caplog):
    pipeline = ZonePipeline(
        MagicMock(),
        "bronze",
        get_config({"streaming_enabled": True}),
    )
    query = MagicMock()

    with (
        patch.object(pipeline, "_claim_query", return_value=True),
        patch.object(pipeline, "read_source", return_value=MagicMock()),
        patch.object(pipeline, "apply_transformations", return_value=MagicMock()),
        patch.object(pipeline, "write_target", return_value=query),
        caplog.at_level(logging.INFO, logger="arcflow.pipelines.zone_pipeline"),
    ):
        pipeline.process_table(_flow(trigger_mode="processingTime"))

    assert (
        "Input stream started: bronze.orders "
        "trigger=processingTime interval=30 seconds"
    ) in caplog.messages


def test_available_now_root_stream_has_no_per_stream_info(caplog):
    pipeline = ZonePipeline(
        MagicMock(),
        "bronze",
        get_config({"streaming_enabled": True}),
    )

    with (
        patch.object(pipeline, "_claim_query", return_value=True),
        patch.object(pipeline, "read_source", return_value=MagicMock()),
        patch.object(pipeline, "apply_transformations", return_value=MagicMock()),
        patch.object(pipeline, "write_target", return_value=MagicMock()),
        caplog.at_level(logging.INFO, logger="arcflow.pipelines.zone_pipeline"),
    ):
        pipeline.process_table(_flow())

    assert not any("Input stream started" in message for message in caplog.messages)


def test_stage_chain_transitions_are_debug_only(caplog):
    listener = StageChainListener(
        ["bronze", "silver"],
        MagicMock(),
    )

    with (
        patch.object(listener._executor, "submit", return_value=MagicMock()),
        caplog.at_level(logging.DEBUG, logger="arcflow.core.stage_chain_listener"),
    ):
        listener._try_spawn_downstream_table("silver", "orders")

    transition_records = [
        record for record in caplog.records
        if "spawning silver.orders" in record.getMessage()
    ]
    assert len(transition_records) == 1
    assert transition_records[0].levelno == logging.DEBUG
    listener.shutdown(wait=False)


def test_controller_logs_one_pipeline_start_summary(caplog):
    spark = MagicMock()
    spark.streams.active = []
    controller = Controller(
        spark,
        get_config({
            "streaming_enabled": True,
            "autoset_spark_configs": False,
        }),
        {"orders": _flow()},
    )

    with (
        patch.object(controller, "_run_event_driven_pipeline"),
        caplog.at_level(logging.INFO, logger="arcflow.controller"),
    ):
        controller.run_full_pipeline(zones=["bronze", "silver"])

    start_messages = [
        message for message in caplog.messages
        if message.startswith("Starting ArcFlow pipeline:")
    ]
    assert start_messages == [
        "Starting ArcFlow pipeline: mode=event-driven, "
        "zones=['bronze', 'silver'], tables=1"
    ]

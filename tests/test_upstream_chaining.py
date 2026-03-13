"""Tests for test_input / test_output upstream transform chaining."""
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from arcflow.models import FlowConfig, StageConfig
from arcflow.pipelines.zone_pipeline import ZonePipeline
from pyspark.sql.types import StructType, StructField, StringType


_SCHEMA = StructType([StructField("id", StringType())])
_CONFIG = {"streaming_enabled": False}


def _flow(name="item", zones=None, fmt="parquet", **kwargs):
    return FlowConfig(
        name=name, schema=_SCHEMA, format=fmt, zones=zones or {}, **kwargs
    )


# ── _get_upstream_zones ─────────────────────────────────────────────


class TestGetUpstreamZones:
    def test_first_zone_returns_empty(self):
        fc = _flow(zones={"bronze": StageConfig(), "silver": StageConfig()})
        pipeline = ZonePipeline(MagicMock(), zone="bronze", config=_CONFIG)
        assert pipeline._get_upstream_zones(fc) == []

    def test_second_zone_returns_first(self):
        fc = _flow(zones={"bronze": StageConfig(), "silver": StageConfig()})
        pipeline = ZonePipeline(MagicMock(), zone="silver", config=_CONFIG)
        assert pipeline._get_upstream_zones(fc) == ["bronze"]

    def test_third_zone_returns_first_two(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver": StageConfig(),
            "gold": StageConfig(),
        })
        pipeline = ZonePipeline(MagicMock(), zone="gold", config=_CONFIG)
        assert pipeline._get_upstream_zones(fc) == ["bronze", "silver"]

    def test_branches_excluded_from_upstream(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
            "silver": StageConfig(),
        })
        pipeline = ZonePipeline(MagicMock(), zone="silver", config=_CONFIG)
        assert pipeline._get_upstream_zones(fc) == ["bronze"]

    def test_branch_zone_returns_empty(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
        })
        pipeline = ZonePipeline(MagicMock(), zone="raw_archive", config=_CONFIG)
        assert pipeline._get_upstream_zones(fc) == []

    def test_single_zone_returns_empty(self):
        fc = _flow(zones={"bronze": StageConfig()})
        pipeline = ZonePipeline(MagicMock(), zone="bronze", config=_CONFIG)
        assert pipeline._get_upstream_zones(fc) == []


# ── _apply_upstream_transforms ──────────────────────────────────────


class TestApplyUpstreamTransforms:
    def test_no_upstream_returns_df_unchanged(self):
        fc = _flow(zones={"bronze": StageConfig()})
        pipeline = ZonePipeline(MagicMock(), zone="bronze", config=_CONFIG)
        mock_df = MagicMock()
        result = pipeline._apply_upstream_transforms(mock_df, fc)
        assert result is mock_df

    def test_applies_bronze_transform_for_silver(self):
        fc = _flow(zones={
            "bronze": StageConfig(custom_transform="explode_data"),
            "silver": StageConfig(custom_transform="silver_item"),
        })
        pipeline = ZonePipeline(MagicMock(), zone="silver", config=_CONFIG)
        mock_df = MagicMock()
        transformed_df = MagicMock()

        with patch.object(pipeline, "apply_transformations", return_value=transformed_df) as mock_apply:
            result = pipeline._apply_upstream_transforms(mock_df, fc)

        mock_apply.assert_called_once_with(mock_df, fc, fc.zones["bronze"])
        assert result is transformed_df

    def test_chains_two_upstream_zones_for_gold(self):
        fc = _flow(zones={
            "bronze": StageConfig(custom_transform="t1"),
            "silver": StageConfig(custom_transform="t2"),
            "gold": StageConfig(custom_transform="t3"),
        })
        pipeline = ZonePipeline(MagicMock(), zone="gold", config=_CONFIG)

        df0 = MagicMock(name="original")
        df1 = MagicMock(name="after_bronze")
        df2 = MagicMock(name="after_silver")

        with patch.object(pipeline, "apply_transformations", side_effect=[df1, df2]) as mock_apply:
            result = pipeline._apply_upstream_transforms(df0, fc)

        assert mock_apply.call_count == 2
        mock_apply.assert_any_call(df0, fc, fc.zones["bronze"])
        mock_apply.assert_any_call(df1, fc, fc.zones["silver"])
        assert result is df2

    def test_skips_disabled_upstream_zone(self):
        fc = _flow(zones={
            "bronze": StageConfig(enabled=False, custom_transform="t1"),
            "silver": StageConfig(custom_transform="t2"),
        })
        pipeline = ZonePipeline(MagicMock(), zone="silver", config=_CONFIG)
        mock_df = MagicMock()

        with patch.object(pipeline, "apply_transformations") as mock_apply:
            result = pipeline._apply_upstream_transforms(mock_df, fc)

        mock_apply.assert_not_called()
        assert result is mock_df


# ── test_input chaining ─────────────────────────────────────────────


class TestTestInputChaining:
    def test_bronze_test_input_no_upstream(self):
        """Bronze test_input should NOT apply any upstream transforms."""
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver": StageConfig(),
        })
        pipeline = ZonePipeline(MagicMock(), zone="bronze", config=_CONFIG)

        mock_df = MagicMock()
        mock_df.limit.return_value = mock_df

        with patch.object(pipeline, "read_source", return_value=mock_df), \
             patch.object(pipeline, "_apply_upstream_transforms", return_value=mock_df) as mock_upstream:
            pipeline.test_input(fc)

        mock_upstream.assert_called_once_with(mock_df, fc)

    def test_silver_test_input_chains_bronze(self):
        """Silver test_input should apply bronze upstream transforms."""
        fc = _flow(zones={
            "bronze": StageConfig(custom_transform="explode_data"),
            "silver": StageConfig(custom_transform="silver_item"),
        })
        pipeline = ZonePipeline(MagicMock(), zone="silver", config=_CONFIG)

        source_df = MagicMock()
        transformed_df = MagicMock()
        transformed_df.limit.return_value = transformed_df

        with patch.object(pipeline, "read_source", return_value=source_df), \
             patch.object(pipeline, "_apply_upstream_transforms", return_value=transformed_df) as mock_upstream:
            result = pipeline.test_input(fc)

        mock_upstream.assert_called_once_with(source_df, fc)
        assert result is transformed_df

    def test_streaming_silver_test_input_chains_bronze(self):
        """Silver test_input for streaming sources should also chain upstream."""
        fc = _flow(
            zones={
                "bronze": StageConfig(custom_transform="explode_data"),
                "silver": StageConfig(custom_transform="silver_item"),
            },
            fmt="eventhub",
            source_uri="Endpoint=sb://test;EntityPath=topic",
        )
        pipeline = ZonePipeline(MagicMock(), zone="silver", config={**_CONFIG, "streaming_enabled": True})

        streaming_df = MagicMock()
        upstream_df = MagicMock()
        mock_reader = MagicMock()
        mock_reader.read.return_value = streaming_df

        with patch("arcflow.pipelines.zone_pipeline.StreamEndpointValidator") as mock_validator, \
             patch("arcflow.pipelines.zone_pipeline.ReaderFactory") as mock_rf, \
             patch.object(pipeline, "_apply_upstream_transforms", return_value=upstream_df) as mock_upstream, \
             patch.object(pipeline, "_stream_to_memory", return_value=MagicMock()) as mock_mem:
            mock_validator.validate.return_value = MagicMock(valid=True)
            mock_rf.return_value.create_reader.return_value = mock_reader
            pipeline.test_input(fc, raw=False)

        mock_upstream.assert_called_once_with(streaming_df, fc)

    def test_streaming_raw_skips_upstream(self):
        """raw=True should skip upstream transforms (no schema to transform)."""
        fc = _flow(
            zones={
                "bronze": StageConfig(custom_transform="explode_data"),
                "silver": StageConfig(),
            },
            fmt="kafka",
            source_uri="Endpoint=sb://test;EntityPath=topic",
        )
        pipeline = ZonePipeline(MagicMock(), zone="silver", config={**_CONFIG, "streaming_enabled": True})

        streaming_df = MagicMock()
        mock_reader = MagicMock()
        mock_reader.read.return_value = streaming_df

        with patch("arcflow.pipelines.zone_pipeline.StreamEndpointValidator") as mock_validator, \
             patch("arcflow.pipelines.zone_pipeline.ReaderFactory") as mock_rf, \
             patch.object(pipeline, "_apply_upstream_transforms") as mock_upstream, \
             patch.object(pipeline, "_stream_to_memory", return_value=MagicMock()):
            mock_validator.validate.return_value = MagicMock(valid=True)
            mock_rf.return_value.create_reader.return_value = mock_reader
            pipeline.test_input(fc, raw=True)

        mock_upstream.assert_not_called()


# ── test_output chaining ────────────────────────────────────────────


class TestTestOutputChaining:
    def test_bronze_test_output_applies_own_transform_only(self):
        fc = _flow(zones={
            "bronze": StageConfig(custom_transform="explode_data"),
            "silver": StageConfig(),
        })
        pipeline = ZonePipeline(MagicMock(), zone="bronze", config=_CONFIG)

        source_df = MagicMock()
        upstream_df = MagicMock()
        final_df = MagicMock()
        final_df.limit.return_value = final_df

        with patch.object(pipeline, "read_source", return_value=source_df), \
             patch.object(pipeline, "_apply_upstream_transforms", return_value=upstream_df) as mock_up, \
             patch.object(pipeline, "apply_transformations", return_value=final_df) as mock_apply:
            result = pipeline.test_output(fc)

        mock_up.assert_called_once_with(source_df, fc)
        mock_apply.assert_called_once_with(upstream_df, fc, fc.zones["bronze"])
        assert result is final_df

    def test_silver_test_output_chains_then_applies_own(self):
        fc = _flow(zones={
            "bronze": StageConfig(custom_transform="explode_data"),
            "silver": StageConfig(custom_transform="silver_item"),
        })
        pipeline = ZonePipeline(MagicMock(), zone="silver", config=_CONFIG)

        source_df = MagicMock()
        upstream_df = MagicMock()
        final_df = MagicMock()
        final_df.limit.return_value = final_df

        with patch.object(pipeline, "read_source", return_value=source_df), \
             patch.object(pipeline, "_apply_upstream_transforms", return_value=upstream_df) as mock_up, \
             patch.object(pipeline, "apply_transformations", return_value=final_df) as mock_apply:
            result = pipeline.test_output(fc)

        mock_up.assert_called_once_with(source_df, fc)
        mock_apply.assert_called_once_with(upstream_df, fc, fc.zones["silver"])

    def test_streaming_silver_test_output_chains_upstream(self):
        fc = _flow(
            zones={
                "bronze": StageConfig(custom_transform="explode_data"),
                "silver": StageConfig(custom_transform="silver_item"),
            },
            fmt="eventhub",
            source_uri="Endpoint=sb://test;EntityPath=topic",
        )
        pipeline = ZonePipeline(MagicMock(), zone="silver", config={**_CONFIG, "streaming_enabled": True})

        streaming_df = MagicMock()
        upstream_df = MagicMock()
        final_df = MagicMock()
        mock_reader = MagicMock()
        mock_reader.read.return_value = streaming_df

        with patch("arcflow.pipelines.zone_pipeline.StreamEndpointValidator") as mock_validator, \
             patch("arcflow.pipelines.zone_pipeline.ReaderFactory") as mock_rf, \
             patch.object(pipeline, "_apply_upstream_transforms", return_value=upstream_df) as mock_up, \
             patch.object(pipeline, "apply_transformations", return_value=final_df), \
             patch.object(pipeline, "_stream_to_memory", return_value=MagicMock()):
            mock_validator.validate.return_value = MagicMock(valid=True)
            mock_rf.return_value.create_reader.return_value = mock_reader
            pipeline.test_output(fc)

        mock_up.assert_called_once_with(streaming_df, fc)


# ── write_target downstream trigger enforcement ─────────────────────


class TestWriteTargetDownstreamTrigger:
    """write_target forces availableNow for any zone reading from a prior zone."""

    def test_root_zone_uses_configured_trigger(self):
        """Bronze (root) should NOT have _trigger_mode_override."""
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver": StageConfig(),
        }, trigger_mode="processingTime", trigger_interval="5 seconds")
        config = {"streaming_enabled": False, "checkpoint_uri": "/tmp"}
        pipeline = ZonePipeline(MagicMock(), zone="bronze", config=config)

        captured_config = {}

        def mock_create_writer(tc, zc):
            writer = MagicMock()
            writer.write.return_value = MagicMock()
            return writer

        with patch("arcflow.pipelines.zone_pipeline.WriterFactory") as mock_wf:
            mock_wf.return_value.create_writer = mock_create_writer
            # Capture the config passed to WriterFactory
            def capture_factory(spark, is_streaming, cfg):
                captured_config.update(cfg)
                return MagicMock(create_writer=mock_create_writer)
            mock_wf.side_effect = capture_factory

            pipeline.write_target(MagicMock(), fc, fc.zones["bronze"])

        assert "_trigger_mode_override" not in captured_config

    def test_downstream_zone_forced_available_now(self):
        """Silver (downstream) should have _trigger_mode_override='availableNow'."""
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver": StageConfig(),
        }, trigger_mode="processingTime", trigger_interval="5 seconds")
        config = {"streaming_enabled": False, "checkpoint_uri": "/tmp"}
        pipeline = ZonePipeline(MagicMock(), zone="silver", config=config)

        captured_config = {}

        def mock_create_writer(tc, zc):
            writer = MagicMock()
            writer.write.return_value = MagicMock()
            return writer

        with patch("arcflow.pipelines.zone_pipeline.WriterFactory") as mock_wf:
            def capture_factory(spark, is_streaming, cfg):
                captured_config.update(cfg)
                return MagicMock(create_writer=mock_create_writer)
            mock_wf.side_effect = capture_factory

            pipeline.write_target(MagicMock(), fc, fc.zones["silver"])

        assert captured_config.get("_trigger_mode_override") == "availableNow"

    def test_third_zone_also_forced_available_now(self):
        """Gold (third zone) should also be forced to availableNow."""
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver": StageConfig(),
            "gold": StageConfig(),
        }, trigger_mode="processingTime", trigger_interval="5 seconds")
        config = {"streaming_enabled": False, "checkpoint_uri": "/tmp"}
        pipeline = ZonePipeline(MagicMock(), zone="gold", config=config)

        captured_config = {}

        def mock_create_writer(tc, zc):
            writer = MagicMock()
            writer.write.return_value = MagicMock()
            return writer

        with patch("arcflow.pipelines.zone_pipeline.WriterFactory") as mock_wf:
            def capture_factory(spark, is_streaming, cfg):
                captured_config.update(cfg)
                return MagicMock(create_writer=mock_create_writer)
            mock_wf.side_effect = capture_factory

            pipeline.write_target(MagicMock(), fc, fc.zones["gold"])

        assert captured_config.get("_trigger_mode_override") == "availableNow"

    def test_branch_reading_root_uses_configured_trigger(self):
        """A branch with stage_input=FlowConfig.name reads root — no override."""
        fc = _flow(zones={
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
        }, trigger_mode="processingTime", trigger_interval="5 seconds")
        config = {"streaming_enabled": False, "checkpoint_uri": "/tmp"}
        pipeline = ZonePipeline(MagicMock(), zone="raw_archive", config=config)

        captured_config = {}

        def mock_create_writer(tc, zc):
            writer = MagicMock()
            writer.write.return_value = MagicMock()
            return writer

        with patch("arcflow.pipelines.zone_pipeline.WriterFactory") as mock_wf:
            def capture_factory(spark, is_streaming, cfg):
                captured_config.update(cfg)
                return MagicMock(create_writer=mock_create_writer)
            mock_wf.side_effect = capture_factory

            pipeline.write_target(MagicMock(), fc, fc.zones["raw_archive"])

        assert "_trigger_mode_override" not in captured_config

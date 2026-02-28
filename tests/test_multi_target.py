"""Tests for multi-target output via stage_input."""
from unittest.mock import MagicMock, patch, call
import pytest

from arcflow.models import FlowConfig, StageConfig
from arcflow.controller import Controller
from pyspark.sql.types import StructType, StructField, StringType


_SCHEMA = StructType([StructField("id", StringType())])


def _flow(name="item", zones=None, **kwargs):
    return FlowConfig(name=name, schema=_SCHEMA, format="parquet", zones=zones or {}, **kwargs)


# ── StageConfig field defaults ──────────────────────────────────────


class TestStageConfigFields:
    def test_defaults_are_none(self):
        cfg = StageConfig()
        assert cfg.stage_input is None
        assert cfg.table_name is None
        assert cfg.schema_name is None

    def test_custom_values(self):
        cfg = StageConfig(stage_input="bronze", table_name="orders", schema_name="archive")
        assert cfg.stage_input == "bronze"
        assert cfg.table_name == "orders"
        assert cfg.schema_name == "archive"


# ── Input resolution ────────────────────────────────────────────────


class TestResolveStageInput:
    def test_first_main_chain_resolves_to_root(self):
        zones = {"bronze": StageConfig(), "silver": StageConfig()}
        result = Controller._resolve_stage_input("bronze", zones["bronze"], zones, "item")
        assert result == ("root", "item")

    def test_second_main_chain_resolves_to_previous(self):
        zones = {"bronze": StageConfig(), "silver": StageConfig()}
        result = Controller._resolve_stage_input("silver", zones["silver"], zones, "item")
        assert result == ("stage", "bronze")

    def test_explicit_flow_name_resolves_to_root(self):
        zones = {
            "bronze": StageConfig(),
            "raw": StageConfig(stage_input="item"),
        }
        result = Controller._resolve_stage_input("raw", zones["raw"], zones, "item")
        assert result == ("root", "item")

    def test_explicit_stage_name_resolves_to_stage(self):
        zones = {
            "bronze": StageConfig(),
            "silver": StageConfig(),
            "silver_b": StageConfig(stage_input="bronze"),
        }
        result = Controller._resolve_stage_input("silver_b", zones["silver_b"], zones, "item")
        assert result == ("stage", "bronze")

    def test_branch_skipped_in_main_chain(self):
        """silver should read from bronze even though raw_archive sits between them."""
        zones = {
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
            "silver": StageConfig(),
        }
        result = Controller._resolve_stage_input("silver", zones["silver"], zones, "item")
        assert result == ("stage", "bronze")

    def test_invalid_stage_input_raises(self):
        zones = {"bronze": StageConfig(stage_input="nonexistent")}
        with pytest.raises(ValueError, match="not a valid stage name"):
            Controller._resolve_stage_input("bronze", zones["bronze"], zones, "item")

    def test_three_zone_main_chain(self):
        zones = {"bronze": StageConfig(), "silver": StageConfig(), "gold": StageConfig()}
        assert Controller._resolve_stage_input("gold", zones["gold"], zones, "item") == ("stage", "silver")

    def test_branch_between_second_and_third(self):
        zones = {
            "bronze": StageConfig(),
            "silver": StageConfig(),
            "silver_extra": StageConfig(stage_input="bronze"),
            "gold": StageConfig(),
        }
        # gold should read from silver (previous primary), not silver_extra (branch)
        assert Controller._resolve_stage_input("gold", zones["gold"], zones, "item") == ("stage", "silver")


# ── Stage grouping ──────────────────────────────────────────────────


class TestBuildStageGroups:
    def test_single_stage_single_group(self):
        fc = _flow(zones={"bronze": StageConfig()})
        groups = Controller._build_stage_groups(fc)
        assert len(groups) == 1
        assert groups[0][0][0] == "bronze"

    def test_main_chain_yields_separate_groups(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver": StageConfig(),
        })
        groups = Controller._build_stage_groups(fc)
        assert len(groups) == 2
        assert groups[0][0][0] == "bronze"
        assert groups[1][0][0] == "silver"

    def test_root_fanout_groups_together(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
            "silver": StageConfig(),
        })
        groups = Controller._build_stage_groups(fc)
        # Group 1: bronze + raw_archive (both root)
        # Group 2: silver
        assert len(groups) == 2
        root_group = groups[0]
        names = [s[0] for s in root_group]
        assert "bronze" in names
        assert "raw_archive" in names
        assert groups[1][0][0] == "silver"

    def test_mid_pipeline_fanout(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver_a": StageConfig(),
            "silver_b": StageConfig(stage_input="bronze"),
        })
        groups = Controller._build_stage_groups(fc)
        # bronze = root, silver_a + silver_b both read from bronze
        assert len(groups) == 2
        bronze_group = groups[0]
        assert len(bronze_group) == 1
        assert bronze_group[0][0] == "bronze"

        silver_group = groups[1]
        names = [s[0] for s in silver_group]
        assert "silver_a" in names
        assert "silver_b" in names

    def test_disabled_stages_excluded(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "archive": StageConfig(stage_input="item", enabled=False),
        })
        groups = Controller._build_stage_groups(fc)
        assert len(groups) == 1
        assert len(groups[0]) == 1
        assert groups[0][0][0] == "bronze"

    def test_root_groups_before_stage_groups(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver": StageConfig(),
            "raw": StageConfig(stage_input="item"),
        })
        groups = Controller._build_stage_groups(fc)
        # First group should be root (bronze + raw), second should be stage (silver)
        root_names = [s[0] for s in groups[0]]
        assert "bronze" in root_names or "raw" in root_names
        # silver should come after root groups
        stage_names = [s[0] for g in groups[1:] for s in g]
        assert "silver" in stage_names


# ── ZonePipeline._get_source_zone with stage_input ─────────────────


class TestGetSourceZoneWithStageInput:
    def test_first_main_chain_returns_none(self):
        from arcflow.pipelines.zone_pipeline import ZonePipeline
        pipeline = ZonePipeline(MagicMock(), "bronze", {})
        fc = _flow(zones={"bronze": StageConfig(), "silver": StageConfig()})
        assert pipeline._get_source_zone(fc) is None

    def test_branch_with_flow_name_returns_none(self):
        from arcflow.pipelines.zone_pipeline import ZonePipeline
        pipeline = ZonePipeline(MagicMock(), "raw_archive", {})
        fc = _flow(zones={
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
            "silver": StageConfig(),
        })
        assert pipeline._get_source_zone(fc) is None

    def test_branch_with_stage_name(self):
        from arcflow.pipelines.zone_pipeline import ZonePipeline
        pipeline = ZonePipeline(MagicMock(), "silver_b", {})
        fc = _flow(zones={
            "bronze": StageConfig(),
            "silver_a": StageConfig(),
            "silver_b": StageConfig(stage_input="bronze"),
        })
        assert pipeline._get_source_zone(fc) == "bronze"

    def test_main_chain_skips_branches(self):
        from arcflow.pipelines.zone_pipeline import ZonePipeline
        pipeline = ZonePipeline(MagicMock(), "silver", {})
        fc = _flow(zones={
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
            "silver": StageConfig(),
        })
        # silver should read from bronze, not raw_archive
        assert pipeline._get_source_zone(fc) == "bronze"

    def test_invalid_stage_input_raises(self):
        from arcflow.pipelines.zone_pipeline import ZonePipeline
        pipeline = ZonePipeline(MagicMock(), "bad", {})
        fc = _flow(zones={"bad": StageConfig(stage_input="nonexistent")})
        with pytest.raises(ValueError, match="not a valid stage name"):
            pipeline._get_source_zone(fc)

    def test_zone_not_in_config_raises(self):
        from arcflow.pipelines.zone_pipeline import ZonePipeline
        pipeline = ZonePipeline(MagicMock(), "gold", {})
        fc = _flow(zones={"bronze": StageConfig()})
        with pytest.raises(ValueError, match="not found"):
            pipeline._get_source_zone(fc)


# ── DeltaWriter multi-target helpers ────────────────────────────────


class TestDeltaWriterMultiTarget:
    def _make_writer(self, is_streaming=True):
        from arcflow.writers.delta_writer import DeltaWriter
        spark = MagicMock()
        spark.streams.active = []
        writer = DeltaWriter(spark, is_streaming, {"checkpoint_uri": "/tmp"})
        return writer

    def test_resolve_target_uses_overrides(self):
        writer = self._make_writer()
        cfg = StageConfig(table_name="orders", schema_name="archive")
        fc = _flow(name="item")
        ref, schema = writer._resolve_target("bronze", cfg, fc, "bronze")
        assert "archive" in ref
        assert "orders" in ref
        assert schema == "archive"

    def test_resolve_target_defaults(self):
        writer = self._make_writer()
        cfg = StageConfig()
        fc = _flow(name="item")
        ref, schema = writer._resolve_target("bronze", cfg, fc, "bronze")
        assert "item" in ref
        assert schema == "bronze"

    def test_apply_stage_transforms_with_custom(self):
        writer = self._make_writer()
        df = MagicMock()
        # Patch the transformer lookup
        with patch("arcflow.writers.delta_writer.has_zone_transformer", return_value=True), \
             patch("arcflow.writers.delta_writer.get_zone_transformer") as mock_get, \
             patch("arcflow.writers.delta_writer.normalize_columns_to_snake_case", side_effect=lambda x: x), \
             patch("arcflow.writers.delta_writer.apply_processing_timestamp", side_effect=lambda x: x):
            mock_transform = MagicMock(return_value=df)
            mock_get.return_value = mock_transform
            cfg = StageConfig(custom_transform="my_transform")
            result = writer._apply_stage_transforms(df, cfg)
            mock_transform.assert_called_once_with(df)

    def test_apply_stage_transforms_without_custom(self):
        writer = self._make_writer()
        df = MagicMock()
        with patch("arcflow.writers.delta_writer.normalize_columns_to_snake_case", side_effect=lambda x: x) as mock_snake, \
             patch("arcflow.writers.delta_writer.apply_processing_timestamp", side_effect=lambda x: x) as mock_ts:
            cfg = StageConfig()
            writer._apply_stage_transforms(df, cfg)
            mock_snake.assert_called_once()
            mock_ts.assert_called_once()

    def test_write_single_target_append(self):
        writer = self._make_writer()
        batch_df = MagicMock()
        cfg = StageConfig(mode="append")
        writer._write_single_target(batch_df, cfg, "bronze.item")
        batch_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with("bronze.item")

    def test_write_single_target_upsert_calls_merge(self):
        writer = self._make_writer()
        batch_df = MagicMock()
        cfg = StageConfig(mode="upsert", merge_keys=["id"])
        with patch.object(writer, "_merge_batch") as mock_merge:
            writer._write_single_target(batch_df, cfg, "bronze.item")
            mock_merge.assert_called_once_with(batch_df, ["id"], "bronze.item")


# ── DeltaWriter.write() with schema_name/table_name fields ─────────


class TestDeltaWriterFieldResolution:
    def test_write_uses_stage_config_schema_name(self):
        from arcflow.writers.delta_writer import DeltaWriter
        spark = MagicMock()
        spark.streams.active = []
        writer = DeltaWriter(spark, is_streaming=False, config={"checkpoint_uri": "/tmp"})

        fc = _flow(name="item", zones={"bronze": StageConfig(schema_name="archive")})
        zone_config = StageConfig(schema_name="archive")
        df = MagicMock()

        with patch.object(writer, "create_schema_if_not_exists"), \
             patch.object(writer, "_write_batch") as mock_batch:
            writer.write(df, fc, zone_config, "bronze")
            # table_reference should use 'archive' schema, not 'bronze'
            args = mock_batch.call_args
            table_ref = args[0][2]  # third positional arg is table_reference
            assert "archive" in table_ref

    def test_write_uses_stage_config_table_name(self):
        from arcflow.writers.delta_writer import DeltaWriter
        spark = MagicMock()
        spark.streams.active = []
        writer = DeltaWriter(spark, is_streaming=False, config={"checkpoint_uri": "/tmp"})

        fc = _flow(name="item", zones={"bronze": StageConfig(table_name="orders")})
        zone_config = StageConfig(table_name="orders")
        df = MagicMock()

        with patch.object(writer, "create_schema_if_not_exists"), \
             patch.object(writer, "_write_batch") as mock_batch:
            writer.write(df, fc, zone_config, "bronze")
            args = mock_batch.call_args
            table_ref = args[0][2]
            assert "orders" in table_ref


# ── Controller.run_zone_pipeline with multi-target ──────────────────


class TestControllerMultiTarget:
    def _make_controller(self, table_registry):
        spark = MagicMock()
        spark.streams.active = []
        config = {
            "streaming_enabled": True,
            "autoset_spark_configs": False,
            "event_driven_chaining": False,
        }
        return Controller(spark, config, table_registry)

    def test_single_stage_uses_process_table(self):
        fc = _flow(zones={"bronze": StageConfig()})
        ctrl = self._make_controller({"item": fc})

        with patch("arcflow.controller.ZonePipeline") as MockPipeline:
            mock_pipeline = MagicMock()
            mock_query = MagicMock()
            mock_query.name = "bronze_item_stream"
            mock_query.isActive = True
            mock_pipeline.process_table.return_value = mock_query
            MockPipeline.return_value = mock_pipeline

            queries = ctrl.run_zone_pipeline("bronze")

            mock_pipeline.process_table.assert_called_once_with(fc)
            mock_pipeline.process_table_group.assert_not_called()
            assert len(queries) == 1

    def test_multi_stage_uses_process_table_group(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
        })
        ctrl = self._make_controller({"item": fc})

        with patch("arcflow.controller.ZonePipeline") as MockPipeline:
            mock_pipeline = MagicMock()
            mock_query = MagicMock()
            mock_query.name = "bronze_item_stream"
            mock_query.isActive = True
            mock_pipeline.process_table_group.return_value = mock_query
            MockPipeline.return_value = mock_pipeline

            queries = ctrl.run_zone_pipeline("bronze")

            mock_pipeline.process_table_group.assert_called_once()
            assert len(queries) == 1

    def test_processed_stages_skipped_on_second_call(self):
        fc = _flow(zones={
            "bronze": StageConfig(),
            "raw_archive": StageConfig(stage_input="item"),
            "silver": StageConfig(),
        })
        ctrl = self._make_controller({"item": fc})

        with patch("arcflow.controller.ZonePipeline") as MockPipeline:
            mock_pipeline = MagicMock()
            mock_query = MagicMock()
            mock_query.name = "bronze_item_stream"
            mock_query.isActive = True
            mock_pipeline.process_table_group.return_value = mock_query
            mock_pipeline.process_table.return_value = mock_query
            MockPipeline.return_value = mock_pipeline

            # First call processes bronze group (bronze + raw_archive)
            ctrl.run_zone_pipeline("bronze")
            # raw_archive should be marked as processed
            assert ("item", "raw_archive") in ctrl._processed_stages

    def test_stop_all_clears_processed_stages(self):
        fc = _flow(zones={"bronze": StageConfig()})
        ctrl = self._make_controller({"item": fc})
        ctrl._processed_stages.add(("item", "bronze"))
        ctrl.stop_all()
        assert len(ctrl._processed_stages) == 0

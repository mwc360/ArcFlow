"""Tests for the YAML configuration loader."""

import textwrap
from pathlib import Path

import pytest
import yaml
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from arcflow.models import DimensionConfig, FlowConfig, StageConfig
from arcflow.yaml_loader import (
    _parse_schema,
    _parse_stage_config,
    load_dimensions,
    load_tables,
    load_yaml_config,
)


# ---------------------------------------------------------------------------
# Schema parsing
# ---------------------------------------------------------------------------

class TestParseSchema:
    def test_ddl_string(self):
        schema = _parse_schema("ItemId STRING, Cost DOUBLE, Qty LONG")
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 3
        assert schema["ItemId"].dataType == StringType()
        assert schema["Cost"].dataType == DoubleType()
        assert schema["Qty"].dataType == LongType()

    def test_ddl_nested_struct(self):
        ddl = "_meta STRUCT<producer: STRING>, data ARRAY<STRUCT<ItemId: STRING, Cost: DOUBLE>>"
        schema = _parse_schema(ddl)
        assert isinstance(schema["_meta"].dataType, StructType)
        assert isinstance(schema["data"].dataType, ArrayType)

    def test_json_dict(self):
        expected = StructType([
            StructField("id", StringType(), True),
            StructField("value", DoubleType(), True),
        ])
        schema = _parse_schema(expected.jsonValue())
        assert schema == expected

    def test_field_list_shorthand(self):
        fields = [
            {"name": "id", "type": "string", "nullable": True, "metadata": {}},
            {"name": "value", "type": "double", "nullable": True, "metadata": {}},
        ]
        schema = _parse_schema(fields)
        assert len(schema.fields) == 2
        assert schema["id"].dataType == StringType()

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError, match="DDL string"):
            _parse_schema(42)


# ---------------------------------------------------------------------------
# StageConfig parsing
# ---------------------------------------------------------------------------

class TestParseStageConfig:
    def test_defaults(self):
        sc = _parse_stage_config({})
        assert sc.mode == "append"
        assert sc.enabled is True
        assert sc.merge_keys is None

    def test_full_stage(self):
        sc = _parse_stage_config({
            "mode": "upsert",
            "merge_keys": ["order_id"],
            "partition_by": ["region"],
            "custom_transform": "silver_item",
            "enabled": False,
            "stage_input": "bronze",
            "table_name": "order_items",
            "schema_name": "archive",
        })
        assert sc.mode == "upsert"
        assert sc.merge_keys == ["order_id"]
        assert sc.partition_by == ["region"]
        assert sc.custom_transform == "silver_item"
        assert sc.enabled is False
        assert sc.stage_input == "bronze"
        assert sc.table_name == "order_items"
        assert sc.schema_name == "archive"

    def test_unknown_keys_ignored(self):
        sc = _parse_stage_config({"mode": "append", "bogus_key": 999})
        assert sc.mode == "append"


# ---------------------------------------------------------------------------
# FlowConfig (load_tables) parsing
# ---------------------------------------------------------------------------

class TestLoadTables:
    def test_minimal_table(self):
        raw = {
            "item": {
                "schema": "ItemId STRING, Cost DOUBLE",
            }
        }
        tables = load_tables(raw)
        assert "item" in tables
        fc = tables["item"]
        assert isinstance(fc, FlowConfig)
        assert fc.name == "item"
        assert fc.format == "parquet"
        assert len(fc.schema.fields) == 2

    def test_full_table(self):
        raw = {
            "shipment": {
                "name": "shipment",
                "format": "eventhub",
                "source_uri": "Endpoint=sb://test.servicebus.windows.net/;EntityPath=topic",
                "schema": "ShipmentId STRING, Weight DOUBLE",
                "description": "Shipment header",
                "trigger_mode": "processingTime",
                "trigger_interval": "10 seconds",
                "clean_source": True,
                "tags": ["logistics", "streaming"],
                "owner": "data-team",
                "reader_options": {"maxOffsetsPerTrigger": 1000},
                "zones": {
                    "bronze": {
                        "mode": "append",
                        "custom_transform": "explode_message_payload",
                    },
                    "silver": {
                        "mode": "upsert",
                        "merge_keys": ["shipment_id"],
                        "custom_transform": "silver_shipment",
                    },
                },
            }
        }
        tables = load_tables(raw)
        fc = tables["shipment"]
        assert fc.format == "eventhub"
        assert fc.trigger_mode == "processingTime"
        assert fc.trigger_interval == "10 seconds"
        assert fc.clean_source is True
        assert fc.tags == ["logistics", "streaming"]
        assert fc.reader_options == {"maxOffsetsPerTrigger": 1000}
        assert "bronze" in fc.zones
        assert fc.zones["silver"].mode == "upsert"
        assert fc.zones["silver"].merge_keys == ["shipment_id"]

    def test_name_defaults_to_key(self):
        tables = load_tables({"my_table": {"schema": "id STRING"}})
        assert tables["my_table"].name == "my_table"

    def test_name_override(self):
        tables = load_tables({"key": {"name": "override", "schema": "id STRING"}})
        assert tables["key"].name == "override"

    def test_multiple_tables(self):
        raw = {
            "t1": {"schema": "a STRING"},
            "t2": {"schema": "b LONG"},
        }
        tables = load_tables(raw)
        assert len(tables) == 2

    def test_config_defaults_apply_trigger_mode(self):
        raw = {"t1": {"schema": "id STRING"}}
        defaults = {"trigger_mode": "processingTime", "trigger_interval": "5 seconds"}
        tables = load_tables(raw, defaults=defaults)
        assert tables["t1"].trigger_mode == "processingTime"
        assert tables["t1"].trigger_interval == "5 seconds"

    def test_table_level_overrides_config_defaults(self):
        raw = {"t1": {"schema": "id STRING", "trigger_mode": "continuous", "trigger_interval": "1 second"}}
        defaults = {"trigger_mode": "processingTime", "trigger_interval": "5 seconds"}
        tables = load_tables(raw, defaults=defaults)
        assert tables["t1"].trigger_mode == "continuous"
        assert tables["t1"].trigger_interval == "1 second"

    def test_no_defaults_uses_dataclass_defaults(self):
        raw = {"t1": {"schema": "id STRING"}}
        tables = load_tables(raw)
        assert tables["t1"].trigger_mode == "availableNow"
        assert tables["t1"].trigger_interval is None


# ---------------------------------------------------------------------------
# DimensionConfig (load_dimensions) parsing
# ---------------------------------------------------------------------------

class TestLoadDimensions:
    def test_full_dimension(self):
        raw = {
            "dim_shipment": {
                "dimension_type": "dimension",
                "source_tables": ["shipment", "facility"],
                "source_zone": "silver",
                "target_zone": "gold",
                "transform": "build_dim_shipment",
                "zone_config": {
                    "mode": "upsert",
                    "merge_keys": ["shipment_id"],
                },
                "watermark_column": "event_time",
                "watermark_delay": "10 minutes",
                "scd_type": "type2",
                "scd_key_columns": ["shipment_id"],
                "description": "Enriched shipment dimension",
                "tags": ["gold"],
                "owner": "analytics",
            }
        }
        dims = load_dimensions(raw)
        dc = dims["dim_shipment"]
        assert isinstance(dc, DimensionConfig)
        assert dc.name == "dim_shipment"
        assert dc.dimension_type == "dimension"
        assert dc.source_tables == ["shipment", "facility"]
        assert dc.zone_config.mode == "upsert"
        assert dc.zone_config.merge_keys == ["shipment_id"]
        assert dc.watermark_delay == "10 minutes"
        assert dc.scd_type == "type2"

    def test_name_defaults_to_key(self):
        raw = {
            "fact_daily": {
                "dimension_type": "fact",
                "source_tables": ["orders"],
                "source_zone": "silver",
                "target_zone": "gold",
                "transform": "build_fact",
                "zone_config": {},
            }
        }
        dims = load_dimensions(raw)
        assert dims["fact_daily"].name == "fact_daily"


# ---------------------------------------------------------------------------
# Full YAML file loading
# ---------------------------------------------------------------------------

class TestLoadYamlConfig:
    def test_full_file(self, tmp_path: Path):
        content = textwrap.dedent("""\
            config:
              streaming_enabled: true
              checkpoint_uri: "Files/checkpoints"
              landing_uri: "Files/landing"

            tables:
              item:
                format: parquet
                source_uri: "Files/landing/item"
                schema: "ItemId STRING, Cost DOUBLE"
                zones:
                  bronze:
                    mode: append
                  silver:
                    mode: upsert
                    merge_keys: [item_id]
        """)
        yml_file = tmp_path / "pipeline.yml"
        yml_file.write_text(content, encoding="utf-8")

        tables, config = load_yaml_config(yml_file)

        assert config["streaming_enabled"] is True
        assert config["checkpoint_uri"] == "Files/checkpoints"

        assert "item" in tables
        assert tables["item"].format == "parquet"
        assert tables["item"].zones["silver"].merge_keys == ["item_id"]

    def test_tables_only(self, tmp_path: Path):
        content = textwrap.dedent("""\
            tables:
              t1:
                schema: "id STRING"
        """)
        yml_file = tmp_path / "tables.yml"
        yml_file.write_text(content, encoding="utf-8")

        tables, config = load_yaml_config(yml_file)
        assert len(tables) == 1
        assert config == {}

    def test_empty_file_gives_defaults(self, tmp_path: Path):
        yml_file = tmp_path / "empty.yml"
        yml_file.write_text("{}", encoding="utf-8")

        tables, config = load_yaml_config(yml_file)
        assert tables == {}
        assert config == {}

    def test_invalid_top_level_raises(self, tmp_path: Path):
        yml_file = tmp_path / "bad.yml"
        yml_file.write_text("- list_item", encoding="utf-8")

        with pytest.raises(ValueError, match="YAML mapping"):
            load_yaml_config(yml_file)

    def test_config_trigger_mode_propagates_to_tables(self, tmp_path: Path):
        content = textwrap.dedent("""\
            config:
              trigger_mode: processingTime
              trigger_interval: "0 seconds"

            tables:
              t1:
                schema: "id STRING"
              t2:
                schema: "id STRING"
                trigger_mode: continuous
        """)
        yml_file = tmp_path / "trigger.yml"
        yml_file.write_text(content, encoding="utf-8")

        tables, _ = load_yaml_config(yml_file)
        assert tables["t1"].trigger_mode == "processingTime"
        assert tables["t1"].trigger_interval == "0 seconds"
        assert tables["t2"].trigger_mode == "continuous"

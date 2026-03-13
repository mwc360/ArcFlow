"""
YAML configuration loader for ArcFlow

Provides an alternative to Python dataclass instantiation by allowing
pipeline configuration to be defined in YAML files.

Schema fields accept either:
  - DDL string: "ItemId STRING, SKU STRING, Cost DOUBLE"
  - JSON object: the StructType JSON representation (from StructType.jsonValue())

Usage:
    from arcflow.yaml_loader import load_yaml_config

    tables, dimensions, config = load_yaml_config("pipeline.yml")
    controller = Controller(spark, config, tables, dimensions)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimestampNTZType,
)

from arcflow.models import DimensionConfig, FlowConfig, StageConfig


# ---------------------------------------------------------------------------
# Pure-Python DDL parser (no SparkContext required)
# ---------------------------------------------------------------------------

_SIMPLE_TYPES = {
    "STRING": StringType,
    "INT": IntegerType,
    "INTEGER": IntegerType,
    "BIGINT": LongType,
    "LONG": LongType,
    "SMALLINT": ShortType,
    "SHORT": ShortType,
    "TINYINT": ByteType,
    "BYTE": ByteType,
    "FLOAT": FloatType,
    "DOUBLE": DoubleType,
    "BOOLEAN": BooleanType,
    "TIMESTAMP": TimestampType,
    "TIMESTAMP_NTZ": TimestampNTZType,
    "DATE": DateType,
    "BINARY": BinaryType,
}


class _DDLTokenizer:
    """Minimal tokenizer for PySpark DDL strings."""

    def __init__(self, text: str):
        self._text = text
        self._pos = 0

    def _skip_ws(self):
        while self._pos < len(self._text) and self._text[self._pos] in " \t\n\r":
            self._pos += 1

    def peek(self) -> str | None:
        self._skip_ws()
        if self._pos >= len(self._text):
            return None
        return self._text[self._pos]

    def consume(self, expected: str):
        self._skip_ws()
        if self._text[self._pos] != expected:
            raise ValueError(
                f"Expected '{expected}' at position {self._pos}, "
                f"got '{self._text[self._pos]}'"
            )
        self._pos += 1

    def read_word(self) -> str:
        self._skip_ws()
        start = self._pos
        while self._pos < len(self._text) and self._text[self._pos] not in " \t\n\r,<>:()":
            self._pos += 1
        word = self._text[start:self._pos]
        if not word:
            raise ValueError(f"Expected a token at position {self._pos}")
        return word

    @property
    def done(self) -> bool:
        self._skip_ws()
        return self._pos >= len(self._text)


def _parse_type(tok: _DDLTokenizer):
    """Parse a single data type from the token stream."""
    type_name = tok.read_word().upper()

    if type_name in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[type_name]()

    if type_name.startswith("DECIMAL"):
        # DECIMAL or DECIMAL(p) or DECIMAL(p, s)
        if tok.peek() == "(":
            tok.consume("(")
            precision = int(tok.read_word())
            scale = 0
            if tok.peek() == ",":
                tok.consume(",")
                scale = int(tok.read_word())
            tok.consume(")")
            return DecimalType(precision, scale)
        return DecimalType()

    if type_name == "ARRAY":
        tok.consume("<")
        element_type = _parse_type(tok)
        tok.consume(">")
        return ArrayType(element_type, True)

    if type_name == "MAP":
        tok.consume("<")
        key_type = _parse_type(tok)
        tok.consume(",")
        value_type = _parse_type(tok)
        tok.consume(">")
        return MapType(key_type, value_type, True)

    if type_name == "STRUCT":
        tok.consume("<")
        fields = _parse_struct_fields(tok, delimiter=">")
        tok.consume(">")
        return StructType(fields)

    raise ValueError(f"Unknown DDL type: {type_name}")


def _parse_struct_fields(tok: _DDLTokenizer, delimiter: str = "") -> list[StructField]:
    """Parse a comma-separated list of ``name: TYPE`` fields."""
    fields: list[StructField] = []
    while True:
        if tok.done:
            break
        if delimiter and tok.peek() == delimiter:
            break
        name = tok.read_word()
        # Field separator is either ':' (inside STRUCT<>) or implicit (top-level)
        if tok.peek() == ":":
            tok.consume(":")
        field_type = _parse_type(tok)
        fields.append(StructField(name, field_type, True))
        if tok.peek() == ",":
            tok.consume(",")
    return fields


def _ddl_to_struct(ddl: str) -> StructType:
    """Parse a DDL string into a StructType without requiring SparkContext."""
    tok = _DDLTokenizer(ddl)
    fields = _parse_struct_fields(tok)
    return StructType(fields)


# ---------------------------------------------------------------------------
# Schema entry-point
# ---------------------------------------------------------------------------

def _parse_schema(raw: Union[str, dict, list]) -> StructType:
    """Convert a YAML schema value into a PySpark StructType.

    Accepts:
        str  — DDL string, e.g. "ItemId STRING, SKU STRING"
        dict — StructType JSON ({"type": "struct", "fields": [...]})
        list — Shorthand list of {"name", "type", "nullable"} dicts
    """
    if isinstance(raw, str):
        return _ddl_to_struct(raw)

    if isinstance(raw, dict):
        return StructType.fromJson(raw)

    if isinstance(raw, list):
        return StructType.fromJson({"type": "struct", "fields": raw})

    raise TypeError(
        f"schema must be a DDL string, JSON dict, or field list — got {type(raw).__name__}"
    )


def _parse_stage_config(data: dict) -> StageConfig:
    """Build a StageConfig from a plain dict."""
    known_fields = {
        "mode", "merge_keys", "partition_by", "custom_transform",
        "enabled", "stage_input", "table_name", "schema_name",
    }
    filtered = {k: v for k, v in data.items() if k in known_fields}
    return StageConfig(**filtered)


def _parse_flow_config(name: str, data: dict, defaults: Optional[Dict[str, Any]] = None) -> FlowConfig:
    """Build a FlowConfig from a plain dict (one table entry).

    Args:
        name: Table key from the YAML.
        data: Table definition dict.
        defaults: Optional global defaults from the ``config`` section.
                  Table-level values take precedence.
    """
    defaults = defaults or {}
    schema = _parse_schema(data["schema"])

    zones: Dict[str, StageConfig] = {}
    for zone_name, zone_data in data.get("zones", {}).items():
        zones[zone_name] = _parse_stage_config(zone_data)

    return FlowConfig(
        name=data.get("name", name),
        schema=schema,
        format=data.get("format", "parquet"),
        source_uri=data.get("source_uri"),
        zones=zones,
        trigger_mode=data.get("trigger_mode", defaults.get("trigger_mode", "availableNow")),
        trigger_interval=data.get("trigger_interval", defaults.get("trigger_interval")),
        clean_source=data.get("clean_source", False),
        explode_column=data.get("explode_column"),
        explode_alias=data.get("explode_alias"),
        reader_options=data.get("reader_options", {}),
        writer_options=data.get("writer_options", {}),
        description=data.get("description"),
        tags=data.get("tags", []),
        owner=data.get("owner"),
    )


def _parse_dimension_config(name: str, data: dict) -> DimensionConfig:
    """Build a DimensionConfig from a plain dict."""
    zone_config = _parse_stage_config(data.get("zone_config", {}))

    return DimensionConfig(
        name=data.get("name", name),
        dimension_type=data["dimension_type"],
        source_tables=data["source_tables"],
        source_zone=data["source_zone"],
        target_zone=data["target_zone"],
        zone_config=zone_config,
        transform=data["transform"],
        trigger_mode=data.get("trigger_mode", "availableNow"),
        trigger_interval=data.get("trigger_interval"),
        watermark_column=data.get("watermark_column"),
        watermark_delay=data.get("watermark_delay"),
        scd_type=data.get("scd_type"),
        scd_key_columns=data.get("scd_key_columns"),
        description=data.get("description"),
        tags=data.get("tags", []),
        owner=data.get("owner"),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_tables(raw: dict, defaults: Optional[Dict[str, Any]] = None) -> Dict[str, FlowConfig]:
    """Parse the ``tables`` section of a YAML document.

    Args:
        raw: dict where keys are table names and values are table definitions.
        defaults: Optional global defaults from the ``config`` section.

    Returns:
        Dict[str, FlowConfig] suitable for ``Controller(table_registry=...)``.
    """
    return {name: _parse_flow_config(name, body, defaults) for name, body in raw.items()}


def load_dimensions(raw: dict) -> Dict[str, DimensionConfig]:
    """Parse the ``dimensions`` section of a YAML document.

    Args:
        raw: dict where keys are dimension names and values are definitions.

    Returns:
        Dict[str, DimensionConfig] suitable for ``Controller(dimension_registry=...)``.
    """
    return {name: _parse_dimension_config(name, body) for name, body in raw.items()}


def load_yaml_config(
    path: Union[str, Path],
) -> Tuple[Dict[str, FlowConfig], Optional[Dict[str, DimensionConfig]], Dict[str, Any]]:
    """Load a full ArcFlow pipeline configuration from a YAML file.

    Expected top-level keys (all optional):
        ``config``      — global pipeline settings (passed to ``get_config``)
        ``tables``      — table registry (key = table name)
        ``dimensions``  — dimension registry (key = dimension name)

    Args:
        path: Path to the YAML file.

    Returns:
        Tuple of (table_registry, dimension_registry, config_dict).
        Missing sections default to empty dicts / None.
    """
    path = Path(path)
    with open(path, "r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)

    if not isinstance(doc, dict):
        raise ValueError(f"Expected a YAML mapping at top level, got {type(doc).__name__}")

    config_dict: Dict[str, Any] = doc.get("config", {})

    tables: Dict[str, FlowConfig] = {}
    if "tables" in doc:
        tables = load_tables(doc["tables"], defaults=config_dict)

    dimensions: Optional[Dict[str, DimensionConfig]] = None
    if "dimensions" in doc:
        dimensions = load_dimensions(doc["dimensions"])

    return tables, dimensions, config_dict

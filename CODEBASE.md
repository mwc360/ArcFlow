# ArcFlow Codebase Reference

> Quick-reference for Copilot and contributors. Keep this in sync when adding major features.

---

## What is ArcFlow?

ArcFlow is a **PySpark streaming ELT framework** designed for lakehouse architectures (e.g., Microsoft Fabric, Databricks). It ingests data from streaming sources (Kafka, Azure Event Hubs) or file sources (Parquet, JSON, CSV), processes it through configurable zones (`bronze → silver → gold`), and writes to Delta Lake tables.

---

## Project Layout

```
src/arcflow/
├── config.py               # Global defaults (paths, spark settings, retry config)
├── models.py               # Core dataclasses: FlowConfig, StageConfig, DimensionConfig
├── controller.py           # Orchestrator — entry point for running pipelines
│
├── core/
│   ├── spark_session.py    # SparkSession factory helpers
│   ├── spark_configurator.py  # Auto-applies best-practice Spark configs
│   └── stream_manager.py   # Tracks/awaits/stops StreamingQuery instances
│
├── pipelines/
│   ├── zone_pipeline.py    # Single-source zone processing (landing→bronze, etc.)
│   └── dimension_pipeline.py  # Multi-source dimensional/fact table building
│
├── readers/
│   ├── base_reader.py      # ABC for all readers (streaming/batch toggle)
│   ├── kafka_reader.py     # Kafka + EventHub-via-Kafka (SASL_SSL, port 9093)
│   ├── eventhub_reader.py  # Native Azure EventHubs connector (AMQP, port 443)
│   ├── parquet_reader.py   # Parquet file reader
│   ├── json_reader.py      # JSON file reader
│   └── reader_factory.py   # Instantiates the correct reader from FlowConfig.format
│
├── writers/
│   ├── base_writer.py      # ABC for all writers
│   ├── delta_writer.py     # Delta Lake writer (append, upsert/merge, streaming)
│   └── writer_factory.py   # Instantiates the correct writer
│
├── transformations/        # Custom transformation functions referenced by name
│
└── utils/
    ├── table_utils.py          # build/parse Delta table references
    └── endpoint_validator.py   # Pre-flight TCP + format check for Kafka/EventHub
```

---

## Core Models (`models.py`)

### `FlowConfig` — one source → one Delta table
| Field | Type | Notes |
|---|---|---|
| `name` | `str` | Table identifier |
| `schema` | `StructType` | PySpark schema for JSON deserialization |
| `format` | `Literal['parquet','json','csv','kafka','eventhub']` | Source format |
| `source_uri` | `Optional[str]` | File path or Event Hubs connection string |
| `zones` | `Dict[str, StageConfig]` | Per-zone behaviour config |
| `trigger_mode` | `'availableNow'`\|`'processingTime'`\|`'continuous'` | Spark trigger |
| `trigger_interval` | `Optional[str]` | e.g. `"60 seconds"` |
| `reader_options` | `Dict[str, Any]` | Extra Spark reader options |

### `StageConfig` — per-zone behaviour
| Field | Notes |
|---|---|
| `mode` | `'append'` or `'upsert'` |
| `merge_keys` | Required when `mode='upsert'` |
| `partition_by` | Optional partition columns |
| `custom_transform` | Name of function in `transformations/` |
| `enabled` | Toggle a zone on/off per table |

### `DimensionConfig` — multiple sources → one enriched table
Used for dimensions, facts, and bridge tables. Reads from a `source_zone`, applies a named `transform` function, writes to `target_zone`.

---

## Readers

All readers extend `BaseReader`. Toggle batch vs streaming with `is_streaming=True/False`.

### `KafkaReader` (`format='kafka'`)
- Parses an **Azure Event Hubs connection string** into Kafka SASL options automatically.
- `read(table_config, raw=False)` — `raw=True` returns `payload` (string) + all metadata; `raw=False` returns deserialized schema fields + all metadata.
- `read_raw(table_config, limit=5)` — convenience batch shortcut for `read(raw=True)`.
- Requires JAR: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`


**Connection string format:**
```
Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>;EntityPath=<topic>
```

### `EventHubReader` (`format='eventhub'`)
- Uses the **native Azure EventHubs-Spark connector** (not Kafka protocol).
- Encrypts the connection string via `EventHubsUtils.encrypt()` JVM call.
- `read(table_config, raw=False)` — same `raw` flag as KafkaReader.
- `read_raw(table_config, limit=5)` — convenience batch shortcut for `read(raw=True)`.
- Requires JAR: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`


---

## Writers

### `DeltaWriter`
- Supports `append` (streaming `.toTable()`) and `upsert` (Delta `MERGE` via `foreachBatch`).
- Checkpoint path auto-derived from `config['checkpoint_uri']` + zone + table name.
- Trigger modes map directly to Spark trigger types.

---

## Stream Development Workflow

Use `ZonePipeline` test methods to iterate on a new Kafka/EventHub stream without writing
to Delta or needing a checkpoint. Both methods use Spark's `memory` sink with
`trigger(availableNow=True)` — reads a limited batch of backlogged messages then stops automatically.
The in-memory view is returned as a DataFrame.

```python
pipeline = ZonePipeline(spark, zone='bronze', config=config)

# 1 — Discover: no schema, see raw string payload + all connector metadata
df = pipeline.test_input(table['shipment'], raw=True)
df.show(truncate=False)

# 2 — Validate schema: does from_json() with the configured schema parse the payload correctly?
df = pipeline.test_input(table['shipment'])
df.printSchema()
df.show()

# 3 — Test full transform: schema + custom_transform (e.g. explode_message_payload) + normalisation
df = pipeline.test_output(table['shipment'])
df.show()

# 4 — Ship it
controller.run_zone_pipeline('bronze')
```

**`test_input(table_config, limit=20, timeout_seconds=60, raw=False)`**
- Kafka/EventHub: validates endpoint, streams to `_arcflow_test_<name>` memory view
- `raw=True`: returns `body`/`value` as-is (no schema needed) + all metadata columns
- `raw=False`: applies `from_json(schema)` + all metadata columns
- `limit` is applied **at the broker** via `maxOffsetsPerTrigger` / `eventhubs.maxEventsPerTrigger` — only `limit` records are fetched, not the full backlog
- File formats: standard batch read + `.limit(limit)`, `raw` ignored

**`test_output(table_config, limit=20, timeout_seconds=60)`**
- Kafka/EventHub: same streaming→memory path, always applies full transform chain
- `limit` applied at the broker (same mechanism as `test_input`)
- Transform chain: `from_json(schema)` → `custom_transform` → `normalize_columns_to_snake_case` → `apply_processing_timestamp`
- File formats: batch read + full transform chain + `.limit(limit)`

**Transformer pattern for `_meta` + `data[]` payloads** (common EventHub/Kafka pattern):
```python
# Bronze custom_transform — explode the data array
@register_zone_transformer
def explode_message_payload(df):
    df_expanded = df.select("body.*")          # or "value.*" for Kafka
    return df_expanded.selectExpr("_meta", "explode(data) as data")

# Silver custom_transform — flatten to columns
@register_zone_transformer
def silver_shipment(df):
    return df.selectExpr("_meta.*", "data.*").drop("_meta", "data", "_processing_timestamp")
```

---

## Controller (`controller.py`)

Main orchestration entry point. Takes a `SparkSession`, global `config` dict, and registries.

```python
controller = Controller(spark, config, table_registry, dimension_registry)

controller.run_zone_pipeline('bronze')          # landing → bronze
controller.run_zone_pipeline('silver')          # bronze → silver
controller.run_full_pipeline()                  # bronze + silver + gold + dimensions
controller.await_completion()                   # block until availableNow streams finish
controller.stop_all()                           # graceful shutdown
controller.get_status()                         # dict of stream statuses
```

On init, the Controller automatically applies best-practice Spark configs via `SparkConfigurator` (see below). Disable with `autoset_spark_configs=False` in the config dict.

---

## Spark Best-Practice Configurator (`core/spark_configurator.py`)

Applied automatically by `Controller.__init__` when `autoset_spark_configs=True` (default).
Never overwrites configs already set on the session — safe to use alongside managed Spark environments (Fabric, Databricks).

**Configs applied:**

| Config | Value | Purpose |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` | Dynamic join/partition optimisation (AQE) |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Merge small post-shuffle partitions |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Auto-split skewed join partitions |
| `spark.databricks.delta.optimizeWrite.enabled` | `true` | Compact small files on Delta write |
| `spark.databricks.delta.autoCompact.enabled` | `true` | Background compaction |
| `spark.databricks.delta.schema.autoMerge.enabled` | `false` | Prevent silent schema drift |
| `spark.sql.shuffle.partitions` | `8` | Low count suited for streaming micro-batch |
| `spark.sql.parquet.compression.codec` | `snappy` | Balanced compression |
| `spark.sql.files.ignoreMissingFiles` | `true` | Resilience to transient file drops |
| `spark.sql.datetime.java8API.enabled` | `true` | Modern timestamp handling |
| RocksDB state store | (best-effort) | Efficient stateful streaming — silently skipped if unavailable |

**Disabling or overriding:**
```python
# Disable entirely
config = {'autoset_spark_configs': False}

# Override specific keys (these ARE written, even if already set)
# Set to None to unset a config key on the session
config = {
    'spark_config_overrides': {
        'spark.sql.shuffle.partitions': '200',   # override to higher value
        'spark.databricks.delta.autoCompact.enabled': None,  # unset (remove) this key
    }
}
```

**Using directly (without Controller):**
```python
from arcflow.core.spark_configurator import SparkConfigurator

result = SparkConfigurator.apply(spark)
# result = {'applied': [...], 'skipped': [...], 'failed': [...]}
```

---

## Endpoint Validation (`utils/endpoint_validator.py`)

Pre-flight check for Kafka and EventHub endpoints — **no Spark required**.

```python
from arcflow.utils.endpoint_validator import validate_endpoint, StreamEndpointValidator

# From a FlowConfig (dispatches automatically by format)
result = validate_endpoint(table_config)
print(result)                     # "[OK] kafka endpoint reachable (...)"
if not result.valid:
    raise RuntimeError(result.error)

# From a raw connection string
result = StreamEndpointValidator.validate_kafka("Endpoint=sb://...")
result = StreamEndpointValidator.validate_eventhub("Endpoint=sb://...")
```

`ValidationResult` fields: `valid`, `format`, `endpoint`, `topic`, `reachable`, `latency_ms`, `error`.

Two-phase check:
1. **Format validation** — parses the connection string, raises descriptive errors on missing fields.
2. **TCP probe** — opens a socket to `<namespace>.servicebus.windows.net:9093` (Kafka) or `:443` (EventHub) and measures latency. Fails fast on DNS or firewall errors.

---

## Configuration (`config.py`)

`Defaults` class holds all default values. Override via the `config` dict passed to `Controller`.

```python
from arcflow.config import get_config

config = get_config({
    'landing_uri': 'abfss://...',
    'checkpoint_uri': 'abfss://...',
    'streaming_enabled': True,
    'await_termination': False,   # False for notebooks, True for Spark Job Definitions
})
```

Key config keys: `landing_uri`, `archive_uri`, `checkpoint_uri`, `streaming_enabled`,
`trigger_interval`, `await_termination`, `optimize_write`, `auto_compact`.

---

## Required JARs

| Connector | Format | JAR coordinate |
|---|---|---|
| Kafka / EventHub via Kafka | `'kafka'` | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` |
| Native EventHub | `'eventhub'` | `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22` |

Pass via `spark_configs`:
```python
{'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'}
```

---

## Testing

```bash
pytest                  # run all tests
pytest tests/ -v        # verbose
```

Tests live in `tests/`. Reference files for test assertions are in the root `test_ref_files.py`.

---

## Key Conventions

- **Zone-agnostic** — zone names (`bronze`, `silver`, `gold`) are arbitrary strings; the framework doesn't hardcode them.
- **`availableNow` trigger** — default trigger mode; processes all available data then stops (good for scheduled jobs and notebooks).
- **`source_uri`** for streaming sources is always an **Event Hubs connection string** (full format with `EntityPath`).
- **`reader_options`** / **`writer_options`** pass arbitrary Spark options through without code changes.
- **`custom_transform`** in `StageConfig` is a string name looked up in `transformations/` — keeps configs serializable.

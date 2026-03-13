# ArcFlow — Copilot Instructions

ArcFlow is a **PySpark streaming ELT framework** for lakehouse architectures (Microsoft Fabric, Databricks). It ingests from streaming sources (Kafka, Azure Event Hubs) or file sources (Parquet, JSON, CSV), processes through configurable zones (`bronze → silver → gold`), and writes to Delta Lake tables.

## Project Structure

```
src/arcflow/
├── config.py                  # Global defaults (paths, spark settings, retry config)
├── models.py                  # Core dataclasses: FlowConfig, StageConfig, DimensionConfig
├── yaml_loader.py             # YAML config loader (alternative to Python dataclasses)
├── controller.py              # Orchestrator — entry point for running pipelines
├── core/
│   ├── spark_session.py       # SparkSession factory helpers
│   ├── spark_configurator.py  # Auto-applies best-practice Spark configs
│   ├── stage_chain_listener.py # Event-driven downstream stage triggering via StreamingQueryListener
│   └── stream_manager.py      # Tracks/awaits/stops StreamingQuery instances
├── pipelines/
│   ├── zone_pipeline.py       # Single-source zone processing (landing→bronze, etc.)
│   └── dimension_pipeline.py  # Multi-source dimensional/fact table building
├── readers/
│   ├── base_reader.py         # ABC for all readers (streaming/batch toggle)
│   ├── kafka_reader.py        # Kafka + EventHub-via-Kafka (SASL_SSL, port 9093)
│   ├── eventhub_reader.py     # Native Azure EventHubs connector (AMQP, port 443)
│   ├── parquet_reader.py      # Parquet file reader
│   ├── json_reader.py         # JSON file reader
│   └── reader_factory.py      # Instantiates the correct reader from FlowConfig.format
├── writers/
│   ├── base_writer.py         # ABC for all writers
│   ├── delta_writer.py        # Delta Lake writer (append, upsert/merge, streaming)
│   └── writer_factory.py      # Instantiates the correct writer
├── transformations/           # Custom transform functions referenced by name
└── utils/
    ├── table_utils.py         # Build/parse Delta table references
    └── endpoint_validator.py  # Pre-flight TCP + format check for Kafka/EventHub
```

## Tech Stack

- Python 3, PySpark, Delta Lake
- Azure Event Hubs (Kafka protocol or native AMQP connector)
- Target environments: Microsoft Fabric, Databricks

## Build & Test

```bash
pytest                  # run all tests
pytest tests/ -v        # verbose
```

Tests live in `tests/`. Reference files for test assertions are in `test_ref_files.py`.

## Core Models (`models.py`)

- **`FlowConfig`** — defines one source → one Delta table. Key fields: `name`, `schema` (StructType), `format` (`parquet|json|csv|kafka|eventhub`), `source_uri`, `zones` (dict of StageConfig), `trigger_mode`, `trigger_interval`, `reader_options`.
- **`StageConfig`** — per-zone behaviour: `mode` (`append|upsert`), `merge_keys` (required for upsert), `partition_by`, `custom_transform` (string name of function in `transformations/`), `enabled`, `stage_input` (declares input source for multi-target), `table_name` (override target table), `schema_name` (override target schema).
- **`DimensionConfig`** — multiple sources → one enriched table. Reads from `source_zone`, applies a named `transform`, writes to `target_zone`.

## Readers

All readers extend `BaseReader`. Toggle batch vs streaming with `is_streaming=True/False`.

- **`KafkaReader`** (`format='kafka'`): Parses Azure Event Hubs connection strings into Kafka SASL options. `read(table_config, raw=False)` — `raw=True` returns string payload + metadata; `raw=False` returns deserialized fields + metadata. Requires JAR `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`.
- **`EventHubReader`** (`format='eventhub'`): Uses native Azure EventHubs-Spark connector. Encrypts connection string via `EventHubsUtils.encrypt()`. Same `raw` flag API. Requires JAR `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`.
- Metadata columns are **not aliased** in the final output — they pass through with their original names.

Connection string format: `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>;EntityPath=<topic>`

## Writers

- **`DeltaWriter`**: Supports `append` (streaming `.toTable()`) and `upsert` (Delta `MERGE` via `foreachBatch`). Checkpoint path is auto-derived from `config['checkpoint_uri']` + zone + table name. Trigger modes map directly to Spark trigger types.

## Stream Development Workflow

Use `ZonePipeline` test methods to iterate without writing to Delta or needing a checkpoint. Both use Spark's `memory` sink with `trigger(availableNow=True)`.

```python
pipeline = ZonePipeline(spark, zone='bronze', config=config)

# 1 — Raw payload + all connector metadata (no schema needed)
df = pipeline.test_input(table['shipment'], raw=True)

# 2 — Validate schema parsing (from_json with configured schema)
df = pipeline.test_input(table['shipment'])

# 3 — Full transform chain: schema + custom_transform + normalisation
df = pipeline.test_output(table['shipment'])

# 4 — Ship it
controller.run_zone_pipeline('bronze')
```

- **`test_input(table_config, limit=20, timeout_seconds=60, raw=False)`** — For streaming: validates endpoint, streams to `_arcflow_test_<name>` memory view. `limit` is applied at the broker via `maxOffsetsPerTrigger` / `eventhubs.maxEventsPerTrigger`. For files: standard batch read + `.limit(limit)`.
- **`test_output(table_config, limit=20, timeout_seconds=60)`** — Same streaming→memory path, always applies full transform chain: `from_json(schema)` → `custom_transform` → `normalize_columns_to_snake_case` → `apply_processing_timestamp`.

### Transformer Pattern for `_meta` + `data[]` Payloads

```python
@register_zone_transformer
def explode_message_payload(df):
    df_expanded = df.select("body.*")          # or "value.*" for Kafka
    return df_expanded.selectExpr("_meta", "explode(data) as data")

@register_zone_transformer
def silver_shipment(df):
    return df.selectExpr("_meta.*", "data.*").drop("_meta", "data", "_processing_timestamp")
```

## Controller (`controller.py`)

```python
controller = Controller(spark, config, table_registry, dimension_registry)

controller.run_zone_pipeline('bronze')     # landing → bronze
controller.run_zone_pipeline('silver')     # bronze → silver
controller.run_full_pipeline()             # bronze + silver + gold + dimensions
controller.await_completion()              # block until availableNow streams finish
controller.stop_all()                      # graceful shutdown
controller.get_status()                    # dict of stream statuses
```

Controller auto-applies best-practice Spark configs via `SparkConfigurator` on init. Disable with `autoset_spark_configs=False`.

### Event-Driven Stage Chaining

By default (`event_driven_chaining=True`), `run_full_pipeline` uses a `StageChainListener` (Spark `StreamingQueryListener`) to cascade downstream zones. The first zone runs with its configured trigger; downstream zones are automatically spawned as `availableNow` when upstream produces data.

- **processingTime bronze:** Each micro-batch that produces rows triggers downstream.
- **availableNow bronze:** Downstream triggers after all bronze queries terminate.
- **Dedup:** If a downstream zone is already running, a pending re-trigger flag is set.
- **Recovery:** On startup, all downstream zones are spawned once as `availableNow` to catch up on pending data from prior failed runs.
- **Dimensions** are excluded from chaining — they run after all zones (or manually).

```python
# Event-driven (default) — downstream zones auto-triggered
config = get_config({'event_driven_chaining': True})    # default

# Legacy sequential — all zones started upfront
config = get_config({'event_driven_chaining': False})
```

### Multi-Target Output (`stage_input`)

Write one source read to multiple Delta tables in a single streaming query via `foreachBatch`. Use `stage_input` on `StageConfig` to declare where a stage reads from:

| `stage_input` value | Meaning |
|---|---|
| `None` (default) | Sequential chain — first zone reads from root, subsequent from previous primary |
| FlowConfig.name (e.g. `'item'`) | Read from root source (Kafka/EventHub/files) |
| Stage name (e.g. `'bronze'`) | Read from that stage's Delta output |

Stages resolving to the same input are **auto-grouped** into one `foreachBatch` query. Each stage applies its own `custom_transform` independently per micro-batch.

**Main chain rule:** Stages with `stage_input` set are **branches** — they're skipped when determining the sequential chain for stages with `stage_input=None`.

```python
# Root fan-out: archive raw + transform bronze
FlowConfig(name='item', ..., zones={
    "bronze": StageConfig(mode='append', custom_transform='explode_data'),
    "raw_archive": StageConfig(mode='append', stage_input='item', schema_name='archive'),
    "silver": StageConfig(mode='append', custom_transform='silver_transform'),
})
# bronze + raw_archive grouped (both read from root) → one foreachBatch
# silver reads from bronze output (main chain)

# Mid-pipeline fan-out: two silver tables from bronze
FlowConfig(name='item', ..., zones={
    "bronze": StageConfig(mode='append', custom_transform='explode_data'),
    "silver_orders": StageConfig(mode='upsert', merge_keys=['order_id'], custom_transform='extract_orders'),
    "silver_items": StageConfig(mode='append', stage_input='bronze', table_name='order_items',
                                custom_transform='extract_order_items'),
})
# silver_orders + silver_items grouped (both read from bronze) → one foreachBatch
```

Additional `StageConfig` fields for multi-target:
- `table_name: Optional[str]` — override target table name (defaults to `FlowConfig.name`)
- `schema_name: Optional[str]` — override target schema (defaults to stage/zone name)

## Spark Configurator (`core/spark_configurator.py`)

Applied automatically by Controller. Never overwrites configs already set on the session.

Key configs applied: AQE enabled, coalesce partitions, skew join handling, Delta optimizeWrite + autoCompact, schema autoMerge disabled, shuffle partitions = 8, snappy compression, ignoreMissingFiles, java8 datetime API, RocksDB state store (best-effort).

Override specific keys or disable entirely:

```python
config = {'autoset_spark_configs': False}              # disable entirely
config = {'spark_config_overrides': {                  # override specific keys
    'spark.sql.shuffle.partitions': '200',
    'spark.databricks.delta.autoCompact.enabled': None, # unset this key
}}
```

## Endpoint Validation (`utils/endpoint_validator.py`)

Pre-flight check for Kafka/EventHub endpoints — no Spark required.

```python
from arcflow.utils.endpoint_validator import validate_endpoint
result = validate_endpoint(table_config)  # dispatches by format automatically
# result fields: valid, format, endpoint, topic, reachable, latency_ms, error
```

Two-phase: format validation (parse connection string) → TCP probe (DNS + port check with latency measurement).

## Configuration (`config.py`)

```python
from arcflow.config import get_config
config = get_config({
    'landing_uri': 'abfss://...',
    'checkpoint_uri': 'abfss://...',
    'streaming_enabled': True,
    'await_termination': False,   # False for notebooks, True for Spark Job Definitions
})
```

Key config keys: `landing_uri`, `archive_uri`, `checkpoint_uri`, `streaming_enabled`, `trigger_interval`, `await_termination`, `event_driven_chaining`, `optimize_write`, `auto_compact`.

## YAML Configuration (`yaml_loader.py`)

Pipeline configuration can be defined in YAML as an alternative to Python dataclasses. All three top-level keys are optional.

```yaml
# pipeline.yml
config:
  streaming_enabled: true
  checkpoint_uri: "Files/checkpoints"

tables:
  item:
    format: parquet
    source_uri: "Files/landing/item"
    schema: "ItemId STRING, SKU STRING, Cost DOUBLE"
    zones:
      bronze:
        mode: append
      silver:
        mode: upsert
        merge_keys: [item_id]
        custom_transform: silver_item

dimensions:
  dim_shipment:
    dimension_type: dimension
    source_tables: [shipment, facility]
    source_zone: silver
    target_zone: gold
    transform: build_dim_shipment
    zone_config:
      mode: upsert
      merge_keys: [shipment_id]
```

Load with:

```python
from arcflow import load_yaml_config
tables, dimensions, config = load_yaml_config("pipeline.yml")
controller = Controller(spark, config, tables, dimensions)
```

- **`load_yaml_config(path)`** — returns `(Dict[str, FlowConfig], Optional[Dict[str, DimensionConfig]], Dict[str, Any])`
- **`load_tables(raw_dict)`** / **`load_dimensions(raw_dict)`** — parse subsections independently
- **Schema formats**: DDL string (`"Name STRING, Cost DOUBLE"`), StructType JSON dict, or field list. DDL parsing is pure Python (no SparkContext required).
- Nested DDL types supported: `STRUCT<...>`, `ARRAY<...>`, `MAP<...>`, `DECIMAL(p,s)`

See `examples/pipeline_config.yml` for a full example.

## Coding Conventions

- **Zone-agnostic** — zone names (`bronze`, `silver`, `gold`) are arbitrary strings; the framework doesn't hardcode them.
- **`availableNow` trigger** — default trigger mode; processes all available data then stops.
- **`source_uri`** for streaming sources is always an Event Hubs connection string (full format with `EntityPath`).
- **`reader_options`** / **`writer_options`** pass arbitrary Spark options through without code changes.
- **`custom_transform`** in `StageConfig` is a string name looked up in `transformations/` — keeps configs serializable.
- **Metadata columns are not aliased** in readers — they pass through with their original connector names.

## Required JARs

| Connector | Format | JAR |
|---|---|---|
| Kafka / EventHub via Kafka | `kafka` | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` |
| Native EventHub | `eventhub` | `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22` |

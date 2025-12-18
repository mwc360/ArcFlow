# ⚡️ArcFlow

**A streaming-first PySpark ELT framework for Microsoft Fabric**

ArcFlow, named after an electrical arc (a continiuous discharge of electricity across two conductors), is a streaming-first PySpark data engineering framework designed for scalable and testable lakehouse architectures. It provides a flexible, zone-agnostic approach to building ELT pipelines with support for both single-source table processing and multi-source dimensional modeling.

ArcFlow provides a means of rapidly moving and transforming data between zones of a lakehouse.

## Features

- **Zone-Agnostic Architecture**: No hardcoded bronze/silver/gold - use any zone names you want
- **Single-Source Processing**: Process tables through zones with `FlowConfig`
- **Multi-Source Modeling**: Build dimensional tables from multiple sources with `DimensionConfig`
- **Stream & Batch Toggle**: Easy development-to-production switch
- **Factory Patterns**: Extensible readers and writers for any format
- **Custom Transformations**: Table-specific, zone-specific transformations via registry

## Architecture

### Zone-Based Processing Flow

```
Landing (raw files)
    ↓
Bronze (raw ingestion)
    ↓
Silver (curation, deduplication, semi-structured data parsing)
    ↓
Gold (aggregations, business logic, dimensional modeling)
```

### Core Components

- **Models**: `FlowConfig`, `DimensionConfig`, `StageConfig` - Type-safe table definitions
- **Readers**: Factory pattern for Parquet, JSON, CSV, etc.
- **Writers**: Delta Lake append and upsert operations
- **Transformations**: Universal + custom per-table-per-zone
- **Pipelines**: `ZonePipeline` (single-source), `DimensionPipeline` (multi-source)
- **Controller**: `Controller` - Coordinates all pipelines

## Installation

```bash
# Clone repository
git clone <your-repo>
cd ArcFlow

# Install dependencies with UV
uv sync

# Or with pip
pip install -e .
```

## Quick Start

### 1. Define Your Tables

Create a table registry with `FlowConfig`:

```python
from arcflow import FlowConfig, StageConfig

tables = {}

# Define a table with zone-specific processing
tables['sensor_data'] = FlowConfig(
    name='sensor_data',
    format='parquet',
    landing_path='Files/landing/opc_ua/sensor_data/',
    zones={
        'bronze': StageConfig(
            enabled=True,
            mode='append',
            description='Raw sensor readings'
        ),
        'silver': StageConfig(
            enabled=True,
            mode='upsert',
            merge_keys=['sensor_id', 'timestamp'],
            custom_transform='clean_sensor_data',
            description='Deduplicated and validated'
        ),
        'gold': StageConfig(
            enabled=True,
            mode='append',
            custom_transform='aggregate_hourly',
            description='Hourly aggregations'
        )
    }
)
```

### 2. Define Custom DataFrame Transformers (Optional)

Register table-specific, zone-specific transformations:

```python
from arcflow.transformations.zone_transforms import register_zone_transformer
from pyspark.sql import DataFrame

@register_zone_transformer()
def clean_sensor_data(df: DataFrame) -> DataFrame:
    """Custom cleaning logic for sensor data in silver zone"""
    return df.filter(df.sensor_value.isNotNull()) \
             .filter(df.sensor_value >= 0)
```

### 3. Run the Pipeline

```python
from arcflow import Controller
from arcflow.core.spark_session import create_spark_session

# Create Spark session
spark = create_spark_session(app_name="MyELT")

# Configure pipeline
config = {
    'streaming_enabled': True,
    'checkpoint_uri': 'Files/checkpoints/',
    'landing_uri': 'Files/landing/',
}

# Initialize controller
controller = Controller(
    spark=spark,
    config=config,
    table_registry=tables
)

# Run full pipeline
orchestrator.run_full_pipeline(zones=['bronze', 'silver', 'gold'])
```

## Multi-Source Dimensional Modeling

Build fact tables, dimensions, or bridge tables from multiple sources:

```python
from arcflow import DimensionConfig

# Define a fact table combining multiple sources
equipment_fact = DimensionConfig(
    name='fact_equipment_performance',
    dimension_type='fact',
    source_tables=[
        {'table': 'sensor_data', 'zone': 'silver', 'alias': 'sensors'},
        {'table': 'maintenance_logs', 'zone': 'silver', 'alias': 'maintenance'},
        {'table': 'production_schedule', 'zone': 'silver', 'alias': 'schedule'}
    ],
    builder_transform='build_equipment_fact',
    mode='upsert',
    merge_keys=['equipment_id', 'timestamp'],
    enabled=True
)
```

Register the builder function:

```python
from arcflow.transformations.dimension_transforms import register_dimension_transformer

@register_dimension_transformer
def build_equipment_fact(source_tables: dict, dimension_config) -> DataFrame:
    """Join sensors, maintenance, and schedule data"""
    sensors = source_tables['sensors']
    maintenance = source_tables['maintenance']
    schedule = source_tables['schedule']
    
    # Your custom join logic
    return sensors.join(maintenance, ['equipment_id'], 'left') \
                  .join(schedule, ['equipment_id'], 'left')
```

## Development vs Production

### Development Mode (Batch)

Fast iteration with batch processing:

```python
config = {
    'streaming_enabled': False  # Batch mode
}
```

### Production Mode (Streaming)

Continuous processing:

```python
config = {
    'streaming_enabled': True,  # Streaming mode
    'checkpoint_uri': 'Files/checkpoints/',
}
```

## Deploying to Microsoft Fabric

### 1. Build Package

```bash
uv build
```

This creates a wheel file in `dist/`:

```
dist/arcflow-0.1.0-py3-none-any.whl
```

### 2. Upload to Fabric Workspace

1. Open your Fabric workspace
2. Go to **Settings** → **Workspace settings** → **Data Engineering/Science**
3. Upload the wheel file

### 3. Create Spark Job Definition

1. Create new **Spark Job Definition**
2. Set **Main file**: `src/arcflow/main.py`
3. Add attached libraries: `arcflow-0.1.0-py3-none-any.whl`
4. Configure Spark properties if needed
5. Run the job

### 4. Customize Configuration

Edit `src/arcflow/main.py` to:
- Define your table registry
- Configure zones
- Register custom transformations
- Set pipeline behavior

## Project Structure

```
src/arcflow/
├── __init__.py                      # Package exports
├── models.py                        # FlowConfig, DimensionConfig, StageConfig
├── orchestrator.py                  # ArcFlowOrchestrator
├── main.py                          # Entry point for Spark Job Definition
│
├── core/
│   ├── spark_session.py            # Spark session factory
│   └── stream_manager.py           # Streaming query lifecycle
│
├── readers/
│   ├── base_reader.py              # BaseReader abstract class
│   ├── parquet_reader.py           # Parquet implementation
│   ├── json_reader.py              # JSON implementation
│   └── reader_factory.py           # ReaderFactory
│
├── writers/
│   ├── base_writer.py              # BaseWriter abstract class
│   ├── delta_writer.py             # Delta Lake implementation
│   └── writer_factory.py           # WriterFactory
│
├── transformations/
│   ├── common.py                   # Universal transformations
│   ├── zone_transforms.py          # Custom zone transformations registry
│   └── dimension_transforms.py     # Custom dimension builders registry
│
└── pipelines/
    ├── zone_pipeline.py            # Single-source zone processing
    └── dimension_pipeline.py       # Multi-source dimensional modeling
```

## Testing

Run tests with pytest:

```bash
# All tests
uv run pytest

# With coverage
uv run pytest --cov=src/arcflow --cov-report=html

# Specific test
uv run pytest tests/test_transformations.py
```

## Configuration Reference

### FlowConfig

```python
FlowConfig(
    name: str,              # Table name
    format: str,            # 'parquet', 'json', 'csv', etc.
    landing_uri: str,      # Path to raw files
    zones: dict,            # Dict of zone_name -> StageConfig
    json_explode_arrays: bool = False,
    json_archive_after_read: bool = False,
    csv_header: bool = True,
    csv_delimiter: str = ',',
    csv_infer_schema: bool = True,
)
```

### StageConfig

```python
StageConfig(
    enabled: bool,                      # Enable this zone
    mode: str,                          # 'append' or 'upsert'
    merge_keys: List[str] = None,       # Required for upsert
    custom_transform: str = None,       # Name of registered transformer
    description: str = '',
)
```

### DimensionConfig

```python
DimensionConfig(
    name: str,                          # Dimension table name
    dimension_type: str,                # 'fact', 'dimension', 'bridge'
    source_tables: List[dict],          # [{'table': 'x', 'zone': 'silver'}]
    builder_transform: str,             # Name of registered builder
    mode: str,                          # 'append' or 'upsert'
    merge_keys: List[str] = None,       # Required for upsert
    enabled: bool = True,
    description: str = '',
)
```

## Best Practices

### 1. Zone Strategy

- **Bronze**: Raw ingestion, append-only, minimal transformations
- **Silver**: Curated, deduplicated, upsert mode with merge keys
- **Gold**: Business aggregations, materialized views
- **Dimensions**: Multi-source modeling in gold or separate zone

### 2. Custom Transformations

- Keep transformations pure functions
- Make them testable with unit tests
- Use descriptive names: `clean_sensor_data`, `enrich_customer_data`
- Register with decorators for discoverability

### 3. Merge Keys

- Always specify merge keys for upsert mode
- Include timestamp or version columns for SCD Type 2
- Use surrogate keys for dimensions

### 4. Streaming vs Batch

- Use batch mode (`streaming_enabled=False`) for development
- Use streaming mode in production for continuous processing
- Test with batch before deploying streaming

### 5. Error Handling

- Monitor streaming query status with `orchestrator.get_status()`
- Implement graceful shutdown with `orchestrator.stop_all()`
- Use checkpointing for exactly-once processing

## Examples

See `src/arcflow/main.py` for complete examples including:
- OPC UA sensor data processing
- Equipment maintenance logs
- Production schedule ingestion
- Equipment performance fact table
- Sensor dimension with SCD Type 2

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - See LICENSE file for details

## Support

For issues, questions, or feature requests, please open a GitHub issue.

---

**Built with ❤️ for data engineers working with Microsoft Fabric**

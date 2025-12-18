# ArcFlow - Quick Start Guide

Get started with ArcFlow in 5 minutes!

## Prerequisites

- Python 3.11
- UV package manager (or pip)
- Microsoft Fabric workspace (for production deployment)

## Installation

```bash
# Clone repository
cd ArcFlow

# Install dependencies
uv sync

# Verify installation
uv run python -c "from arcflow import ArcFlowOrchestrator; print('✅ ArcFlow installed')"
```

## 5-Minute Example

### 1. Create a Simple Pipeline (example_simple.py)

```python
from arcflow import FlowConfig, StageConfig, ArcFlowOrchestrator
from arcflow.core.spark_session import create_spark_session

# Create Spark session
spark = create_spark_session(app_name="QuickStart")

# Define a simple table
table_registry = {
    'my_data': FlowConfig(
        name='my_data',
        format='parquet',
        landing_path='Files/landing/my_data/',
        zones={
            'bronze': StageConfig(
                enabled=True,
                mode='append',
                description='Raw data'
            ),
            'silver': StageConfig(
                enabled=True,
                mode='upsert',
                merge_keys=['id'],
                description='Cleaned data'
            )
        }
    )
}

# Configure pipeline
config = {
    'streaming_enabled': False  # Batch mode for testing
}

# Run pipeline
orchestrator = ArcFlowOrchestrator(
    spark=spark,
    config=config,
    table_registry=table_registry
)

orchestrator.run_full_pipeline(zones=['bronze', 'silver'])
print("✅ Pipeline complete!")
```

### 2. Run the Example

```bash
uv run python example_simple.py
```

## What Just Happened?

1. **Created** a Spark session with Delta Lake optimizations
2. **Defined** a table with 2 zones (bronze, silver)
3. **Configured** the pipeline for batch mode
4. **Ran** the orchestrator to process through both zones

## Next Steps

### Add Custom Transformations

```python
from arcflow.transformations.zone_transforms import register_zone_transformer

@register_zone_transformer
def clean_my_data(df):
    """Remove nulls and filter bad values"""
    return df.filter(df.value >= 0).dropna()
```

Reference in your config:

```python
'silver': StageConfig(
    enabled=True,
    mode='upsert',
    merge_keys=['id'],
    custom_transform='clean_my_data'  # Add this
)
```

### Switch to Streaming Mode

```python
config = {
    'streaming_enabled': True,  # Change to True
    'checkpoint_location': 'Files/checkpoints/',
}
```

### Add More Zones

```python
zones={
    'bronze': StageConfig(...),
    'silver': StageConfig(...),
    'gold': StageConfig(
        enabled=True,
        mode='append',
        custom_transform='aggregate_hourly'
    )
}
```

Then run:

```python
orchestrator.run_full_pipeline(zones=['bronze', 'silver', 'gold'])
```

### Create Multi-Source Tables

```python
from arcflow import DimensionConfig

dimension_registry = {
    'fact_combined': DimensionConfig(
        name='fact_combined',
        dimension_type='fact',
        source_tables=[
            {'table': 'table1', 'zone': 'silver'},
            {'table': 'table2', 'zone': 'silver'}
        ],
        builder_transform='combine_tables',
        mode='upsert',
        merge_keys=['id']
    )
}
```

## Common Patterns

### Pattern 1: Raw → Cleaned → Aggregated

```python
zones={
    'bronze': StageConfig(enabled=True, mode='append'),
    'silver': StageConfig(enabled=True, mode='upsert', merge_keys=['id']),
    'gold': StageConfig(enabled=True, mode='append', custom_transform='aggregate')
}
```

### Pattern 2: JSON with Array Explosion

```python
FlowConfig(
    format='json',
    json_explode_arrays=True,  # Explode nested arrays
    json_archive_after_read=True,  # Move to archive
    ...
)
```

### Pattern 3: Stream-Stream Join

```python
DimensionConfig(
    source_tables=[
        {'table': 'stream1', 'zone': 'silver'},
        {'table': 'stream2', 'zone': 'silver'}
    ],
    builder_transform='join_streams',  # Implement watermark logic
    ...
)
```

## Troubleshooting

### Issue: Import errors

```bash
# Verify installation
uv run python -c "import arcflow; print(arcflow.__version__)"
```

### Issue: Streaming query fails

```python
# Enable logging
import logging
logging.basicConfig(level=logging.INFO)
```

### Issue: Merge keys error

```python
# For upsert mode, ALWAYS specify merge keys
StageConfig(
    mode='upsert',
    merge_keys=['id', 'timestamp']  # Don't forget this!
)
```

## Development Tips

### Tip 1: Use Batch Mode for Testing
```python
config = {'streaming_enabled': False}  # Faster iteration
```

### Tip 2: Test One Zone at a Time
```python
orchestrator.run_zone_pipeline('bronze')  # Just bronze
orchestrator.run_zone_pipeline('silver', source_zone='bronze')  # Just silver
```

### Tip 3: Monitor Streaming Queries
```python
status = orchestrator.get_status()
print(status)
```

### Tip 4: Graceful Shutdown
```python
try:
    orchestrator.run_full_pipeline()
except KeyboardInterrupt:
    orchestrator.stop_all()
```

## Complete Examples

See these files for full examples:

1. **examples/config_complete.py** - Complete industrial IoT setup
2. **examples/transformations.py** - Custom transformation examples
3. **src/arcflow/main.py** - Full production entry point
4. **README_arcflow.md** - Comprehensive documentation

## Deploy to Fabric

### Quick Deployment

```bash
# 1. Build package
uv build

# 2. Upload to Fabric
# - Go to Workspace Settings → Data Engineering/Science
# - Upload dist/arcflow-0.1.0-py3-none-any.whl

# 3. Create Spark Job Definition
# - Main file: src/arcflow/main.py
# - Attach library: arcflow-0.1.0-py3-none-any.whl

# 4. Run!
```

## Get Help

- **Documentation**: README_arcflow.md
- **Examples**: examples/ directory
- **Implementation Details**: IMPLEMENTATION_SUMMARY.md

---

**Happy Data Engineering! 🚀**

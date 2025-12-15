# Deploying ArcFlow to Microsoft Fabric

This guide explains how to deploy ArcFlow as a Spark Job Definition in Microsoft Fabric.

## Architecture

```
Microsoft Fabric Workspace
├── Environment
│   └── arcflow-0.1.0-py3-none-any.whl (library)
│
└── Spark Job Definition
    ├── main.py (entry point)
    └── pipeline_config.py (reference file)
```

## Step-by-Step Deployment

### 1. Build the Wheel

```bash
# From your project directory
uv build

# This creates:
# dist/arcflow-0.1.0-py3-none-any.whl
```

### 2. Create or Update Environment

**Option A: Create New Environment**
1. In your Fabric workspace, click **+ New** → **Environment**
2. Name it (e.g., `arcflow-env`)
3. Go to **Public libraries** tab
4. Click **Add from .whl**
5. Upload `dist/arcflow-0.1.0-py3-none-any.whl`
6. Click **Save** and wait for environment to build

**Option B: Use Existing Environment**
1. Open your existing environment
2. Go to **Public libraries** tab
3. Click **Add from .whl**
4. Upload `dist/arcflow-0.1.0-py3-none-any.whl`
5. Click **Save**

### 3. Create Spark Job Definition

1. In your workspace, click **+ New** → **Spark Job Definition**
2. Name it (e.g., `arcflow-pipeline`)

### 4. Configure Spark Job Definition

**Main File:**
1. Click **Upload files** or drag & drop
2. Upload: `main.py` (from project root)
3. Set as **Main file**

**Reference Files:**
1. Click **Upload files** or drag & drop
2. Upload: `pipeline_config.py` (from project root)

**Environment:**
1. Click **Environment** dropdown
2. Select the environment with arcflow wheel (e.g., `arcflow-env`)

**Optional - Spark Properties:**
```properties
# Tune based on your data volume
spark.executor.instances: 4
spark.executor.cores: 4
spark.executor.memory: 16g
spark.driver.memory: 8g
```

### 5. Configure Your Pipeline

Edit `pipeline_config.py` to define your tables:

```python
def get_table_registry() -> Dict[str, SourceConfig]:
    tables = {}
    
    tables['my_table'] = SourceConfig(
        name='my_table',
        format='parquet',
        landing_path='Files/landing/my_table/',
        zones={
            'bronze': ZoneConfig(enabled=True, mode='append'),
            'silver': ZoneConfig(
                enabled=True,
                mode='upsert',
                merge_keys=['id']
            )
        }
    )
    
    return tables
```

### 6. Run the Job

1. Click **Run** button
2. Monitor execution in **Run history**
3. Check logs for progress

## File Structure

### main.py
- Entry point for Spark Job Definition
- Imports from installed `arcflow` wheel
- Imports configuration from `pipeline_config.py`
- Orchestrates the full ELT pipeline

### pipeline_config.py
- Defines table registry (single-source tables)
- Defines dimension registry (multi-source models)
- Registers custom transformations
- Uploaded as **reference file** (not main)

### arcflow wheel
- Installed in Environment
- Contains all framework code
- Provides: Controller, SourceConfig, ZoneConfig, etc.

## Configuration Options

### Streaming vs Batch Mode

In `main.py`, edit `get_pipeline_config()`:

```python
# For development/testing (faster)
return get_config({
    'streaming_enabled': False  # Batch mode
})

# For production (continuous)
return get_config({
    'streaming_enabled': True  # Streaming mode
})
```

### Custom Paths

Override default paths in `get_pipeline_config()`:

```python
return get_config({
    'streaming_enabled': True,
    'landing_uri': 'Files/landing/',
    'checkpoint_uri': 'Files/checkpoints/',
    'archive_path': 'Files/archive/',
})
```

### Performance Tuning

```python
return get_config({
    'streaming_enabled': True,
    'max_files_per_trigger': 1000,  # Adjust based on file size
    'trigger_interval': '60 seconds',  # Micro-batch interval
})
```

## Typical Workflow

### Development
1. Edit `pipeline_config.py` locally
2. Test with batch mode: `'streaming_enabled': False`
3. Upload updated `pipeline_config.py` to Spark Job Definition
4. Run and verify

### Adding New Tables
1. Edit `pipeline_config.py` → `get_table_registry()`
2. Add new `SourceConfig` entry
3. Upload updated file to Spark Job Definition
4. Run job

### Adding Custom Transformations
1. Edit `pipeline_config.py` → `register_custom_transformations()`
2. Use `@register_zone_transformer` decorator
3. Reference by name in `ZoneConfig`
4. Upload updated file
5. Run job

### Updating Framework
1. Build new wheel: `uv build`
2. Upload to Environment
3. Wait for environment to rebuild
4. Run job (automatically uses new version)

## Monitoring

### Check Logs
1. Open Spark Job Definition
2. Go to **Run history**
3. Click on a run
4. View **Logs** tab

### Check Streaming Status
Logs will show:
```
✓ Loaded 3 tables
✓ Loaded 2 dimensions
✓ Streaming mode: True
✓ Started 5 streams for bronze zone
✓ Started 3 streams for silver zone
```

### Check Data
Query your lakehouse:
```sql
SELECT * FROM bronze.sensor_data LIMIT 10;
SELECT * FROM silver.sensor_data LIMIT 10;
SELECT * FROM gold.sensor_data LIMIT 10;
```

## Troubleshooting

### Import Error: "No module named 'arcflow'"
- **Cause**: Environment doesn't have arcflow wheel
- **Fix**: Upload wheel to Environment and wait for build

### File Not Found: "pipeline_config"
- **Cause**: Reference file not uploaded
- **Fix**: Upload `pipeline_config.py` as reference file

### Table Not Found
- **Cause**: Table not in registry or zone not enabled
- **Fix**: Check `get_table_registry()` in `pipeline_config.py`

### Checkpoint Already Exists
- **Cause**: Reprocessing with existing checkpoint
- **Fix**: Delete checkpoint folder or change checkpoint path

## Example: Complete Setup

**1. pipeline_config.py**
```python
def get_table_registry():
    return {
        'sensor_data': SourceConfig(
            name='sensor_data',
            format='parquet',
            landing_path='Files/landing/sensors/',
            zones={
                'bronze': ZoneConfig(enabled=True, mode='append'),
                'silver': ZoneConfig(enabled=True, mode='upsert', 
                                    merge_keys=['sensor_id', 'timestamp'])
            }
        )
    }
```

**2. Upload Files**
- Environment: `arcflow-0.1.0-py3-none-any.whl`
- Spark Job Definition:
  - Main file: `main.py`
  - Reference: `pipeline_config.py`

**3. Run**
- Click **Run**
- Monitor logs
- Verify data in Tables

## Best Practices

1. **Version Control**: Keep `pipeline_config.py` in git
2. **Testing**: Test with batch mode first (`streaming_enabled=False`)
3. **Incremental**: Add tables one at a time
4. **Monitoring**: Check logs regularly
5. **Checkpoints**: Use unique checkpoint paths per table
6. **Environment**: Create separate environments for dev/prod
7. **Documentation**: Document custom transformations

## Next Steps

- Review `pipeline_config.py` examples
- Add your tables to the registry
- Define custom transformations
- Test in batch mode
- Deploy to production with streaming

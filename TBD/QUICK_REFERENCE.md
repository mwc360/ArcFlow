# ArcFlow - Spark Job Definition Quick Reference

## 📁 File Structure

```
your-project/
├── dist/
│   └── arcflow-0.1.0-py3-none-any.whl    # Framework (upload to Environment)
│
├── main.py                                # Entry point (Spark Job main file)
├── pipeline_config.py                     # Configuration (reference file)
│
└── src/arcflow/                           # Source code (framework)
    └── ...
```

## 🚀 Deployment Checklist

### ☑️ Environment Setup
- [ ] Build wheel: `uv build`
- [ ] Create Fabric Environment (or use existing)
- [ ] Upload `arcflow-0.1.0-py3-none-any.whl` to Environment
- [ ] Wait for Environment to finish building

### ☑️ Spark Job Definition Setup
- [ ] Create new Spark Job Definition
- [ ] Upload `main.py` as **Main file**
- [ ] Upload `pipeline_config.py` as **Reference file**
- [ ] Select Environment with arcflow wheel
- [ ] (Optional) Configure Spark properties

### ☑️ Configuration
- [ ] Edit `pipeline_config.py` → `get_table_registry()`
- [ ] Edit `pipeline_config.py` → `get_dimension_registry()`
- [ ] Edit `pipeline_config.py` → `register_custom_transformations()`
- [ ] Set streaming mode in `main.py` → `get_pipeline_config()`

### ☑️ Testing
- [ ] Test with batch mode first (`streaming_enabled=False`)
- [ ] Verify tables created in lakehouse
- [ ] Check logs for errors
- [ ] Switch to streaming mode for production

## 📝 Key Files Explained

### main.py
**Purpose**: Spark Job Definition entry point  
**Location**: Upload as main file  
**What it does**:
- Imports arcflow from installed wheel
- Loads configuration from pipeline_config.py
- Creates Spark session
- Runs the ELT pipeline

**Edit this for**:
- Changing streaming vs batch mode
- Overriding default paths
- Performance tuning

### pipeline_config.py
**Purpose**: Table and dimension definitions  
**Location**: Upload as reference file  
**What it does**:
- Defines all tables to process
- Defines dimensional models
- Registers custom transformations

**Edit this for**:
- Adding new tables
- Configuring zones (bronze/silver/gold)
- Adding custom business logic

### arcflow wheel
**Purpose**: Framework code  
**Location**: Environment libraries  
**What it provides**:
- Controller class
- FlowConfig, StageConfig, DimensionConfig
- Readers, writers, transformations
- Pipeline orchestration

**Update when**:
- Framework features change
- Bug fixes
- New capabilities added

## 🔧 Common Configurations

### Batch Mode (Testing)
```python
# In main.py → get_pipeline_config()
return get_config({
    'streaming_enabled': False
})
```

### Streaming Mode (Production)
```python
# In main.py → get_pipeline_config()
return get_config({
    'streaming_enabled': True,
    'trigger_interval': '60 seconds',
    'max_files_per_trigger': 1000
})
```

### Add a Table
```python
# In pipeline_config.py → get_table_registry()
tables['my_table'] = FlowConfig(
    name='my_table',
    format='parquet',
    landing_path='Files/landing/my_table/',
    zones={
        'bronze': StageConfig(
            enabled=True,
            mode='append'
        ),
        'silver': StageConfig(
            enabled=True,
            mode='upsert',
            merge_keys=['id'],
            custom_transform='clean_my_table'
        )
    }
)
```

### Add Custom Transformation
```python
# In pipeline_config.py → register_custom_transformations()
@register_zone_transformer
def clean_my_table(df: DataFrame, table_config, zone_config) -> DataFrame:
    """Remove nulls and invalid values"""
    return df.filter(col('value') >= 0).dropna()
```

## 🎯 Typical Workflow

1. **Local Development**
   ```bash
   # Edit pipeline_config.py
   # Test locally (optional)
   # Build wheel
   uv build
   ```

2. **First Deployment**
   - Upload wheel to Environment
   - Create Spark Job Definition
   - Upload main.py (main file)
   - Upload pipeline_config.py (reference)
   - Run in batch mode first

3. **Iterating**
   - Edit pipeline_config.py
   - Upload updated file to Spark Job Definition
   - Run job
   - (No need to rebuild wheel if only config changes)

4. **Framework Updates**
   - Make changes to src/arcflow/
   - Rebuild: `uv build`
   - Upload new wheel to Environment
   - Wait for environment rebuild
   - Run job with updated framework

## 📊 Monitoring

### Check if Job Started
```
Starting ArcFlow ELT Framework
✓ Custom transformations registered
✓ Loaded 3 tables
✓ Loaded 2 dimensions
✓ Spark version: 3.5.0
```

### Check Pipeline Progress
```
✓ Started 3 streams for bronze zone
✓ Started 3 streams for silver zone
✓ Started 2 streams for gold zone
✓ Started 2 dimension streams for gold zone
```

### Check Completion
```
✓ ArcFlow ELT Framework completed successfully
```

## 🐛 Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `No module named 'arcflow'` | Wheel not in Environment | Upload wheel, rebuild environment |
| `No module named 'pipeline_config'` | Reference file missing | Upload pipeline_config.py |
| `Controller not found` | Wrong import name | Check it's `Controller` not `Orchestrator` |
| Checkpoint exists | Rerunning stream | Delete checkpoint or change path |
| Table not found | Zone not enabled | Check zone config in pipeline_config.py |

## 💡 Pro Tips

1. **Version Control**: Keep pipeline_config.py in git
2. **Testing**: Always test batch mode first
3. **Logging**: Check logs regularly in Run History
4. **Checkpoints**: Each table gets unique checkpoint path automatically
5. **Incremental**: Add one table at a time, test, then add more
6. **Environment**: Use separate environments for dev/test/prod
7. **Reference**: Keep FABRIC_DEPLOYMENT.md handy

## 📚 Documentation

- **FABRIC_DEPLOYMENT.md** - Complete deployment guide
- **README.md** - Framework overview
- **pipeline_config.py** - Configuration examples with comments
- **main.py** - Entry point with inline documentation

## ✅ Success Criteria

Your deployment is successful when:
- ✓ Job runs without errors
- ✓ Logs show all tables processing
- ✓ Data appears in lakehouse Tables
- ✓ Bronze/silver/gold zones populated
- ✓ Streaming queries running (if enabled)

---

**Need Help?** Check FABRIC_DEPLOYMENT.md for detailed troubleshooting

# Development Environment Setup

## ⚠️ Known Issue: PySpark 3.5 + Windows + Python 3.14

There's a known compatibility issue with PySpark 3.5 on Windows with Python 3.14 related to object serialization (pickling). This manifests as a `RecursionError: Stack overflow` when creating DataFrames locally.

### Workaround Options:

#### Option 1: Use Python 3.11 (Recommended for Local Development)

```powershell
# Remove current environment
uv venv --python 3.11

# Reinstall dependencies
uv sync
```

#### Option 2: Develop Directly in Microsoft Fabric

Since Fabric uses Linux-based Spark clusters with Python 3.11, your code will work perfectly in production. You can:

1. Upload your modules directly to Fabric workspace
2. Test and develop using Fabric notebooks
3. Deploy as Spark Job Definition when ready

#### Option 3: Use WSL2 (Windows Subsystem for Linux)

```bash
# In WSL2
cd /mnt/c/Users/milescole/source/ArcFlow
python3.11 -m venv .venv
source .venv/bin/activate
pip install pyspark==3.5.7
python test_spark_local.py
```

## Project Structure

Your project is now set up with:

```
ArcFlow/
├── src/
│   └── industrial_streaming/
│       ├── __init__.py           # Package initialization
│       ├── spark_job.py          # Main Spark job (ready for Fabric)
│       └── config.py              # Configuration management
├── tests/
│   └── test_spark_job.py         # Unit tests
├── examples/
│   └── run_local.py               # Local development example
├── pyproject.toml                 # UV project configuration
├── .gitignore                     # Git ignore patterns
└── README.md                      # Project documentation
```

## Module Features

### `spark_job.py` - Production-Ready Features

✅ **Fabric-Compatible**: Automatically detects and uses Fabric Spark session  
✅ **Local Development**: Creates local Spark session when Fabric not available  
✅ **Delta Lake Support**: Reads/writes Delta format for Fabric lakehouses  
✅ **Logging**: Comprehensive logging for debugging and monitoring  
✅ **Error Handling**: Robust error handling with proper exceptions  
✅ **Configurable**: Supports input/output paths and parameters  

### Key Components

1. **IndustrialStreamingJob** class
   - `__init__()` - Initialize with or without Spark session
   - `run()` - Main execution method
   - `read_data()` - Read from various formats (Delta, Parquet, CSV)
   - `process_data()` - Transform and process data
   - `write_data()` - Write to Delta Lake format

2. **Configuration** (`config.py`)
   - `SparkConfig` - Spark-specific settings
   - `JobConfig` - Job parameters and paths

## Deploying to Microsoft Fabric

### Method 1: Build and Upload Wheel Package

```powershell
# Build the wheel package
$env:Path = "C:\Users\milescole\.local\bin;$env:Path"
uv build
```

This creates a `.whl` file in the `dist/` folder.

**In Fabric**:
1. Go to your workspace
2. Create new **Spark Job Definition**
3. Upload the `.whl` file
4. Set main module: `industrial_streaming.spark_job`
5. Configure parameters (optional):
   - `input_path`
   - `output_path`
   - `lakehouse_name`

### Method 2: Direct Python File Upload

1. Navigate to `src/industrial_streaming/spark_job.py`
2. Copy the file content
3. In Fabric, create a new **Spark Job Definition**
4. Paste the code directly
5. The job will use Fabric's native PySpark environment

### Method 3: Use Fabric Notebook (For Development/Testing)

```python
# In a Fabric notebook cell
from industrial_streaming.spark_job import IndustrialStreamingJob

# Fabric provides the spark session automatically
job = IndustrialStreamingJob(spark=spark)

# Run with lakehouse paths
job.run(
    input_path="Tables/raw_sensor_data",
    output_path="Tables/processed_sensor_data"
)
```

## Testing in Fabric

Once uploaded, you can test the job:

1. **Attach a Lakehouse** to your Spark Job Definition
2. **Set Parameters** (if needed):
   ```json
   {
     "input_table": "raw_sensor_data",
     "output_table": "processed_data"
   }
   ```
3. **Run the job** and monitor logs

## Next Steps

1. **Customize the Processing Logic**: Edit `process_data()` method in `spark_job.py`
2. **Add More Transformations**: Extend with your specific business logic
3. **Configure for Your Data**: Update `config.py` with your parameters
4. **Add Tests**: Expand `test_spark_job.py` with your test cases
5. **Schedule in Fabric**: Set up scheduling in Fabric for automated runs

## Dependencies

- **PySpark 3.5.x**: Matches Fabric Spark runtime
- **Python >=3.10**: Compatible with Fabric

## Best Practices for Fabric Spark Jobs

1. **Use Delta Lake** for all lakehouse tables
2. **Leverage Fabric's mssparkutils** for advanced features
3. **Use adaptive query execution** (already configured)
4. **Implement proper logging** for debugging
5. **Handle failures gracefully** with try/except blocks
6. **Use parameters** for flexibility across environments

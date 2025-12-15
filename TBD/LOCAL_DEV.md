# Local Development on Windows - Quick Reference

## ✅ Setup Complete!

Your environment is now configured for local PySpark development on Windows with Python 3.11 (matching Fabric's runtime).

## 🚀 Quick Start Commands

### Run the Test Script
```powershell
uv run python test_spark_local.py
```

### Run the Module Example
```powershell
uv run python examples/run_local.py
```

### Run Unit Tests
```powershell
uv run pytest tests/ -v
```

### Run Tests with Coverage
```powershell
uv run pytest tests/ --cov=industrial_streaming --cov-report=html
```

### Build the Package for Fabric
```powershell
uv build
```

## 📁 Project Files

### Main Module Files
- `src/industrial_streaming/spark_job.py` - Your main Spark job
- `src/industrial_streaming/config.py` - Configuration classes

### Test Files
- `test_spark_local.py` - Quick local test (standalone)
- `examples/run_local.py` - Module usage example
- `tests/test_spark_job.py` - Unit tests

### Package Files
- `dist/industrial_streaming-0.1.0-py3-none-any.whl` - Built package for Fabric

## 🔧 Development Workflow

### 1. Make Changes
Edit files in `src/industrial_streaming/`

### 2. Test Locally
```powershell
# Quick test
uv run python test_spark_local.py

# Full test suite
uv run pytest tests/ -v
```

### 3. Build for Fabric
```powershell
uv build
```

### 4. Deploy to Fabric
Upload `dist/industrial_streaming-0.1.0-py3-none-any.whl` to your Fabric workspace

## 🎯 Common Development Tasks

### Add a New Transformation
Edit the `process_data()` method in `src/industrial_streaming/spark_job.py`:

```python
def process_data(self, df: DataFrame) -> DataFrame:
    """Process the industrial streaming data"""
    logger.info("Processing data")
    
    # Add your transformations here
    processed_df = (df
        .withColumn("processing_timestamp", current_timestamp())
        .filter(col("value").isNotNull())
        # Add more transformations...
        .withColumn("quality_flag", 
            when(col("value") > 100, "high")
            .otherwise("normal"))
    )
    
    return processed_df
```

### Test with Your Own Data
Modify `test_spark_local.py` to load your data:

```python
# Instead of create_sample_data(), load your data
df = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)
```

### Add a New Configuration Parameter
Edit `src/industrial_streaming/config.py`:

```python
@dataclass
class JobConfig:
    """Job configuration settings"""
    input_path: Optional[str] = None
    output_path: Optional[str] = None
    # Add your parameters
    max_threshold: float = 100.0
    min_threshold: float = 0.0
```

## 🐛 Debugging Tips

### Enable Verbose Spark Logging
In your script:
```python
spark.sparkContext.setLogLevel("INFO")  # or "DEBUG"
```

### Print DataFrame Schema
```python
df.printSchema()
```

### Show Sample Data
```python
df.show(20, truncate=False)
```

### Check Data Quality
```python
df.describe().show()
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
```

## 📦 Package Management

### Add a New Dependency
```powershell
uv add package-name
```

### Add a Dev Dependency
```powershell
uv add --dev package-name
```

### Update Dependencies
```powershell
uv sync
```

## 🔄 Keeping in Sync with Fabric

### Python Version
- **Local**: Python 3.11 (✓ matches Fabric)
- **Fabric**: Python 3.11

### PySpark Version
- **Local**: PySpark 3.5.7 (✓ matches Fabric)
- **Fabric**: PySpark 3.5.x

### Delta Lake
PySpark 3.5 includes Delta Lake support by default, matching Fabric.

## 💡 Pro Tips

1. **Use the standalone test script** (`test_spark_local.py`) for quick iterations
2. **Run unit tests** (`pytest tests/`) before committing changes
3. **Build frequently** to catch packaging issues early
4. **Test in Fabric early** - some features only work in the cloud environment
5. **Use logging** extensively - it helps debug in Fabric where you can't print()

## 🎓 Next Steps

1. **Customize the processing logic** in `spark_job.py`
2. **Add your data sources** (CSV, Parquet, Delta, etc.)
3. **Write comprehensive tests** for your transformations
4. **Build and deploy** to Fabric
5. **Set up CI/CD** (optional) for automated testing and deployment

## 📚 Resources

- **PySpark Documentation**: https://spark.apache.org/docs/latest/api/python/
- **UV Documentation**: https://docs.astral.sh/uv/
- **Fabric Spark Jobs**: https://learn.microsoft.com/en-us/fabric/data-engineering/spark-job-definition
- **Delta Lake**: https://delta.io/

---

**Your local development environment is ready! Happy coding! 🚀**

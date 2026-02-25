# Local Development Getting Started Guide

This guide will help you set up a local development environment for **ArcFlow** where changes to the source code are immediately available without reinstalling.

---

## Prerequisites

- [uv](https://docs.astral.sh/uv/) — fast Python package manager

---

## 🎯 Quick Start

### 1. Create Development Virtual Environment

Run the provided PowerShell script to create a development environment:

```powershell
.\create_dev_venv.ps1
```

Or manually with uv:

```powershell
uv sync --group dev
```

This will:
- Create a `.venv` virtual environment (if it doesn't exist)
- Install ArcFlow in **editable mode** with all dev dependencies
- Lock versions from `uv.lock`

### 2. Run Commands

Use `uv run` to execute commands in the synced environment:

```powershell
uv run pytest
uv run python -c "from arcflow import Controller; print('OK')"
```

Or activate the venv directly:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Start Developing!

Any changes you make to files in `src/arcflow/` will be immediately available - no need to reinstall!

---

## 🔧 What is Editable Mode?

When you run `uv sync`, ArcFlow is installed in **editable mode** (development mode). Python creates a link to your source code instead of copying it. This means:

✅ **Edit** → **Test** → **Repeat** (no reinstall step!)
✅ Changes are live immediately
✅ Perfect for active development and debugging

### Standard Install vs Editable Install

```powershell
# Standard Install (copies files, requires reinstall after changes)
pip install arcflow-0.1.0-py3-none-any.whl

# Editable Install via uv (links to source, changes are live)
uv sync --group dev
```

---

## 📁 Project Structure

```
ArcFlow/
├── src/
│   └── arcflow/          ← YOUR CODE LIVES HERE
│       ├── __init__.py
│       ├── config.py
│       ├── controller.py
│       ├── models.py
│       ├── core/
│       ├── pipelines/
│       ├── readers/
│       ├── transformations/
│       ├── utils/
│       └── writers/
├── examples/
│   └── arcflow_example.ipynb
├── tests/
├── .venv/                ← Development virtual environment (created by uv)
├── create_dev_venv.ps1   ← Setup script
├── pyproject.toml        ← Project configuration
└── uv.lock               ← Locked dependency versions
```

---

## 🧪 Development Workflow

### Making Changes

1. **Edit** any file in `src/arcflow/`
2. **Save** the file
3. **Run/Test** immediately - changes are live!

### Example Workflow

```python
# 1. Edit src/arcflow/controller.py
# 2. Save the file
# 3. In your notebook or terminal:

from arcflow import Controller  # Your changes are already loaded!
```

### Auto-Reload in Jupyter/IPython

Add this to the top of your notebooks to automatically reload changed modules:

```python
%load_ext autoreload
%autoreload 2
```

Now when you edit `src/arcflow/*.py` and save, the notebook will automatically pick up changes!

---

## 🌊 Developing a New Stream Pipeline

ArcFlow has a built-in iterative development flow for Kafka and Event Hubs sources.
`ZonePipeline.test_input()` and `test_output()` **stream data into a Spark memory view** using
`trigger(availableNow=True)` — no checkpoints, no Delta writes, row-limited. This lets you
explore and validate a stream entirely in a notebook before wiring it into the full pipeline.

### Step 1 — Discover the payload (no schema yet)

```python
from arcflow.pipelines.zone_pipeline import ZonePipeline
from pipeline_config import tables

pipeline = ZonePipeline(spark=spark, zone="bronze", config={"streaming_enabled": True})

# Streams real messages into memory, returns them as a batch DataFrame
df = pipeline.test_input(tables["shipment"], raw=True, limit=20)
df.show(truncate=False)
```

`raw=True` skips JSON deserialization entirely — you see the raw message body as a string
alongside all connector metadata columns. No schema is required.

**Kafka metadata columns:** `key`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`, `headers`

**Event Hubs metadata columns:** `offset`, `sequenceNumber`, `enqueuedTime`, `publisher`, `partitionKey`, `properties`, `systemProperties`

> **How it works:** ArcFlow opens a real streaming connection to the broker, reads all
> backlogged messages using `availableNow` trigger (stops automatically when caught up),
> writes results to a Spark memory view, then returns a limited batch for inspection.
> The view (`_arcflow_test_<name>`) persists for follow-up `spark.sql(...)` queries.

---

### Step 2 — Draft a schema and validate deserialization

Add a `schema` to your `FlowConfig` based on what you saw in Step 1, then call `test_input`
without `raw=True`:

```python
# raw=False (default) — applies from_json() using FlowConfig.schema
df = pipeline.test_input(tables["shipment"], limit=20)
df.printSchema()
df.show()
```

If columns are null or missing, refine the `StructType` in `pipeline_config.py` and retry.

---

### Step 3 — Test the transformer

Add a `custom_transform` to the zone's `StageConfig` in `pipeline_config.py`, then call
`test_output` to see the full result — schema deserialization + transformer + snake_case
normalisation applied:

```python
# Applies: from_json(schema) → custom_transform → snake_case → _processing_timestamp
df = pipeline.test_output(tables["shipment"], limit=20)
df.show()
```

> **Memory view:** `_arcflow_test_out_<name>` — persists for further `spark.sql(...)` queries.

---

### Step 4 — Ship it

Once `test_output` looks right:

```python
from arcflow import Controller
from pipeline_config import tables

controller = Controller(spark, config, tables)
controller.run_zone_pipeline("bronze")   # starts the real streaming query
```

---

### `test_input` / `test_output` reference

| Method | `raw` flag | What runs | Memory view |
|---|---|---|---|
| `test_input(..., raw=True)` | No schema needed | Raw bytes → string | `_arcflow_test_<name>` |
| `test_input(...)` | Schema required | `from_json(schema)` | `_arcflow_test_<name>` |
| `test_output(...)` | Schema required | `from_json` + transformer + normalise | `_arcflow_test_out_<name>` |

Both methods work for **file-based sources** too (parquet, json, csv) — they use a standard
batch read with `.limit(limit)` instead of the streaming path. The `raw` flag is ignored for
file sources.

---

## 🚀 Running Tests

### Run all tests:
```powershell
uv run pytest
```

### Run with coverage:
```powershell
uv run pytest --cov=arcflow --cov-report=html
```

### Run specific test file:
```powershell
uv run pytest tests/test_controller.py
```

### Run specific test:
```powershell
uv run pytest tests/test_controller.py::test_function_name
```

---

## 📓 Using with Jupyter Notebooks

### Option 1: Launch Jupyter from Dev Environment

```powershell
# Install jupyter (if not already a dependency)
uv add --group dev jupyter

# Launch notebook
uv run jupyter notebook examples/
```

### Option 2: Add Kernel to VS Code

VS Code can automatically detect and use `.venv`:

1. Open `examples/arcflow_example.ipynb`
2. Click the kernel selector (top-right)
3. Choose "Select Another Kernel" → "Python Environments"
4. Select `.venv (Python 3.x.x)`

---

## 🛠️ Common Development Tasks

### Check Installed Packages
```powershell
uv pip list
```

### Verify Editable Install
```powershell
uv pip show arcflow
```
Look for: `Location: c:\users\...\arcflow\src` (points to your source!)

### Update Dependencies
```powershell
uv lock --upgrade-package pyspark
uv sync --group dev
```

### Rebuild if Needed
```powershell
# Only needed if you change pyproject.toml
uv sync --group dev --reinstall
```

---

## 🐛 Debugging Tips

### 1. Import Errors After Changes

If you're NOT using `%autoreload` in notebooks, restart the Python kernel:
- **Jupyter**: Kernel → Restart
- **VS Code**: Click "Restart" in notebook toolbar

### 2. Changes Not Appearing

Verify you're using the dev environment:
```python
import arcflow
print(arcflow.__file__)
# Should show: c:\Users\milescole\source\ArcFlow\src\arcflow\__init__.py
```

### 3. Check Active Environment

```powershell
# In PowerShell (should show venv-dev path)
Get-Command python | Select-Object Source

# Or in Python
import sys
print(sys.executable)
```

---

## 📊 Example Development Session

```powershell
# 1. Sync environment
uv sync --group dev

# 2. Edit source code
code src/arcflow/controller.py

# 3. Test changes in Python
uv run python
>>> from arcflow import Controller
>>> # Your changes are live!

# 4. Or run notebook with changes
uv run jupyter notebook examples/arcflow_example.ipynb

# 5. Run tests
uv run pytest tests/

# 6. Make more changes, repeat!
```

---

## 🆚 When to Use Each Environment

### Use `.venv` (uv sync, Editable Install) When:
✅ Actively developing ArcFlow
✅ Testing changes frequently
✅ Debugging issues
✅ Adding new features
✅ Running tests

### Use Wheel Install When:
✅ Testing deployment packages
✅ Simulating production environment
✅ Validating wheel builds
✅ Final QA before release

---

## 📋 Environment Comparison

| Feature | uv sync (Editable) | Wheel Install |
|---------|---------------------|-------------------|
| Setup Command | `uv sync --group dev` | `pip install arcflow-x.x.x.whl` |
| Code Changes | Live immediately | Requires reinstall |
| Use Case | Development | Testing deployment |
| Location | Links to `src/` | Copies to `site-packages/` |
| Debugging | Easy (source visible) | Harder (compiled) |

---

## 🔄 Switching Environments

### Deactivate Current Environment
```powershell
deactivate
```

### Activate Dev Environment
```powershell
.\.venv\Scripts\Activate.ps1
```

---

## 📚 Next Steps

1. ✅ Sync dev environment: `uv sync --group dev` (or `.\create_dev_venv.ps1`)
2. ✅ Run commands via: `uv run <command>`
3. ✅ Open notebook: `examples/arcflow_example.ipynb`
4. ✅ Add autoreload magic: `%load_ext autoreload; %autoreload 2`
5. ✅ Start coding!

---

## 🆘 Troubleshooting

### "uv sync" fails

Make sure you're in the project root directory and uv is installed:
```powershell
cd c:\Users\milescole\source\ArcFlow
uv --version
```

### Python can't find arcflow after install

Verify installation:
```powershell
uv pip show arcflow
uv run python -c "import arcflow; print(arcflow.__file__)"
```

### Autoreload not working in notebook

Restart the notebook kernel and ensure autoreload is enabled:
```python
%load_ext autoreload
%autoreload 2
```

---

## 📖 Additional Resources

- **pyproject.toml**: Project configuration and dependencies
- **uv.lock**: Locked dependency versions
- **README.md**: High-level project overview
- **tests/**: Test suite examples
- **examples/**: Sample notebooks and usage patterns

---

Happy Coding! 🚀

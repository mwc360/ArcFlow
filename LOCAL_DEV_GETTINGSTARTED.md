# Local Development Getting Started Guide

This guide will help you set up a local development environment for **ArcFlow** where changes to the source code are immediately available without reinstalling.

---

## 🎯 Quick Start

### 1. Create Development Virtual Environment

Run the provided PowerShell script to create a development environment:

```powershell
.\create_dev_venv.ps1
```

This script will:
- Create a new `venv-dev` virtual environment
- Install ArcFlow in **editable mode** (`pip install -e .`)
- Install development dependencies (pytest, pytest-cov)
- Verify the installation

### 2. Activate the Environment

```powershell
.\venv-dev\Scripts\Activate.ps1
```

You should see `(venv-dev)` in your terminal prompt.

### 3. Start Developing!

Any changes you make to files in `src/arcflow/` will be immediately available - no need to reinstall!

---

## 🔧 What is Editable Mode?

When you install a package in **editable mode** (also called "development mode"), Python creates a link to your source code instead of copying it. This means:

✅ **Edit** → **Test** → **Repeat** (no reinstall step!)
✅ Changes are live immediately
✅ Perfect for active development and debugging

### Standard Install vs Editable Install

```powershell
# Standard Install (copies files, requires reinstall after changes)
pip install arcflow-0.1.0-py3-none-any.whl

# Editable Install (links to source, changes are live)
pip install -e .
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
├── venv-dev/             ← Development virtual environment
├── create_dev_venv.ps1   ← Setup script
└── pyproject.toml        ← Project configuration
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

## 🚀 Running Tests

### Run all tests:
```powershell
pytest
```

### Run with coverage:
```powershell
pytest --cov=arcflow --cov-report=html
```

### Run specific test file:
```powershell
pytest tests/test_controller.py
```

### Run specific test:
```powershell
pytest tests/test_controller.py::test_function_name
```

---

## 📓 Using with Jupyter Notebooks

### Option 1: Launch Jupyter from Dev Environment

```powershell
# Activate environment
.\venv-dev\Scripts\Activate.ps1

# Install jupyter (if not already installed)
pip install jupyter

# Launch notebook
jupyter notebook examples/
```

### Option 2: Add Kernel to VS Code

VS Code can automatically detect and use `venv-dev`:

1. Open `examples/arcflow_example.ipynb`
2. Click the kernel selector (top-right)
3. Choose "Select Another Kernel" → "Python Environments"
4. Select `venv-dev (Python 3.x.x)`

---

## 🛠️ Common Development Tasks

### Check Installed Packages
```powershell
pip list
```

### Verify Editable Install
```powershell
pip show arcflow
```
Look for: `Location: c:\users\...\arcflow\src` (points to your source!)

### Update Dependencies
```powershell
pip install --upgrade pyspark delta-spark
```

### Rebuild if Needed
```powershell
# Only needed if you change pyproject.toml
pip install -e . --force-reinstall --no-deps
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
# 1. Activate environment
.\venv-dev\Scripts\Activate.ps1

# 2. Edit source code
code src/arcflow/controller.py

# 3. Test changes in Python
python
>>> from arcflow import Controller
>>> # Your changes are live!

# 4. Or run notebook with changes
jupyter notebook examples/arcflow_example.ipynb

# 5. Run tests
pytest tests/

# 6. Make more changes, repeat!
```

---

## 🆚 When to Use Each Environment

### Use `venv-dev` (Editable Install) When:
✅ Actively developing ArcFlow
✅ Testing changes frequently
✅ Debugging issues
✅ Adding new features
✅ Running tests

### Use `venv-test` (Wheel Install) When:
✅ Testing deployment packages
✅ Simulating production environment
✅ Validating wheel builds
✅ Final QA before release

---

## 📋 Environment Comparison

| Feature | venv-dev (Editable) | venv-test (Wheel) |
|---------|---------------------|-------------------|
| Setup Command | `pip install -e .` | `pip install arcflow-x.x.x.whl` |
| Code Changes | Live immediately | Requires reinstall |
| Use Case | Development | Testing deployment |
| Location | Links to `src/` | Copies to `site-packages/` |
| Debugging | Easy (source visible) | Harder (compiled) |

---

## 🔄 Switching Between Environments

### Deactivate Current Environment
```powershell
deactivate
```

### Activate Dev Environment
```powershell
.\venv-dev\Scripts\Activate.ps1
```

### Activate Test Environment
```powershell
.\venv-test\Scripts\Activate.ps1
```

---

## 📚 Next Steps

1. ✅ Create dev environment: `.\create_dev_venv.ps1`
2. ✅ Activate it: `.\venv-dev\Scripts\Activate.ps1`
3. ✅ Open notebook: `examples/arcflow_example.ipynb`
4. ✅ Add autoreload magic: `%load_ext autoreload; %autoreload 2`
5. ✅ Start coding!

---

## 🆘 Troubleshooting

### "pip install -e ." fails

Make sure you're in the project root directory:
```powershell
cd c:\Users\milescole\source\ArcFlow
```

### Python can't find arcflow after install

Verify installation:
```powershell
pip show arcflow
python -c "import arcflow; print(arcflow.__file__)"
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
- **README.md**: High-level project overview
- **tests/**: Test suite examples
- **examples/**: Sample notebooks and usage patterns

---

Happy Coding! 🚀

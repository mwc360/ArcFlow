# Create Development Virtual Environment for ArcFlow
# Uses uv to sync the dev dependency group from pyproject.toml and uv.lock
# Installs arcflow in editable mode (changes to source code are immediately available)

Write-Host "🔧 Creating development virtual environment for ArcFlow..." -ForegroundColor Cyan

# Step 1: Sync environment with uv (creates .venv, installs all dev deps + editable arcflow)
Write-Host "`n1️⃣ Running uv sync (dev group)..." -ForegroundColor Yellow
uv sync --group dev
Write-Host "✅ Environment synced" -ForegroundColor Green

# Step 2: Test installation
Write-Host "`n2️⃣ Testing installation..." -ForegroundColor Yellow
uv run python -c "from arcflow import Controller, FlowConfig, DimensionConfig, StageConfig, get_config; print('✅ All imports successful!')"

# Step 3: Show environment info
Write-Host "`n3️⃣ Environment Information:" -ForegroundColor Yellow
Write-Host "  📁 Location: .venv\" -ForegroundColor Cyan
Write-Host "  🐍 Python:" -ForegroundColor Cyan
uv run python --version
Write-Host "  📦 Installed packages:" -ForegroundColor Cyan
uv pip list | Select-String "arcflow|pyspark|delta"

Write-Host "`n✨ Development environment ready!" -ForegroundColor Green
Write-Host "`n💡 Tips:" -ForegroundColor Cyan
Write-Host "  • Source code location: src/arcflow/" -ForegroundColor White
Write-Host "  • Changes to Python files are immediately available (no reinstall needed)" -ForegroundColor White
Write-Host "  • Run commands via: uv run <command>" -ForegroundColor White
Write-Host "  • Run tests: uv run pytest" -ForegroundColor White
Write-Host "  • Jupyter/IPython will auto-reload with: %load_ext autoreload; %autoreload 2" -ForegroundColor White

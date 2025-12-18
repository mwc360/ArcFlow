# Create Development Virtual Environment for ArcFlow
# This script creates a venv with arcflow installed in editable mode
# for active development (changes to source code are immediately available)

Write-Host "🔧 Creating development virtual environment for ArcFlow..." -ForegroundColor Cyan

# Step 1: Remove old dev venv if it exists
Write-Host "`n1️⃣ Cleaning up old development environment..." -ForegroundColor Yellow
if (Test-Path ".venv-dev") {
    Remove-Item -Path ".venv-dev" -Recurse -Force
    Write-Host "✅ Removed old .venv-dev" -ForegroundColor Green
} else {
    Write-Host "✅ No old venv to clean up" -ForegroundColor Green
}

# Step 2: Create new venv
Write-Host "`n2️⃣ Creating new virtual environment..." -ForegroundColor Yellow
python -m venv .venv-dev
Write-Host "✅ Created .venv-dev" -ForegroundColor Green

# Step 3: Activate and upgrade pip
Write-Host "`n3️⃣ Activating environment and upgrading pip..." -ForegroundColor Yellow
& ".venv-dev\Scripts\Activate.ps1"
python -m pip install --upgrade pip
Write-Host "✅ pip upgraded" -ForegroundColor Green

# Step 4: Install arcflow in editable mode
Write-Host "`n4️⃣ Installing arcflow in editable/development mode..." -ForegroundColor Yellow
pip install -e .
Write-Host "✅ arcflow installed in editable mode (changes to src/arcflow will be live)" -ForegroundColor Green

# Step 5: Install development dependencies (optional)
Write-Host "`n5️⃣ Installing development dependencies..." -ForegroundColor Yellow
pip install pytest pytest-cov
Write-Host "✅ Dev dependencies installed" -ForegroundColor Green

# Step 6: Test installation
Write-Host "`n6️⃣ Testing installation..." -ForegroundColor Yellow
python -c "from arcflow import Controller, FlowConfig, DimensionConfig, StageConfig, get_config; print('✅ All imports successful!')"

# Step 7: Show environment info
Write-Host "`n7️⃣ Environment Information:" -ForegroundColor Yellow
Write-Host "  📁 Location: .venv-dev\" -ForegroundColor Cyan
Write-Host "  🐍 Python:" -ForegroundColor Cyan
python --version
Write-Host "  📦 Installed packages:" -ForegroundColor Cyan
pip list | Select-String "arcflow|pyspark|delta"

Write-Host "`n✨ Development environment ready!" -ForegroundColor Green
Write-Host "`n💡 Tips:" -ForegroundColor Cyan
Write-Host "  • Source code location: src/arcflow/" -ForegroundColor White
Write-Host "  • Changes to Python files are immediately available (no reinstall needed)" -ForegroundColor White
Write-Host "  • Activate this environment: venv-dev\Scripts\Activate.ps1" -ForegroundColor White
Write-Host "  • Run tests: pytest" -ForegroundColor White
Write-Host "  • Jupyter/IPython will auto-reload with: %load_ext autoreload; %autoreload 2" -ForegroundColor White

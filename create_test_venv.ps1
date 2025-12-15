# Create Test Virtual Environment for ArcFlow
# This script creates a fresh venv for testing the arcflow package

Write-Host "🔧 Creating test virtual environment for ArcFlow..." -ForegroundColor Cyan

# Step 1: Remove old test venv if it exists
Write-Host "`n1️⃣ Cleaning up old test environment..." -ForegroundColor Yellow
if (Test-Path "venv-test") {
    Remove-Item -Path "venv-test" -Recurse -Force
    Write-Host "✅ Removed old venv-test" -ForegroundColor Green
} else {
    Write-Host "✅ No old venv to clean up" -ForegroundColor Green
}

# Step 2: Create new venv
Write-Host "`n2️⃣ Creating new virtual environment..." -ForegroundColor Yellow
python -m venv venv-test
Write-Host "✅ Created venv-test" -ForegroundColor Green

# Step 3: Activate and upgrade pip
Write-Host "`n3️⃣ Activating environment and upgrading pip..." -ForegroundColor Yellow
& "venv-test\Scripts\Activate.ps1"
python -m pip install --upgrade pip
Write-Host "✅ pip upgraded" -ForegroundColor Green

# Step 4: Install arcflow from wheel
Write-Host "`n4️⃣ Installing arcflow from wheel..." -ForegroundColor Yellow
if (Test-Path "dist\arcflow-0.1.0-py3-none-any.whl") {
    pip install "dist\arcflow-0.1.0-py3-none-any.whl"
    Write-Host "✅ arcflow installed from wheel" -ForegroundColor Green
} else {
    Write-Host "⚠️  Wheel not found. Building..." -ForegroundColor Yellow
    & uv build
    pip install "dist\arcflow-0.1.0-py3-none-any.whl"
    Write-Host "✅ arcflow built and installed" -ForegroundColor Green
}

# Step 5: Test installation
Write-Host "`n5️⃣ Testing installation..." -ForegroundColor Yellow
python -c "from arcflow import Controller, SourceConfig, DimensionConfig, ZoneConfig, get_config; print('✅ All imports successful!')"

# Step 6: Show environment info
Write-Host "`n6️⃣ Environment Information:" -ForegroundColor Yellow
Write-Host "  📁 Location: venv-test\" -ForegroundColor Cyan
Write-Host "  🐍 Python:" -ForegroundColor Cyan
python --version
Write-Host "  📦 Installed packages:" -ForegroundColor Cyan
pip list | Select-String "arcflow|pyspark|delta"

Write-Host "`n✨ Test environment ready!" -ForegroundColor Green
Write-Host "`nTo activate:" -ForegroundColor Yellow
Write-Host "  .\venv-test\Scripts\Activate.ps1" -ForegroundColor Cyan
Write-Host "`nTo deactivate:" -ForegroundColor Yellow
Write-Host "  deactivate" -ForegroundColor Cyan
Write-Host "`nTo test:" -ForegroundColor Yellow
Write-Host "  python main.py" -ForegroundColor Cyan
Write-Host "  # or" -ForegroundColor Cyan
Write-Host "  python -c `"from arcflow import Controller; print('Ready!')`"" -ForegroundColor Cyan

# Documentation Setup Script
# Run this script to quickly set up documentation development environment

Write-Host "Setting up tm2py documentation development environment..." -ForegroundColor Green

# Check if conda is available
if (Get-Command conda -ErrorAction SilentlyContinue) {
    Write-Host "[OK] Conda is available" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Conda not found. Please install Anaconda or Miniconda first." -ForegroundColor Red
    exit 1
}

# Check if we're in the tm2py directory
if (-not (Test-Path "mkdocs.yml")) {
    Write-Host "[ERROR] Please run this script from the tm2py repository root directory" -ForegroundColor Red
    Write-Host "  (The directory should contain mkdocs.yml)" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Found mkdocs.yml - we're in the right directory" -ForegroundColor Green

# Create conda environment
Write-Host "Creating conda environment tm2py-docs..." -ForegroundColor Yellow
conda create -n tm2py-docs python=3.11 -y 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Created conda environment tm2py-docs" -ForegroundColor Green
} else {
    Write-Host "[INFO] Environment might already exist, continuing..." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Activate the environment: conda activate tm2py-docs" -ForegroundColor White
Write-Host "2. Install requirements: pip install -r docs/requirements.txt" -ForegroundColor White
Write-Host "3. Start the development server: mkdocs serve" -ForegroundColor White
Write-Host "4. Open your browser to: http://127.0.0.1:8000" -ForegroundColor White
Write-Host ""
Write-Host "To edit documentation:" -ForegroundColor Cyan
Write-Host "- Edit .md files in the docs/ folder" -ForegroundColor White
Write-Host "- The site will auto-reload when you save changes" -ForegroundColor White

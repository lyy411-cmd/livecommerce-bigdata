@echo off
title StarCast - Crawl and Estimate
echo.
echo  =====================================================
echo  Crawl e-commerce rooms + Estimate GMV/Orders
echo  =====================================================
echo.
cd /d "%~dp0"
python -c "import subprocess,sys; r=subprocess.run([sys.executable,'-m','data_pipeline.run_crawl_and_estimate']); sys.exit(r.returncode)"
if errorlevel 1 (
    echo.
    echo  [ERROR] Script exited with error code %errorlevel%
    echo  Check the error messages above for details.
    echo.
)
echo.
pause

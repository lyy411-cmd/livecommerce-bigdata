@echo off
title StarCast - Crawler Test
echo.
echo  =====================================================
echo  Crawler Diagnostic Tool
echo  =====================================================
echo.
echo  Select platform to test:
echo    1. Douyin only
echo    2. Kuaishou only
echo    3. All platforms
echo.
set /p choice=Enter choice (1-3, default=3): 

cd /d "%~dp0"

if "%choice%"=="1" (
    python -c "import subprocess,sys; r=subprocess.run([sys.executable,'-m','data_pipeline.test_crawlers','douyin']); sys.exit(r.returncode)"
) else if "%choice%"=="2" (
    python -c "import subprocess,sys; r=subprocess.run([sys.executable,'-m','data_pipeline.test_crawlers','kuaishou']); sys.exit(r.returncode)"
) else (
    python -c "import subprocess,sys; r=subprocess.run([sys.executable,'-m','data_pipeline.test_crawlers']); sys.exit(r.returncode)"
)
echo.
pause

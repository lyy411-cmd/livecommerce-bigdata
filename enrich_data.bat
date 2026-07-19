@echo off
chcp 65001 >/dev/null 2>/dev/null
title StarCast - Data Enrichment Tool
echo.
echo  ===================================================
echo   StarCast - Data Enrichment Tool
echo   (Clean non-commerce + Fill metrics + Gen orders)
echo  ===================================================
echo.
echo  Note: Make sure VM is running and MySQL is accessible
echo.
cd /d "%~dp0"
python -m data_pipeline.enrich_data
echo.
echo  Done. Press any key to exit...
pause >/dev/null

@echo off
chcp 65001 >nul 2>nul
title StarCast - Seed Commerce Data
echo.
echo  ================================================
echo   Seed e-commerce live room data
echo  ================================================
echo.
cd /d "%~dp0"
python -m data_pipeline.seed_commerce_data
echo.
pause

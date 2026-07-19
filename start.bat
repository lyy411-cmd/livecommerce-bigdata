@echo off
chcp 65001 >nul 2>nul
title XingBo Big Data Platform

echo ============================================================
echo   StarCast Live Commerce Big Data Platform
echo ============================================================
echo.
echo [1/4] Checking Python dependencies...
python -m pip install pymysql requests kafka-python websockets -i https://pypi.tuna.tsinghua.edu.cn/simple --quiet 2>nul
if errorlevel 1 (
    echo   [WARN] pip install had issues, continuing anyway...
)

echo [2/4] Installing Playwright browser (first run only)...
python -m playwright install chromium 2>nul
if errorlevel 1 (
    echo   [WARN] Playwright install skipped (not critical for now)
)

echo [3/4] Checking VM connection...
ping -n 1 -w 3000 192.168.104.100 >nul 2>nul
if errorlevel 1 (
    echo   [WARN] VM 192.168.104.100 unreachable
    echo   Please make sure VM is running with IP 192.168.104.100
    echo   MySQL/Kafka/Flink/Hive will be unavailable
    echo.
    echo   The platform will start with fallback mock data.
) else (
    echo   [OK] VM connected
)

echo.
echo [4/4] Starting platform...
echo.
python run_cluster.py

echo.
echo Platform stopped.
pause

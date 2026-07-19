@echo off
chcp 65001 >/dev/null 2>/dev/null
title StarCast - Login and Start
echo.
echo ========================================================
echo   StarCast Live Commerce Big Data Platform
echo ========================================================
echo.
echo   Step 1: Login to Douyin (for danmaku collection)
echo   Step 2: Start the platform
echo.

echo [Step 1] Opening Douyin login page...
python login_douyin.py
echo.

echo [Step 2] Starting the platform...
python run_cluster.py
pause

@echo off
chcp 65001 >nul 2>nul
title Douyin Anchor Crawler
echo ============================================
echo   Douyin Anchor Batch Discovery
echo ============================================
echo.
cd /d "%~dp0"
python crawl_douyin_anchors.py
echo.
echo Done! Press any key to exit...
pause >nul

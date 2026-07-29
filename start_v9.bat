@echo off
chcp 65001 >/dev/null 2>nul
cd /d "C:\Users\MECHREVO\Desktop\星播大数据分析平台"
C:\Users\MECHREVO\anaconda3\python.exe run_cluster.py > run_cluster_v9.log 2>&1

@echo off
cd /d %~dp0

call .venv\Scripts\activate

py scraper.py

pause

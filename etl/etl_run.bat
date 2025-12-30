@echo off
REM Activate conda env and run ETL (Windows) — adjusted for your conda path
call "C:\conda\condabin\conda.bat" activate weather-etl
cd /d "%~dp0"
python run_etl.py >> etl.log 2>&1
call "C:\conda\condabin\conda.bat" deactivate

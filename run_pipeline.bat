@echo off
cd /d "C:\Nagavarmas\DB_Learning"
call venv\Scripts\activate
python run_pipeline.py >> scheduler_output.log 2>&1
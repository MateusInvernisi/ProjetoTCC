@echo off
cd /d %~dp0

REM 
if not exist venv\Scripts\python.exe (
  echo [*] Criando venv...
  py -3.13 -m venv venv
)
call venv\Scripts\activate

REM 
start "API - Uvicorn" cmd /k uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
start "Dashboard - Streamlit" cmd /k streamlit run dashboard\dashboard_streamlit.py

@echo off
setlocal
REM ==========================================================
REM Inicia a API (Uvicorn) e o Dashboard (Streamlit)
REM Estrutura esperada:
REM   backend\main.py  -> FastAPI (objeto app)
REM   streamlit\app.py -> App Streamlit
REM ==========================================================

REM 1) Ir para a pasta do projeto (onde está este .bat)
cd /d "%~dp0"

REM 2) Criar/ativar o venv se necessário
if not exist "venv\Scripts\python.exe" (
  echo [*] Criando venv...
  py -3 -m venv venv
)
call "venv\Scripts\activate"

REM 3) (Opcional, mas recomendado) instalar deps
REM if exist "requirements.txt" (
REM  echo [*] Instalando dependencias...
REM  python -m pip install --upgrade pip --quiet
REM  python -m pip install -r requirements.txt --quiet
REM )

REM 4) Variáveis de ambiente usadas pelo Streamlit
REM    (se mudar a porta da API, troque aqui também)
set "API_BASE=http://127.0.0.1:8000"
set "PYTHONUTF8=1"

REM 5) Subir a API FastAPI (em janela separada)
start "API - Uvicorn" cmd /k ^
 "python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload"

REM 6) Subir o Streamlit (em outra janela)
start "Dashboard - Streamlit" cmd /k ^
 "streamlit run streamlit\app.py"

endlocal

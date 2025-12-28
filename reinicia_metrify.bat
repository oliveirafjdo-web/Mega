@echo off
REM Finaliza todos os processos python.exe
TASKKILL /F /IM python.exe
REM Aguarda 2 segundos
TIMEOUT /T 2 /NOBREAK > NUL
REM Muda para o diretório do projeto MetriFy ERP
cd /d C:\Users\olliv\Downloads\erp-metrifiy-repo
REM Inicia o sistema MetriFy ERP
python app.py
PAUSE

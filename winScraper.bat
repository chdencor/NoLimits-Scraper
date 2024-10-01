@echo off
REM Cambia al directorio donde se encuentra este script
cd /d %~dp0

REM Activa el entorno virtual
call .venv\Scripts\activate

REM Ejecuta el script de scraping
py scraper.py

REM Pausa para que la ventana se mantenga abierta, puedes eliminar esta línea si no es necesaria
pause

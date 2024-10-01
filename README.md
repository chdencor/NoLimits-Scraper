# CriptoVisualizer Scraper
## _Obtención de datos actuales de criptomonedas en tiempo real_

[ESP]
CriptoVisualizer Scraper es un programa en lotes/crontab que permite cargar los datos desde una API a una base de datos en Supabase para utilizar en una página web con datos analíticos.

[ENG]
CriptoVisualizer Scraper is a batch/crontab program that loads data from an API into a Supabase database for use in a data analytics web page.

---

[ESP]
## Características
- Crear registros en tiempo real
- Asignar la ejecución en segundo plano
- Guarda la data analizada en una base de datos 

[ENG]
## Features
- Create real-time records
- Schedule execution in the background
- Save analyzed data in a database

---

## Tech

- [Python](https://www.python.org)
- [PostgreSQL](https://www.postgresql.org)

---

## Installation

[ESP]
1. En la carpeta del directorio, crea un entorno virtual:
   ```bash
   py -m venv .venv
   ```

2. Instalar los requerimientos
   ```bash
   pip install -r requirements.txt
   ```  
3. Reemplazar los datos de "blueprint_url.env" con los de tu base de datos y renombrar el archivo a "db_url.env"
   ```bash
   DATABASE_URL=postgres://username:password@hostname:port/database
   ```  

[ENG]
1. In the project directory, create a virtual enviroment:
   ```bash
   py -m venv .venv
   ```
2. Install the requirements
   ```bash
   pip install -r requirements.txt
   ``` 
3. Replace the data in "blueprint_url.env" with those of your database and rename the file to "db_url.env"
   ```bash
   DATABASE_URL=postgres://username:password@hostname:port/database
   ```  

---
## RUN
[ESP]

Windows:
Correr _winScraper.bat_

Unix:
Añadir a tu sistema un cromjob
   ```bash
   # Crontab -e
   # * * * * * /.venv/bin/python ./scraper.py
   ```  

[ENG]


Windows:
Run _winScraper.bat_

Unix:
Add a cronjob tu your system
   ```bash
   # Crontab -e
   # * * * * * /.venv/bin/python ./scraper.py
   ```

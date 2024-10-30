from dotenv import load_dotenv
import os
from db.dbORM import dbDefinitions
from utils.dbBroker import runDbBroker

def createScraper():
    load_dotenv('db_url.env')
    database_url = os.getenv('DATABASE_URL')

    if database_url:
        db_instance = dbDefinitions(database_url)
        runDbBroker(db_instance)  # Llama a la función renombrada
    else:
        raise ValueError("DATABASE_URL no encontrada en db_url.env")

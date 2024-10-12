import time
import traceback
from services.CriptoService import CriptoService
from services.RawRegisterService import RegistroService
from db.dbORM import dbORM
from models.APICriptoTicker import APICripto

def insertApiIfNotExists(dbInstance, api_name, api_url):
    """Inserta la API en la base de datos si no existe."""
    try:
        existing_api = dbInstance.fetchOneApiByName(api_name)
        if not existing_api:
            dbInstance.insertApi(api_name, api_url, 'GET', 0, None, 'JSON', None, None)
            print(f"API insertada: {api_name} en {api_url}")
            return dbInstance.fetchOneApiByName(api_name).id 
        return existing_api.id
    except Exception as e:
        print(f"Error al insertar la API: {str(e)}")
        traceback.print_exc()  # Imprimir el traceback del error
        return None
    
def processNewCryptos(dbInstance, cripto, api_id):
    """Procesa nuevas criptomonedas y registra su información."""
    registroService = RegistroService()
    ids_obtenidos = []

    # Obtener y procesar nuevas criptomonedas
    ids_nuevos = registroService.criptoNueva()
    for new_id in ids_nuevos:
        if new_id not in ids_obtenidos:
            item = cripto.getCriptoData(new_id)
            if item:
                ids_obtenidos.append(item['id'])  # Guardar el ID para evitar duplicados
                existingCripto = dbInstance.fetchOneCriptoBySymbol(item['symbol'])

                try:
                    if not existingCripto:
                        msupply = item.get('msupply') or 0
                        dbInstance.insertCriptomoneda(name=item['name'], symbol=item['symbol'], msupply=msupply)
                        existingCripto = dbInstance.fetchOneCriptoBySymbol(item['symbol'])

                    registro_values = (
                        existingCripto.id if existingCripto else None,
                        float(item.get('price_usd') or 0),
                        float(item.get('percent_change_24h') or 0),
                        float(item.get('percent_change_1h') or 0),
                        float(item.get('percent_change_7d') or 0),
                        float(item.get('price_btc') or 0),
                        float(item.get('market_cap_usd') or 0),
                        float(item.get('volume24') or 0),
                        float(item.get('volume24a') or 0),
                        float(item.get('csupply') or 0),
                        float(item.get('tsupply') or 0)
                    )

                    dbInstance.insertRegister(*registro_values)

                    if existingCripto:
                        dbInstance.insertMultipleApiCriptomoneda(api_id, [existingCripto.id])

                except Exception as e:
                    print(f"Error al procesar la criptomoneda {item['name']}: {str(e)}")
                    traceback.print_exc()

def dataLoader(dbInstance):
    """Carga los datos de las criptomonedas."""
    cripto = CriptoService()
    api = APICripto()

    api_id = insertApiIfNotExists(dbInstance, "CoinLore", api.url)
    if api_id is None:
        return

    while True:
        start_time = time.time()  # Inicio del cronómetro
        processNewCryptos(dbInstance, cripto, api_id)
        end_time = time.time()  # Fin del cronómetro
        
        # Calcular el tiempo total de carga
        total_time = end_time - start_time
        print(f"Carga de datos completada en {total_time:.2f} segundos. Esperando a la próxima ejecución...")
        
        time.sleep(40)

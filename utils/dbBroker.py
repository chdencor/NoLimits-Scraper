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
        traceback.print_exc()
        return None

def processCryptos(dbInstance, cripto, api_id):
    """Procesa todas las criptomonedas y registra su información.""" 
    registroService = RegistroService()
    ids_obtenidos = []
    
    # Obtener todos los IDs de criptomonedas existentes
    existing_cryptos = {crypto.symbol: crypto.id for crypto in dbInstance.fetchAllCriptomonedas()}

    ids_todas = registroService.criptoTodas()
    
    # Recopilar datos para la inserción en lotes
    new_cryptos = []
    registros = []
    relaciones = []

    for crypto_id in ids_todas:
        item = cripto.getCriptoData(crypto_id)
        
        if item and item['id'] not in ids_obtenidos:
            ids_obtenidos.append(item['id'])
            symbol = item['symbol']
            existingCriptoId = existing_cryptos.get(symbol)

            try:
                if existingCriptoId is None:
                    msupply = item.get('msupply') or 0
                    new_cryptos.append((item['name'], symbol, msupply))
                    # Se obtiene el id mediante la inserción directa en la base de datos
                    existingCriptoId = dbInstance.insertCriptomoneda(item['name'], symbol, msupply)

                # Valores del registro a insertar
                registro_values = (
                    existingCriptoId,
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

                registros.append(registro_values)
                if existingCriptoId:
                    relaciones.append((api_id, existingCriptoId))

            except Exception as e:
                print(f"Error al procesar la criptomoneda {item['name']}: {str(e)}")
                traceback.print_exc()

    # Inserción masiva de criptomonedas
    if new_cryptos:
        dbInstance.insertMultipleApiCriptomoneda(api_id, [crypto[1] for crypto in new_cryptos])

    # Inserción masiva de registros
    if registros:
        dbInstance.insertMultipleRegisters(registros)

    # Inserción de relaciones
    if relaciones:
        dbInstance.insertMultipleApiCriptomoneda(api_id, [rel[1] for rel in relaciones])

def dataLoader(dbInstance):
    """Carga los datos de las criptomonedas.""" 
    cripto = CriptoService()
    api = APICripto()

    api_id = insertApiIfNotExists(dbInstance, "CoinLore", api.url)
    if api_id is None:
        return

    while True:
        start_time = time.time()
        processCryptos(dbInstance, cripto, api_id)
        end_time = time.time()
        
        total_time = end_time - start_time
        print(f"Carga de datos completada en {total_time:.2f} segundos. Esperando a la próxima ejecución...")
        
        time.sleep(40)

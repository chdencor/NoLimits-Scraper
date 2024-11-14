import datetime
import time
import traceback
from services.CriptoService import CriptoService
import db.dbORM
from models.APICriptoTicker import APICripto

def handle_insertion_result(result):
    if result and len(result) > 0:
        print(f"Resultado de la inserción: {result[0]} - {result[1]}")  # Ajusta esto según la estructura
    else:
        print("Error: No se pudo obtener el resultado para la inserción.")

def execute_with_error_handling(session, query, params):
    try:
        print(f"Ejecutando consulta: {query} con parámetros: {params}")
        return session.execute(query, params).fetchone()
    except Exception as e:
        print(f"Error ejecutando la consulta: {str(e)}")
        traceback.print_exc()
        return None

def insertApiIfNotExists(dbInstance, api_name, api_url):
    try:
        print(f"Verificando existencia de la API: {api_name}")
        existing_api = dbInstance.fetchOneApiByName(api_name)
        if not existing_api:
            print(f"La API {api_name} no existe, insertando...")
            ahora = datetime.datetime.now()
            api_data = {
                'nombre': api_name,
                'url': api_url,
                'metodo': 'GET',
                'cantidad_parametros': 0,
                'limite_uso': 0,
                'tiempo_respuesta_promedio_ms': 0,
                'ultima_actualizacion': ahora,
                'formato_respuesta': 'JSON',
                'estado': 'activo',
                'tipo_autenticacion': None
            }
            result = dbInstance.insertApi([api_data])
            handle_insertion_result(result)
            print(f"API insertada con éxito, obteniendo el ID {api_name}...")
            return dbInstance.fetchOneApiByName(api_name).id 
        print(f"La API {api_name} ya existe con ID: {existing_api.id}")
        return existing_api.id
    except Exception as e:
        print(f"Error al insertar la API: {str(e)}")
        traceback.print_exc()

def processCryptos(dbInstance, cripto, api_id):
    ids_obtenidos = []
    print("Obteniendo todas las criptomonedas registradas...")
    existing_cryptos = {crypto['symbol']: crypto['id'] for crypto in dbInstance.fetchAllCriptomonedas()}
    ids_todas = cripto.all_ids  # Obtiene todas las IDs de las criptomonedas disponibles
    
    registros = []

    try:
        for crypto_id in ids_todas:
            print(f"\n--- Procesando criptomoneda con ID: {crypto_id} ---")
            item = cripto.getCriptoData(crypto_id)
            
            if item and item['id'] not in ids_obtenidos:
                print(f"Datos obtenidos de la criptomoneda: {item}")
                ids_obtenidos.append(item['id'])
                symbol = item['symbol']
                existingCriptoId = existing_cryptos.get(symbol)

                if existingCriptoId is None:
                    print(f"No se encontró la criptomoneda {symbol} en la base de datos, intentando agregarla...")
                    msupply = item.get('msupply') or 0
                    new_crypto = {
                        'id': item['id'],
                        'name': item['name'],
                        'symbol': symbol,
                        'msupply': msupply
                    }
                    print(f"Intentando insertar nueva criptomoneda: {new_crypto}")
                    
                    # Inserta la criptomoneda en la base de datos
                    result = dbInstance.insertCripto([new_crypto])
                    handle_insertion_result(result)

                    # Recupera el ID de la criptomoneda recién insertada
                    existingCripto = dbInstance.fetchOneCriptoBySymbol(symbol)
                    existingCriptoId = existingCripto['id'] if existingCripto else None

                    if existingCriptoId is None:
                        print(f"Error: No se pudo obtener un ID para la criptomoneda {symbol} después de insertarla.")
                        continue
                    print(f"Criptomoneda {symbol} insertada exitosamente con ID {existingCriptoId}")

                    # Inserta en la tabla de relación api_criptomoneda
                    print(f"Insertando en api_criptomoneda la relación: api_id={api_id}, criptomoneda_id={existingCriptoId}")
                    dbInstance.insertApiCriptomoneda([{'api_id': api_id, 'criptomoneda_id': existingCriptoId}])

                else:
                    print(f"La criptomoneda {symbol} ya existe con ID {existingCriptoId}.")

                registro_values = {
                    'criptomoneda_id': existingCriptoId,
                    'price_usd': float(item.get('price_usd') or 0),
                    'percent_change_24h': float(item.get('percent_change_24h') or 0),
                    'percent_change_1h': float(item.get('percent_change_1h') or 0),
                    'percent_change_7d': float(item.get('percent_change_7d') or 0),
                    'price_btc': float(item.get('price_btc') or 0),
                    'market_cap_usd': float(item.get('market_cap_usd') or 0),
                    'volume24': float(item.get('volume24') or 0),
                    'volume24a': float(item.get('volume24a') or 0),
                    'csupply': float(item.get('csupply') or 0),
                    'tsupply': float(item.get('tsupply') or 0),
                    'rank': int(item.get('rank') or 0)
                }
                print(f"Datos para insertar en el registro: {registro_values}")

                registros.append(registro_values)

    except Exception as e:
        print(f"Error al procesar criptomonedas: {str(e)}")
        traceback.print_exc()

    finally:
        if registros:
            print("Iniciando inserción de registros de criptomonedas...")
            result = dbInstance.insertRegistro(registros)
            handle_insertion_result(result)


def dataLoader(dbInstance):
    cripto = CriptoService()
    api = APICripto()

    api_id = insertApiIfNotExists(dbInstance, "CoinLore", api.url)
    if api_id is None:
        print("Error: No se pudo obtener el ID de la API. Terminando ejecución.")
        return

    while True:
        start_time = time.time()
        print("Iniciando proceso de carga de criptomonedas...")
        processCryptos(dbInstance, cripto, api_id)
        end_time = time.time()
        
        total_time = end_time - start_time
        print(f"Carga de datos completada en {total_time:.2f} segundos. Esperando a la próxima ejecución...")
        
        time.sleep(40)

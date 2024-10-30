import asyncio
import hashlib
import datetime
import time
import traceback
from services.CriptoService import CriptoService
from services.RawRegisterService import RegistroService
from db.dbORM import dbDefinitions
from models.APICriptoTicker import APICripto

def generateCryptoHash(crypto_id, symbol):
    hash_input = f"{crypto_id}:{symbol}".encode('utf-8')
    return hashlib.sha256(hash_input).hexdigest()

async def insertApiIfNotExists(dbInstance, api_name, api_url):
    try:
        existing_api = await dbInstance.fetchOneApiByName(api_name)
        if existing_api is None:
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
            await dbInstance.insertApi([api_data])
            # Volver a buscar la API ya que ahora debería existir
            existing_api = await dbInstance.fetchOneApiByName(api_name)
        
        # Si ya existe, devolver su id
        return existing_api.id if existing_api else None
    except Exception as e:
        print(f"Error al insertar la API: {str(e)}")
        traceback.print_exc()


async def insertCryptos(dbInstance, new_criptomonedas):
    if new_criptomonedas:
        print(f"Inserting new cryptocurrencies: {new_criptomonedas}")
        await dbInstance.insertCripto(new_criptomonedas)

async def insertApiCryptos(dbInstance, new_api_criptomonedas):
    if new_api_criptomonedas:
        print(f"Inserting new API cryptocurrencies: {new_api_criptomonedas}")
        await dbInstance.insertApiCriptomoneda(new_api_criptomonedas)

async def insertRegistros(dbInstance, registros):
    if registros:
        print(f"Inserting new records: {registros}")
        await dbInstance.insertRegistro(registros)

async def processCryptos(dbInstance, cripto, api_id):
    registroService = RegistroService()
    ids_obtenidos = set()
    existing_cryptos = {crypto['symbol']: crypto['id'] for crypto in await dbInstance.fetchAllCriptomonedas()}
    ids_todas = registroService.criptoTodas()  # Call without await

    new_api_criptomonedas = []
    new_criptomonedas = []
    registros = []
    processed_hashes = set()

    try:
        for crypto_id in ids_todas:
            print(f"Fetching data for crypto_id: {crypto_id}")  # Debugging
            item = cripto.getCriptoData(crypto_id)  # Consider not using await
            if item is None:
                continue  # Skip if item is None

            if item['id'] not in ids_obtenidos:
                ids_obtenidos.add(item['id'])
                symbol = item['symbol']
                existingCriptoId = existing_cryptos.get(symbol)

                # Generate a hash for the cryptocurrency
                crypto_hash = generateCryptoHash(item['id'], symbol)

                if crypto_hash in processed_hashes:
                    continue

                processed_hashes.add(crypto_hash)

                # Handle new cryptocurrency insertions
                if existingCriptoId is None:
                    msupply = item.get('msupply') or 0
                    new_cripto = {
                        'id': item['id'],
                        'name': item['name'],
                        'symbol': symbol,
                        'msupply': msupply
                    }
                    new_criptomonedas.append(new_cripto)

                if existingCriptoId is not None:
                    new_api_crypto = {
                        'api_id': api_id,
                        'criptomoneda_id': existingCriptoId
                    }
                    new_api_criptomonedas.append(new_api_crypto)

                # Add a new record with the cryptocurrency data
                if existingCriptoId is not None:
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
                    if all(value is not None for value in registro_values.values()):
                        registros.append(registro_values)

    except Exception as e:
        print(f"Error al procesar criptomonedas: {str(e)}")
        traceback.print_exc()

    # Insert in parallel
    await asyncio.gather(
        insertCryptos(dbInstance, new_criptomonedas),
        insertApiCryptos(dbInstance, new_api_criptomonedas),
        insertRegistros(dbInstance, registros)
    )

async def dataLoader(dbInstance):
    cripto = CriptoService()
    api = APICripto()

    # Llamar a la función insertApiIfNotExists para insertar la API
    api_id = await insertApiIfNotExists(dbInstance, "CoinLore", api.url)
    if api_id is None:
        return

    while True:
        start_time = time.time()
        await processCryptos(dbInstance, cripto, api_id)
        end_time = time.time()

        total_time = end_time - start_time
        print(f"Carga de datos completada en {total_time:.2f} segundos. Esperando a la próxima ejecución...")

        await asyncio.sleep(40)

# Método para iniciar el bucle de eventos
def runDbBroker(dbInstance):
    asyncio.run(dataLoader(dbInstance))

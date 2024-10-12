import time
from services.CriptoService import CriptoService
from services.RawRegisterService import RegistroService
from db.dbORM import dbORM
from models.APICriptoTicker import APICripto

def dataLoader(dbInstance):
    cripto = CriptoService()
    registroService = RegistroService()

    # Crea una instancia de la API
    api = APICripto()

    # Insertar la información de la API antes de cualquier otra inserción
    try:
        api_name = "CoinLore"
        api_url = api.url

        # Verificar si la API ya existe y evitar duplicados
        existing_api = dbInstance.fetchOneApiByName(api_name)
        if not existing_api:
            dbInstance.insert_api_batch([{'name': api_name, 'url': api_url, 'method': 'GET', 'auth_required': 0, 
                                           'rate_limit': None, 'response_format': 'JSON', 'headers': None, 'params': None}])
            print(f"API insertada: {api_name} en {api_url}")

        # Obtener el ID de la API recién insertada
        api_id = dbInstance.fetchOneApiByName(api_name).id

    except Exception as e:
        print(f"Error al insertar la API: {str(e)}")
        return  # Salir de la función si hay un error en la API

    while True:
        data = cripto.parsed_data
        idsObtenidos = []
        criptomonedas_batch = []
        registros_batch = []
        api_criptomonedas_batch = []
        api_registros_batch = []

        # Obtener y procesar nuevas criptomonedas
        idsNuevos = registroService.criptoNueva()
        for new_id in idsNuevos:
            if new_id not in idsObtenidos:
                item = cripto.getCriptoData(new_id)
                if item:
                    data.append(item)

        for item in data:
            idsObtenidos.append(item['id'])

            existingCripto = dbInstance.fetchOneCriptoBySymbol(item['symbol'])
            try:
                if not existingCripto:
                    # Preparar los datos para la inserción por lotes de criptomonedas
                    msupply = item.get('msupply') or 0
                    criptomonedas_batch.append({
                        'name': item['name'],
                        'symbol': item['symbol'],
                        'msupply': msupply
                    })

            except Exception as e:
                print(f"Error al crear la criptomoneda: {item['name']} - {str(e)}")
                continue

            registroValues = {
                'criptomoneda_id': existingCripto.id if existingCripto else None,
                'price_usd': float(item.get('price_usd') or 0),
                'percent_change_24h': float(item.get('percent_change_24h') or 0),
                'percent_change_1h': float(item.get('percent_change_1h') or 0),
                'percent_change_7d': float(item.get('percent_change_7d') or 0),
                'price_btc': float(item.get('price_btc') or 0),
                'market_cap_usd': float(item.get('market_cap_usd') or 0),
                'volume24': float(item.get('volume24') or 0),
                'volume24a': float(item.get('volume24a') or 0),
                'csupply': float(item.get('csupply') or 0),
                'tsupply': float(item.get('tsupply') or 0)
            }

            registros_batch.append(registroValues)

            if existingCripto:
                api_criptomonedas_batch.append({'api_id': api_id, 'criptomoneda_id': existingCripto.id})

        try:
            # Hacer inserciones por lotes
            if criptomonedas_batch:
                dbInstance.insert_criptomoneda_batch(criptomonedas_batch)
                print(f"Criptomonedas insertadas: {len(criptomonedas_batch)}")

            if registros_batch:
                dbInstance.insert_register_batch(registros_batch)
                print(f"Registros insertados: {len(registros_batch)}")

            if api_criptomonedas_batch:
                dbInstance.insert_api_criptomoneda_batch(api_criptomonedas_batch)
                print(f"Relaciones API-Criptomoneda insertadas: {len(api_criptomonedas_batch)}")

        except Exception as e:
            print(f"Error durante la inserción por lotes: {e}")

        # Actualizar la información del cripto
        try:
            registroService.actualizarCripto(idsObtenidos)
        except Exception as e:
            print(f"Error en la actualización de criptomonedas: {e}")

        # Esperar antes de la siguiente iteración
        time.sleep(61)

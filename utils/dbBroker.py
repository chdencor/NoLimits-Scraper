import time
from services.CriptoService import CriptoService
from services.RawRegisterService import RegistroService
from db.dbORM import dbORM
from models.APICriptoTicker import APICripto

def insert_api_if_not_exists(dbInstance, api_name, api_url):
    """Inserta la API en la base de datos si no existe."""
    try:
        existing_api = dbInstance.fetchOneApiByName(api_name)
        if not existing_api:
            dbInstance.insert_api(api_name, api_url, 'GET', 0, None, 'JSON', None, None)
            print(f"API insertada: {api_name} en {api_url}")
            return dbInstance.fetchOneApiByName(api_name).id 
        return existing_api.id
    except Exception as e:
        print(f"Error al insertar la API: {str(e)}")
        return None

def process_new_cryptos(dbInstance, cripto, api_id):
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

                if not existingCripto:
                    # Insertar la nueva criptomoneda
                    msupply = item.get('msupply') or 0
                    dbInstance.insert_criptomoneda({
                        'name': item['name'],
                        'symbol': item['symbol'],
                        'msupply': msupply
                    })
                    # Obtener el ID de la criptomoneda recién insertada
                    existingCripto = dbInstance.fetchOneCriptoBySymbol(item['symbol'])

                # Extraer valores para el registro
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

                
                dbInstance.insert_register(*registro_values)

                
                dbInstance.insert_multiple_api_criptomoneda([{
                    'api_id': api_id,
                    'criptomoneda_id': existingCripto.id if existingCripto else None
                }])

def dataLoader(dbInstance):
    """Carga los datos de las criptomonedas."""
    cripto = CriptoService()
    api = APICripto()

    
    api_id = insert_api_if_not_exists(dbInstance, "CoinLore", api.url)
    if api_id is None:
        return

    while True:
        process_new_cryptos(dbInstance, cripto, api_id)
        print("Carga de datos completada. Esperando a la próxima ejecución...")
        time.sleep(40)

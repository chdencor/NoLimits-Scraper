import time
from services.CriptoService import CriptoService
from services.RawRegisterService import RegistroService
from db.dbORM import dbORM
from models.SQLAlchemyTables import Criptomoneda, Registro

def dataLoader(dbInstance):
    cripto = CriptoService()
    registroService = RegistroService()

    while True:
        # Obtener datos de la API
        data = cripto.parseResponse()
        idsObtenidos = []
        successful_inserts = 0
        failed_inserts = 0
        registros_a_insertar = []  # Lista para inserciones masivas

        # Comprobar y agregar nuevas criptomonedas desde el archivo JSON
        idsNuevos = registroService.criptoNueva()
        for new_id in idsNuevos:
            if new_id not in idsObtenidos:
                # Supongamos que tienes una función que obtiene la información de la cripto por ID
                item = cripto.getCriptoData(new_id)
                if item:
                    data.append(item)
        
        for item in data:
            idsObtenidos.append(item['id'])

            # Verificar si la criptomoneda ya existe
            existingCripto = dbInstance.fetchOneCriptoBySymbol(item['symbol'])
            try:
                if existingCripto:
                    criptoId = existingCripto.id
                else:
                    # Verificar si el msupply está vacío y asignar 0 si es necesario
                    msupply = item.get('msupply')
                    msupply = msupply if msupply not in [None, ""] else 0

                    # Intento de creación de una nueva criptomoneda con el msupply
                    criptoId = dbInstance.createCripto(item['name'], item['symbol'], msupply)
                    print(f"Criptomoneda creada: {item['name']} (ID: {criptoId})")

            except Exception as e:
                print(f"Error al crear la criptomoneda: {item['name']} - {str(e)}")
                failed_inserts += 1
                continue  # Si no se puede crear la criptomoneda, saltar a la siguiente

            # Limpieza de los valores para evitar errores de inserción
            registroValues = {
                'criptomoneda_id': criptoId,  # Asociación directa con la criptomoneda
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

            try:
                # Crea un nuevo registro
                nuevo_registro = Registro(**registroValues)
                registros_a_insertar.append(nuevo_registro)

            except Exception as e:
                failed_inserts += 1
                print(f"Error al procesar registro para criptomoneda ID {criptoId}: {e}")

        try:
            # Inserción masiva de registros
            if registros_a_insertar:
                dbInstance.session.bulk_save_objects(registros_a_insertar)
                dbInstance.session.commit()  # Commit por todas las inserciones
                successful_inserts += len(registros_a_insertar)
                print(f"Se insertaron {successful_inserts} registros exitosamente.")
            else:
                print("No hay registros nuevos para insertar.")

            # Actualizar la información del cripto
            registroService.actualizarCripto(idsObtenidos)

        except Exception as e:
            print(f"Error en la inserción masiva: {e}")
            dbInstance.session.rollback()
            failed_inserts += len(registros_a_insertar)  # Contar los fallos de inserción

        # Resultados finales de la iteración
        print(f"Iteración finalizada: {successful_inserts} registros insertados, {failed_inserts} errores.")
        print("------------------------------------------------------------------")

        # Esperar antes de la siguiente iteración
        time.sleep(60)

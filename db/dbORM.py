from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session

class dbORM:
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    def close(self):
        """Cerrar la sesión."""
        self.Session.remove()  # Close session properly

    def __enter__(self):
        """Context manager enter method."""
        self.current_session = self.Session()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit method."""
        self.close()

    def insertRelationship(self, insert_function, **kwargs):
        """Método genérico para insertar relaciones en la base de datos."""
        try:
            self.current_session.execute(text(f"SELECT {insert_function}({', '.join(f':{k}' for k in kwargs.keys())});"), kwargs)
            self.current_session.commit()
        except Exception as e:
            self.current_session.rollback()
            print(f"Error al insertar en {insert_function}: {e}")

    def insertMultipleCryptocurrencies(self, cryptocurrencies):
        """Insertar múltiples criptomonedas, evitando duplicados."""
        existing_symbols = {row[0] for row in self.current_session.execute(text("SELECT symbol FROM criptomonedas;")).fetchall()}
        new_cryptos = [(name, symbol, msupply) for name, symbol, msupply in cryptocurrencies if symbol not in existing_symbols]

        for name, symbol, msupply in new_cryptos:
            self.insertCriptomoneda(name, symbol, msupply)

    def insertMultipleRegisters(self, registers):
     """Insertar múltiples registros en la tabla 'registros'."""
     for register in registers:
         try:
             cripto_id, price_usd, percent_change_24h, percent_change_1h, percent_change_7d, price_btc, market_cap_usd, volume24, volume24a, csupply, tsupply, ranking = register  # Asegúrate de incluir ranking aquí
             self.insertRegister(cripto_id, price_usd, percent_change_24h, percent_change_1h,
                                 percent_change_7d, price_btc, market_cap_usd, volume24, volume24a, csupply, tsupply, ranking)
         except ValueError as e:
             print(f"Error unpacking register: {register}. Error: {e}")


    def insertApi(self, nombre, url, metodo, cantidad_parametros, tipo_autenticacion, 
                  formato_respuesta, limite_uso, tiempo_respuesta_promedio_ms, estado='activo'):
        """Insertar un nuevo registro en la tabla 'api'."""
        self.insertRelationship('insert_api', nombre=nombre, url=url, metodo=metodo, 
                                cantidad_parametros=cantidad_parametros, 
                                tipo_autenticacion=tipo_autenticacion, 
                                formato_respuesta=formato_respuesta, 
                                limite_uso=limite_uso, 
                                tiempo_respuesta_promedio_ms=tiempo_respuesta_promedio_ms, 
                                estado=estado)

    def insertRegister(self, criptomoneda_id, price_usd, percent_change_24h, percent_change_1h,
                   percent_change_7d, price_btc, market_cap_usd, volume24, volume24a, 
                   csupply, tsupply, ranking):
        """Insertar un nuevo registro en la tabla 'registros' usando la función de PostgreSQL."""
        self.insertRelationship('insert_register', 
                                criptomoneda_id=criptomoneda_id, 
                                price_usd=price_usd, 
                                percent_change_24h=percent_change_24h, 
                                percent_change_1h=percent_change_1h, 
                                percent_change_7d=percent_change_7d, 
                                price_btc=price_btc, 
                                market_cap_usd=market_cap_usd, 
                                volume24=volume24, 
                                volume24a=volume24a, 
                                csupply=csupply, 
                                tsupply=tsupply,
                                ranking=ranking)

    def insertMultipleApiCriptomoneda(self, api_id, criptomoneda_ids):
        """Insertar múltiples relaciones entre API y criptomonedas en la tabla 'api_criptomonedas'."""
        self.insertRelationship('insert_multiple_api_criptomonedas', api_id=api_id, criptomoneda_ids=criptomoneda_ids)

    def insertMultipleApiMercado(self, api_id, mercado_ids):
        """Insertar múltiples relaciones entre API y mercados en la tabla 'api_mercados'."""
        self.insertRelationship('insert_multiple_api_mercados', api_id=api_id, mercado_ids=mercado_ids)

    def insertMultipleApiRegistro(self, api_id, registro_ids):
        """Insertar múltiples relaciones entre API y registros en la tabla 'api_registros'."""
        self.insertRelationship('insert_multiple_api_registros', api_id=api_id, registro_ids=registro_ids)

    def insertMultipleApiMetricaGeneral(self, api_id, metrica_ids):
        """Insertar múltiples relaciones entre API y métricas generales en la tabla 'api_metricas_generales'."""
        self.insertRelationship('insert_multiple_api_metricas_generales', api_id=api_id, metrica_ids=metrica_ids)

    def insertMultipleApiExchange(self, api_id, exchange_ids):
        """Insertar múltiples relaciones entre API y exchanges en la tabla 'api_exchanges'."""
        self.insertRelationship('insert_multiple_api_exchanges', api_id=api_id, exchange_ids=exchange_ids)

    def insertExchange(self, name, name_id, volume_usd, active_pairs, country, url):
        """Insertar un nuevo registro en la tabla 'exchanges'."""
        self.insertRelationship('insert_exchange', name=name, name_id=name_id, 
                                volume_usd=volume_usd, active_pairs=active_pairs, 
                                country=country, url=url)

    def insertMercadoActivo(self, market_name, base_symbol, quote_symbol, price_usd, volume_usd, exchange_id):
        """Insertar un nuevo registro en la tabla 'mercados_activos'."""
        self.insertRelationship('insert_mercado_activo', market_name=market_name, 
                                base_symbol=base_symbol, quote_symbol=quote_symbol, 
                                price_usd=price_usd, volume_usd=volume_usd, 
                                exchange_id=exchange_id)

    def insertMetricaGeneral(self, coins_count, active_markets, total_mcap, total_volume, 
                             btc_d, eth_d, mcap_change, volume_change, 
                             avg_change_percent, volume_ath, mcap_ath):
        """Insertar un nuevo registro en la tabla 'metrica_general'."""
        self.insertRelationship('insert_metrica_general', coins_count=coins_count, 
                                active_markets=active_markets, total_mcap=total_mcap, 
                                total_volume=total_volume, btc_d=btc_d, eth_d=eth_d, 
                                mcap_change=mcap_change, volume_change=volume_change, 
                                avg_change_percent=avg_change_percent, 
                                volume_ath=volume_ath, mcap_ath=mcap_ath)

    def insertCriptomoneda(self, name, symbol, msupply):
        """Insertar un nuevo registro en la tabla 'criptomonedas'."""
        self.insertRelationship('insert_criptomoneda', name=name, symbol=symbol, msupply=msupply)

    def fetchAllCriptomonedas(self):
        """Recupera todas las criptomonedas de la base de datos."""
        try:
            # Eliminar la columna 'ranking' de la consulta
            result = self.current_session.execute(text("SELECT id, name, symbol, msupply FROM criptomonedas"))
            rows = result.fetchall()
            # Convierte a una lista de diccionarios, incluyendo todas las columnas
            return [{'id': row[0], 'name': row[1], 'symbol': row[2], 'msupply': row[3]} for row in rows]
        except Exception as e:
            print(f"Error al obtener criptomonedas: {str(e)}")
            return []



    def fetchOneCriptoBySymbol(self, symbol):
        """Obtener una criptomoneda por símbolo."""
        result = self.current_session.execute(text(""" 
            SELECT * FROM get_criptomoneda_by_symbol(:symbol);
        """), {'symbol': symbol}).fetchone()

        return result if result else None
    
    def fetchOneApiByName(self, nombre):
        """Obtener una API por nombre."""
        result = self.current_session.execute(text(""" 
            SELECT * FROM fetchOneApiByName(:nombre);
        """), {'nombre': nombre}).fetchone()
        return result if result else None
        
    def fetchOneRegisterId(self, criptoId):
        """Obtener el ID de un registro por ID de criptomoneda."""
        query = text("SELECT id FROM registros WHERE criptomoneda_id = :criptoId")
        result = self.current_session.execute(query, {'criptoId': criptoId}).fetchone()
        return result.id if result else None

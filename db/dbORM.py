from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

class dbORM:
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def close(self):
        """Cerrar la sesión."""
        self.session.close()

    def insertRelationship(self, insert_function, **kwargs):
        """Método genérico para insertar relaciones en la base de datos."""
        try:
            self.session.execute(text(f"SELECT {insert_function}({', '.join(f':{k}' for k in kwargs.keys())});"), kwargs)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print(f"Error al insertar en {insert_function}: {e}")

    def insertMultipleCryptocurrencies(self, cryptocurrencies):
        """Insertar múltiples criptomonedas, evitando duplicados."""
        existing_symbols = {row[0] for row in self.session.execute(text("SELECT symbol FROM criptomonedas;")).fetchall()}
        new_cryptos = [(name, symbol, msupply) for name, symbol, msupply in cryptocurrencies if symbol not in existing_symbols]

        for name, symbol, msupply in new_cryptos:
            self.insertCriptomoneda(name, symbol, msupply)

    def insertMultipleRegisters(self, registers):
        """Insertar múltiples registros en la tabla 'registros'."""
        for cripto_id, price_usd, percent_change_24h, percent_change_1h, percent_change_7d, price_btc, market_cap_usd, volume24, volume24a, csupply, tsupply in registers:
            self.insertRegister(cripto_id, price_usd, percent_change_24h, percent_change_1h,
                                percent_change_7d, price_btc, market_cap_usd, volume24, volume24a, csupply, tsupply)

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
                       percent_change_7d, price_btc, market_cap_usd, volume24, volume24a, csupply, tsupply):
        """Insertar un nuevo registro en la tabla 'registros' usando la función de PostgreSQL."""
        self.insertRelationship('insert_register', criptomoneda_id=criptomoneda_id, 
                                price_usd=price_usd, percent_change_24h=percent_change_24h, 
                                percent_change_1h=percent_change_1h, 
                                percent_change_7d=percent_change_7d, price_btc=price_btc, 
                                market_cap_usd=market_cap_usd, volume24=volume24, 
                                volume24a=volume24a, csupply=csupply, tsupply=tsupply)

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
            result = self.session.execute(text("SELECT id, symbol FROM criptomonedas"))
            rows = result.fetchall()
            return rows  # Devuelve filas sin mapear a Cripto
        except Exception as e:
            print(f"Error al obtener criptomonedas: {str(e)}")
            return []

    def fetchOneCriptoBySymbol(self, symbol):
        """Obtener una criptomoneda por símbolo."""
        result = self.session.execute(text(""" 
            SELECT * FROM get_criptomoneda_by_symbol(:symbol);
        """), {'symbol': symbol}).fetchone()

        return result if result else None
    
    def fetchOneApiByName(self, nombre):
        """Obtener una API por nombre."""
        result = self.session.execute(text(""" 
            SELECT * FROM fetchOneApiByName(:nombre);
        """), {'nombre': nombre}).fetchone()
        return result if result else None
        
    def fetchOneRegisterId(self, criptoId):
        """Obtener el ID de un registro por ID de criptomoneda."""
        query = text("SELECT id FROM registros WHERE criptomoneda_id = :criptoId")
        result = self.session.execute(query, {'criptoId': criptoId}).fetchone()
        return result.id if result else None

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

    def insert_relationship(self, insert_function, **kwargs):
        """Método genérico para insertar relaciones en la base de datos."""
        try:
            self.session.execute(text(f"SELECT {insert_function}({', '.join(f':{k}' for k in kwargs.keys())});"), kwargs)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print(f"Error al insertar en {insert_function}: {e}")

    def insert_api(self, nombre, url, metodo, cantidad_parametros, tipo_autenticacion, 
                   formato_respuesta, limite_uso, tiempo_respuesta_promedio_ms, estado='activo'):
        """Insertar un nuevo registro en la tabla 'api'."""
        self.insert_relationship('insert_api', nombre=nombre, url=url, metodo=metodo, 
                                 cantidad_parametros=cantidad_parametros, 
                                 tipo_autenticacion=tipo_autenticacion, 
                                 formato_respuesta=formato_respuesta, 
                                 limite_uso=limite_uso, 
                                 tiempo_respuesta_promedio_ms=tiempo_respuesta_promedio_ms, 
                                 estado=estado)

    def insert_register(self, criptomoneda_id, price_usd, percent_change_24h, percent_change_1h,
                        percent_change_7d, price_btc, market_cap_usd, volume24, volume24a, csupply, tsupply):
        """Insertar un nuevo registro en la tabla 'registros' usando la función de PostgreSQL."""
        self.insert_relationship('insert_register', criptomoneda_id=criptomoneda_id, 
                                 price_usd=price_usd, percent_change_24h=percent_change_24h, 
                                 percent_change_1h=percent_change_1h, 
                                 percent_change_7d=percent_change_7d, price_btc=price_btc, 
                                 market_cap_usd=market_cap_usd, volume24=volume24, 
                                 volume24a=volume24a, csupply=csupply, tsupply=tsupply)

    def insert_multiple_api_criptomoneda(self, api_ids, criptomoneda_ids):
        """Insertar múltiples relaciones entre API y criptomonedas en la tabla 'api_criptomonedas'."""
        print(f"Inserting multiple API - Criptomonedas")
        self.insert_relationship('insert_multiple_api_criptomonedas', api_ids=api_ids, criptomoneda_ids=criptomoneda_ids)

    def insert_multiple_api_mercado(self, api_ids, mercado_ids):
        """Insertar múltiples relaciones entre API y mercados en la tabla 'api_mercados'."""
        self.insert_relationship('insert_multiple_api_mercados', api_ids=api_ids, mercado_ids=mercado_ids)

    def insert_multiple_api_registro(self, api_ids, registro_ids):
        """Insertar múltiples relaciones entre API y registros en la tabla 'api_registros'."""
        self.insert_relationship('insert_multiple_api_registros', api_ids=api_ids, registro_ids=registro_ids)

    def insert_multiple_api_metrica_general(self, api_ids, metrica_ids):
        """Insertar múltiples relaciones entre API y métricas generales en la tabla 'api_metricas_generales'."""
        self.insert_relationship('insert_multiple_api_metricas_generales', api_ids=api_ids, metrica_ids=metrica_ids)

    def insert_multiple_api_exchange(self, api_ids, exchange_ids):
        """Insertar múltiples relaciones entre API y exchanges en la tabla 'api_exchanges'."""
        self.insert_relationship('insert_multiple_api_exchanges', api_ids=api_ids, exchange_ids=exchange_ids)

    def insert_exchange(self, name, name_id, volume_usd, active_pairs, country, url):
        """Insertar un nuevo registro en la tabla 'exchanges'."""
        self.insert_relationship('insert_exchange', name=name, name_id=name_id, 
                                 volume_usd=volume_usd, active_pairs=active_pairs, 
                                 country=country, url=url)

    def insert_mercado_activo(self, market_name, base_symbol, quote_symbol, price_usd, volume_usd, exchange_id):
        """Insertar un nuevo registro en la tabla 'mercados_activos'."""
        self.insert_relationship('insert_mercado_activo', market_name=market_name, 
                                 base_symbol=base_symbol, quote_symbol=quote_symbol, 
                                 price_usd=price_usd, volume_usd=volume_usd, 
                                 exchange_id=exchange_id)

    def insert_metrica_general(self, coins_count, active_markets, total_mcap, total_volume, 
                                btc_d, eth_d, mcap_change, volume_change, 
                                avg_change_percent, volume_ath, mcap_ath):
        """Insertar un nuevo registro en la tabla 'metrica_general'."""
        self.insert_relationship('insert_metrica_general', coins_count=coins_count, 
                                 active_markets=active_markets, total_mcap=total_mcap, 
                                 total_volume=total_volume, btc_d=btc_d, eth_d=eth_d, 
                                 mcap_change=mcap_change, volume_change=volume_change, 
                                 avg_change_percent=avg_change_percent, 
                                 volume_ath=volume_ath, mcap_ath=mcap_ath)

    def insert_criptomoneda(self, name, symbol, msupply):
        """Insertar un nuevo registro en la tabla 'criptomonedas'."""
        self.insert_relationship('insert_criptomoneda', name=name, symbol=symbol, msupply=msupply)

    def fetchOneCriptoBySymbol(self, symbol):
        """Obtener una criptomoneda por símbolo."""
        result = self.session.execute(text(""" 
            SELECT * FROM get_criptomoneda_by_symbol(:symbol);
        """), {'symbol': symbol}).fetchone()

        return result if result else None  # Retornar el resultado o None si no existe
    
    def fetchOneApiByName(self, nombre):
        """Obtener una API por nombre."""
        result = self.session.execute(text(""" 
            SELECT * FROM fetchOneApiByName(:nombre);
        """), {'nombre': nombre}).fetchone()
        return result if result else None
        
    def fetchOneRegisterId(self, criptoId):
        query = text("SELECT id FROM registros WHERE criptomoneda_id = :criptoId")
        result = self.session.execute(query, {'criptoId': criptoId}).fetchone()
        return result.id if result else None  # Retorna solo el ID, o None si no hay resultado

from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

class dbDefinitions:
    def __init__(self, db_url):
        """Inicializa la conexión a la base de datos."""
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

    def insert_data(self, table_name, data):
        """Inserta múltiples registros en una tabla utilizando la función dynamic_insert."""
        session = sessionmaker(bind=self.engine)()
        try:
            for entry in data:
                if isinstance(entry, dict):
                    columns = ', '.join(entry.keys())
                    values = ', '.join([f"'{entry[key]}'" if isinstance(entry[key], str) else str(entry[key]) 
                                        for key in entry.keys()])

                    on_conflict = 'symbol' if table_name == 'criptomonedas' else 'id'  # Cambia esto según la tabla

                    # Ejecutar la función directamente
                    result = session.execute(text(f"""
                        SELECT dynamic_insert(:table_name, :columns, :values, :on_conflict);
                    """), {
                        'table_name': table_name,
                        'columns': columns,
                        'values': values,
                        'on_conflict': on_conflict
                    }).fetchone()  # Usa fetchone() para obtener la primera fila

                    # Acceder al resultado
                    if result and result[0]:  # Asegúrate de acceder al primer elemento
                        print(f"Resultado de la inserción: {result[0]}")  # Imprime el resultado
                    else:
                        print("Error: No se pudo obtener el resultado para la inserción.")

                else:
                    print(f"Entry is not a dictionary: {entry}")

            session.commit()
        except SQLAlchemyError as e:
            print(f"Error durante la inserción en {table_name}: {e}")
            session.rollback()
        finally:
            session.close()  # Cierra la sesión



    def insertApi(self, data):
        """Insertar múltiples registros en la tabla 'api'."""
        session = sessionmaker(bind=self.engine)()
        try:
            for entry in data:
                if isinstance(entry, dict):
                    # Excluir 'id' si es autoincremental
                    entry = {k: v for k, v in entry.items() if k != 'id'}

                    # Crear la consulta de inserción
                    columns = ', '.join(entry.keys())
                    placeholders = ', '.join([f":{key}" for key in entry.keys()])

                    insert_query = text(f"""
                        INSERT INTO api ({columns}) VALUES ({placeholders})
                    """)

                    # Mensaje de depuración
                    print(f"Insert query for API: {insert_query} - Values: {entry}")

                    # Ejecutar la consulta
                    session.execute(insert_query, entry)
                else:
                    print(f"Entry is not a dictionary: {entry}")

            session.commit()
        except SQLAlchemyError as e:
            print(f"Error durante la inserción en la tabla 'api': {e}")
            session.rollback()
        finally:
            session.close()

    def insertCripto(self, data):
        """Insertar múltiples registros en la tabla 'criptomonedas'."""
        self.insert_data('criptomonedas', data)

    def insertRegistro(self, data):
        """Insertar múltiples registros en la tabla 'registros'."""
        self.insert_data('registros', data)

    def insertApiCriptomoneda(self, data):
        """Insertar múltiples criptomonedas en la tabla 'api_criptomonedas' usando la función insert_api_criptomoneda."""
        session = sessionmaker(bind=self.engine)()
        try:
            for entry in data:
                if isinstance(entry, dict) and 'id' in entry and 'symbol' in entry and 'name' in entry:
                    _id = entry['id']
                    _symbol = entry['symbol']
                    _name = entry['name']

                    insert_query = text(f"""
                        SELECT insert_api_criptomoneda(:_id, :_symbol, :_name) AS result;
                    """)    

                    result = session.execute(insert_query, {
                        '_id': _id,
                        '_symbol': _symbol,
                        '_name': _name
                    }).fetchone()   

                    # Accede al campo `result` de la fila
                    if result and result.result:
                        print(result.result)  # Muestra el mensaje de resultado
                    else:
                        print(f"Error: No se pudo obtener el valor esperado de la función insert_api_criptomoneda para id={_id}")   

            session.commit()
        except SQLAlchemyError as e:
            print(f"Error durante la inserción en la tabla 'api_criptomonedas': {e}")
            session.rollback() 
        finally:
            session.close()

    def insertAllRecordsInApiRegistros(self, api_id):
        """Guarda todos los registros en la tabla intermedia 'api_registros'."""
        session = sessionmaker(bind=self.engine)()
        try:
            # Recupera todos los registros de la tabla 'registros'
            registros = self.fetchAllRegistros(session)  # Modificado para pasar la sesión
            for registro in registros:
                # Asegúrate de que 'id' sea la clave que estás usando para los registros
                registro_id = registro['id']

                # Inserta en la tabla intermedia
                insert_query = text("""
                    INSERT INTO api_registros (api_id, registro_id) VALUES (:api_id, :registro_id)
                """)

                session.execute(insert_query, {'api_id': api_id, 'registro_id': registro_id})  

            session.commit()
            print(f"Todos los registros han sido insertados en 'api_registros' para api_id={api_id}.")

        except SQLAlchemyError as e:
            print(f"Error durante la inserción en 'api_registros': {e}")
            session.rollback() 
        finally:
            session.close()

    def fetchAllRegistros(self, session):
        """Recupera todos los registros de la base de datos."""
        try:
            result = session.execute(text("SELECT id FROM registros"))
            rows = result.fetchall()
            return [{'id': row[0]} for row in rows]
        except Exception as e:
            print(f"Error al obtener registros: {str(e)}")
            return []

    def fetchAllCriptomonedas(self):
        """Recupera todas las criptomonedas de la base de datos."""
        session = sessionmaker(bind=self.engine)()
        try:
            result = session.execute(text("SELECT id, name, symbol, msupply FROM criptomonedas"))
            rows = result.fetchall()
            return [{'id': row[0], 'name': row[1], 'symbol': row[2], 'msupply': row[3]} for row in rows]
        except Exception as e:
            print(f"Error al obtener criptomonedas: {str(e)}")
            return []
        finally:
            session.close()

    def fetchOneCriptoBySymbol(self, symbol):
        """Obtener una criptomoneda por símbolo."""
        session = sessionmaker(bind=self.engine)()
        try:
            result = session.execute(text(""" 
                SELECT * FROM get_criptomoneda_by_symbol(:symbol);
            """), {'symbol': symbol}).fetchone()
            return result if result else None
        finally:
            session.close()

    def fetchOneApiByName(self, nombre):
        """Obtener una API por nombre y retornar toda la fila."""
        session = sessionmaker(bind=self.engine)()
        try:
            result = session.execute(text("""
                SELECT * FROM fetchOneApiByName(:nombre);
            """), {'nombre': nombre}).fetchone()
            return result if result else None
        finally:
            session.close()

    def fetchOneRegisterId(self, criptoId):
        """Obtener el ID de un registro por ID de criptomoneda."""
        session = sessionmaker(bind=self.engine)()
        try:
            query = text("SELECT id FROM registros WHERE criptomoneda_id = :criptoId")
            result = session.execute(query, {'criptoId': criptoId}).fetchone()
            return result[0] if result else None  # Cambiado para acceder al primer elemento
        finally:
            session.close() 
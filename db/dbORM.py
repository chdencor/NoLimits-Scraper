import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import traceback
import datetime

class dbDefinitions:
    def __init__(self, db_url):
        """Inicializa la conexión a la base de datos."""
        self.engine = create_async_engine(db_url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine, class_=AsyncSession, expire_on_commit=False)

    async def insert_data(self, table_name, data):
        """Inserta múltiples registros en una tabla utilizando la función dynamic_insert."""
        async with asyncio.Lock():  # Evita condiciones de carrera
            async with self.SessionLocal() as session:
                try:
                    for entry in data:
                        if isinstance(entry, dict):
                            columns = ', '.join(entry.keys())
                            values = ', '.join([
                                f"'{entry[key]}'" if isinstance(entry[key], str) else
                                f"'{entry[key]}'" if isinstance(entry[key], datetime.datetime) else str(entry[key])
                                for key in entry.keys() if entry[key] is not None
                            ])

                            # Mostrar la consulta que se va a ejecutar para depuración
                            print(f'INSERT INTO {table_name} ({columns}) VALUES ({values})')

                            on_conflict = 'symbol' if table_name == 'criptomonedas' else 'id'

                            result = await session.execute(text(f"""
                                SELECT dynamic_insert(:table_name, :columns, :values, :on_conflict);
                            """), {
                                'table_name': table_name,
                                'columns': columns,
                                'values': values,
                                'on_conflict': on_conflict
                            })  # No se usa fetchone aquí

                            result = result.fetchone()  # Usa fetchone() para obtener la primera fila

                            if result and result[0]:
                                print(f"Resultado de la inserción en {table_name}: {result[0]}")
                            else:
                                print(f"Error: No se pudo obtener el resultado para la inserción en {table_name}.")

                    await session.commit()
                except SQLAlchemyError as e:
                    print(f"Error durante la inserción en {table_name}: {e}")
                    await session.rollback()

    async def insertApi(self, data):
        """Insertar múltiples registros en la tabla 'api' de forma asincrónica."""
        async with self.SessionLocal() as session:
            try:
                for entry in data:
                    if isinstance(entry, dict):
                        # Verificar si la API ya existe
                        existing_api = await self.fetchOneApiByName(entry['nombre'])
                        if existing_api:
                            print(f"La API '{entry['nombre']}' ya existe. Omitiendo inserción.")
                            continue  # Omitir la inserción si ya existe    

                        entry = {k: v for k, v in entry.items() if k != 'id'}
                        columns = ', '.join(entry.keys())
                        placeholders = ', '.join([f":{key}" for key in entry.keys()])   

                        insert_query = text(f"""
                            INSERT INTO api ({columns}) VALUES ({placeholders})
                        """)    

                        print(f"Insert query for API: {insert_query} - Values: {entry}")    

                        await session.execute(insert_query, entry)  

                await session.commit()
            except SQLAlchemyError as e:
                print(f"Error durante la inserción en la tabla 'api': {e}")
                await session.rollback()
            except Exception as e:
                print(f"Error inesperado: {e}")
                await session.rollback()
                traceback.print_exc()


    async def insertCripto(self, data):
        """Insertar múltiples registros en la tabla 'criptomonedas'."""
        await self.insert_data('criptomonedas', data)

    async def insertRegistro(self, data):
        """Insertar múltiples registros en la tabla 'registros'."""
        await self.insert_data('registros', data)

    async def insertApiCriptomoneda(self, data):
        """Insertar múltiples criptomonedas en la tabla 'api_criptomonedas'."""
        async with asyncio.Lock():
            async with self.SessionLocal() as session:
                try:
                    for entry in data:
                        if isinstance(entry, dict) and 'id' in entry and 'symbol' in entry and 'name' in entry:
                            _id = entry['id']
                            _symbol = entry['symbol']
                            _name = entry['name']

                            insert_query = text(f"""
                                SELECT insert_api_criptomoneda(:_id, :_symbol, :_name) AS result;
                            """)

                            result = await session.execute(insert_query, {
                                '_id': _id,
                                '_symbol': _symbol,
                                '_name': _name
                            })

                            result = result.fetchone()

                            if result and result.result:
                                print(result.result)
                            else:
                                print(f"Error: No se pudo obtener el valor esperado de la función insert_api_criptomoneda para id={_id}")

                    await session.commit()
                except SQLAlchemyError as e:
                    print(f"Error durante la inserción en la tabla 'api_criptomonedas': {e}")
                    await session.rollback()

    async def fetchAllRegistros(self):
        """Recupera todos los registros de la base de datos."""
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(text("SELECT id FROM registros"))
                rows = result.fetchall()
                return [{'id': row[0]} for row in rows]
            except Exception as e:
                print(f"Error al obtener registros: {str(e)}")
                return []

    async def fetchAllCriptomonedas(self):
        """Recupera todas las criptomonedas de la base de datos."""
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(text("SELECT id, name, symbol, msupply FROM criptomonedas"))
                rows = result.fetchall()
                return [{'id': row[0], 'name': row[1], 'symbol': row[2], 'msupply': row[3]} for row in rows]
            except Exception as e:
                print(f"Error al obtener criptomonedas: {str(e)}")
                return []

    async def fetchOneCriptoBySymbol(self, symbol):
        """Obtener una criptomoneda por símbolo."""
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(text(""" 
                    SELECT * FROM get_cripto_by_symbol(:symbol);
                """), {'symbol': symbol})
                return result.fetchone() if result else None
            except Exception as e:
                print(f"Error al obtener criptomoneda por símbolo: {str(e)}")
                return None

    async def fetchOneApiByName(self, nombre):
        """Obtener una API por nombre y retornar toda la fila."""
        async with self.SessionLocal() as session:
            try:
                result = await session.execute(text(""" 
                    SELECT * FROM fetchOneApiByName(:nombre);
                """), {'nombre': nombre})
                return result.fetchone() if result else None
            except Exception as e:
                print(f"Error al obtener API por nombre: {str(e)}")
                return None

    async def fetchOneRegisterId(self, criptoId):
        """Obtener el ID de un registro por ID de criptomoneda."""
        async with self.SessionLocal() as session:
            try:
                query = text("SELECT id FROM registros WHERE criptomoneda_id = :criptoId")
                result = await session.execute(query, {'criptoId': criptoId})
                return result.fetchone()[0] if result.fetchone() else None
            except Exception as e:
                print(f"Error al obtener ID de registro: {str(e)}")
                return None

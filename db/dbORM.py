from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

class dbORM:
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def close(self):
        self.session.close()

    def batch_insert(self, table_name, values_list):
        """Realizar inserciones por lotes en la tabla especificada."""
        try:
            if values_list:
                # Crear una consulta con múltiples valores
                columns = values_list[0].keys()
                placeholders = ', '.join(f':{col}' for col in columns)
                insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                
                # Ejecutar el batch insert
                self.session.execute(text(insert_query), values_list)
                self.session.commit()
        except Exception as e:
            self.session.rollback()
            print(f"Error al insertar en {table_name}: {e}")

    def insert_api_batch(self, api_list):
        """Insertar múltiples registros en la tabla 'api'."""
        self.batch_insert('api', api_list)

    def insert_register_batch(self, register_list):
        """Insertar múltiples registros en la tabla 'registros'."""
        self.batch_insert('registros', register_list)

    def insert_criptomoneda_batch(self, cripto_list):
        """Insertar múltiples registros en la tabla 'criptomonedas'."""
        self.batch_insert('criptomonedas', cripto_list)

    def insert_api_relationship_batch(self, table_name, relationship_list, unique_columns):
        """Insertar múltiples relaciones en la tabla especificada, evitando duplicados."""
        try:
            for relationship in relationship_list:
                filters = ' AND '.join([f"{col} = :{col}" for col in unique_columns])
                query = f"SELECT 1 FROM {table_name} WHERE {filters}"
    
                # Verificar si la relación ya existe
                existing_relationship = self.session.execute(
                    text(query), {col: relationship[col] for col in unique_columns}
                ).fetchone()
    
                # Si no existe, insertar la nueva relación
                if not existing_relationship:
                    self.batch_insert(table_name, [relationship])
                else:
                    print(f"La relación en {table_name} ya existe: {relationship}")
    
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print(f"Error al insertar relaciones en {table_name}: {e}")

    # Métodos para cada tabla intermedia
    def insert_api_criptomoneda_batch(self, relationship_list):
        self.insert_api_relationship_batch('api_criptomonedas', relationship_list, ['api_id', 'criptomoneda_id'])

    def insert_api_mercado_batch(self, relationship_list):
        self.insert_api_relationship_batch('api_mercados', relationship_list, ['api_id', 'mercado_id'])

    def insert_api_registro_batch(self, relationship_list):
        self.insert_api_relationship_batch('api_registros', relationship_list, ['api_id', 'registro_id'])

    def insert_api_metrica_general_batch(self, relationship_list):
        self.insert_api_relationship_batch('api_metricas_generales', relationship_list, ['api_id', 'metrica_general_id'])

    def insert_api_exchange_batch(self, relationship_list):
        self.insert_api_relationship_batch('api_exchanges', relationship_list, ['api_id', 'exchange_id'])


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
        query = text("SELECT id FROM registros WHERE criptomoneda_id = :criptoId")
        result = self.session.execute(query, {'criptoId': criptoId}).fetchone()
        return result.id if result else None
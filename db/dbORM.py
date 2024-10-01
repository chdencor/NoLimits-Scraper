from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from models.SQLAlchemyTables import Criptomoneda, Registro

# Configuración base
Base = declarative_base()

class dbORM:
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def createTables(self):
        """Crear las tablas si no existen."""
        Base.metadata.create_all(self.engine)

    def close(self):
        """Cerrar la sesión."""
        self.session.close()

    # Operaciones CRUD para Criptomonedas
    def createCripto(self, name, symbol, msupply):
        """Insertar una nueva criptomoneda si no existe."""
        if not self.checkSymbolExists(symbol):
            nuevaCripto = Criptomoneda(name=name, symbol=symbol, msupply=msupply)
            self.session.add(nuevaCripto)
            self.session.commit()
            return nuevaCripto.id
        return None


    def checkSymbolExists(self, symbol):
        """Verificar si el símbolo de una criptomoneda ya existe."""
        return self.session.query(Criptomoneda).filter_by(symbol=symbol).first() is not None

    def fetchAllCriptos(self):
        """Obtener todas las criptomonedas."""
        return self.session.query(Criptomoneda).all()

    def fetchOneCripto(self, idValue):
        """Obtener una criptomoneda por ID."""
        return self.session.query(Criptomoneda).get(idValue)
    
    def fetchOneCriptoBySymbol(self, symbol):
        """Obtener una criptomoneda por símbolo."""
        return self.session.query(Criptomoneda).filter_by(symbol=symbol).first()

    def updateCripto(self, idValue, columns, values):
        """Actualizar los datos de una criptomoneda."""
        cripto = self.session.query(Criptomoneda).get(idValue)
        if cripto:
            for col, val in zip(columns, values):
                setattr(cripto, col, val)
            self.session.commit()

    def deleteCripto(self, idValue):
        """Eliminar una criptomoneda."""
        cripto = self.session.query(Criptomoneda).get(idValue)
        if cripto:
            self.session.delete(cripto)
            self.session.commit()

    # Operaciones CRUD para Registros
    def createRegistro(self, cripto_id, price_usd, percent_change_24h, percent_change_1h,
                       percent_change_7d, price_btc, market_cap_usd,
                       volume24, volume24a, csupply, tsupply):
        """Insertar un nuevo registro asociado a una criptomoneda."""
        # Verificar si la criptomoneda existe
        cripto = self.session.query(Criptomoneda).get(cripto_id)
        if not cripto:
            raise ValueError("La criptomoneda con el ID proporcionado no existe.")
        
        nuevo_registro = Registro(
            cripto_id=cripto_id,  # Asociar el registro a la criptomoneda
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
        )
        self.session.add(nuevo_registro)
        self.session.commit()
        return nuevo_registro.id

    def fetchAllRegistros(self):
        """Obtener todos los registros."""
        return self.session.query(Registro).all()

    def fetchOneRegistro(self, idValue):
        """Obtener un registro por ID."""
        return self.session.query(Registro).get(idValue)

    def updateRegistro(self, idValue, columns, values):
        """Actualizar un registro."""
        registro = self.session.query(Registro).get(idValue)
        if registro:
            for col, val in zip(columns, values):
                setattr(registro, col, val)
            self.session.commit()

    def deleteRegistro(self, idValue):
        """Eliminar un registro."""
        registro = self.session.query(Registro).get(idValue)
        if registro:
            self.session.delete(registro)
            self.session.commit()

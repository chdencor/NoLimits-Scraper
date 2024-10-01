from sqlalchemy import Column, Integer, Float, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Criptomoneda(Base):
    __tablename__ = 'criptomonedas'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    symbol = Column(String(10), nullable=False, unique=True)
    msupply = Column(Float)  # Atributo fijo, máximo suministro que no cambia

    registros = relationship("Registro", back_populates="criptomoneda")

class Registro(Base):
    __tablename__ = 'registros'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    criptomoneda_id = Column(Integer, ForeignKey('criptomonedas.id'), nullable=False)  # Clave foránea hacia la tabla criptomonedas
    price_usd = Column(Float)
    percent_change_24h = Column(Float)
    percent_change_1h = Column(Float)
    percent_change_7d = Column(Float)
    price_btc = Column(Float)
    market_cap_usd = Column(Float)
    volume24 = Column(Float)
    volume24a = Column(Float)
    csupply = Column(Float)  # Circulating supply
    tsupply = Column(Float)  # Total supply
    created_at = Column(DateTime, default=datetime.now)

    criptomoneda = relationship("Criptomoneda", back_populates="registros")  # Relación hacia la tabla criptomonedas

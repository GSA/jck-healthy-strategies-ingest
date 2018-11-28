import psycopg2
from sqlalchemy import create_engine, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Table, Text, \
                       Date, Boolean, Sequence, DateTime
from sqlalchemy.orm import relationship, sessionmaker
import os

#test env
if os.getenv('TEST_DATABASE_URL'):
	DB_URL = os.getenv('DATABASE_URL').replace('postgresql', 'postgresql+psycopg2')
#prod env
else:
	DB_URL = 'postgresql+psycopg2://user:password@hostname/database_name'

Base = declarative_base()

class DataAccessLayer:

    def __init__(self):
        self.engine = None
        self.conn_string = DB_URL

    def connect(self):
        self.engine = create_engine(self.conn_string)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

dal = DataAccessLayer()

class Building(Base):
    __tablename__ = 'building'
    id = Column(Integer, primary_key=True)
    building_name = Column(String(50))
    locations = relationship("Location", back_populates="building")

class Location(Base):
    __tablename__ = 'location'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    location_name = Column(String(50))
    building = relationship("Building", back_populates="locations")
    indicators = locations = relationship("Indicator", back_populates="indicator")

class Indicator(Base):
    __tablename__ = 'indicator'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    location_id = Column(Integer, ForeignKey('location.id'))
    indicator_name = Column(String(50))
    location = relationship("Location", back_populates="indicators")
    units = relationship("Unit", back_populates="indicator")

class Unit(Base):
    __tablename__ = 'unit'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    location_id = Column(Integer, ForeignKey('location.id'))
    indicator_id = Column(Integer, ForeignKey('indicator.id'))
    unit_name = Column(String(50))
    indicator = relationship("Indicator", back_populates='units')
    values = relationship("Value", back_populates='unit')

class Value(Base):
    __tablename__ = 'unit'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    location_id = Column(Integer, ForeignKey('location.id'))
    unit_id = Column(Integer, ForeignKey('unit.id'))
    value_name = Column(String(50))
    unit = relationship("Unit", back_populates='values')

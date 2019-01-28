from sqlalchemy import create_engine, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.orm import relationship

Base = declarative_base()

class Building(Base):
    __tablename__ = 'building'
    id = Column(Integer, primary_key=True)
    building_name = Column(String(100), index=True)
    floors = relationship("Floor", back_populates="building")

class Floor(Base):
    __tablename__ = 'floor'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    floor_name = Column(String(10), index=True)
    building = relationship("Building", back_populates="floors")
    rooms = relationship("Room", back_populates="floor")
    
class Room(Base):
    __tablename__ = 'room'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    floor_id = Column(Integer, ForeignKey('floor.id'))
    room_type = Column(String(100))
    room_number = Column(String(100), index=True)
    floor = relationship("Floor", back_populates='rooms')
    modalities = relationship("Modality", back_populates="room")
    
    
class Modality(Base):
    __tablename__ = 'modality'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    floor_id = Column(Integer, ForeignKey('floor.id'))
    room_id = Column(Integer, ForeignKey('room.id'))
    modality_name = Column(String(100), index=True)
    room = relationship("Room", back_populates="modalities")
    units = relationship("Unit", back_populates="modality")
    
class Unit(Base):
    __tablename__ = 'unit'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    floor_id = Column(Integer, ForeignKey('floor.id'))
    room_id = Column(Integer, ForeignKey('room.id'))
    modality_id = Column(Integer, ForeignKey('modality.id'))
    unit_name = Column(String(50), index=True)
    modality = relationship("Modality", back_populates='units')
    values = relationship("Value", back_populates='unit')

class Value(Base):
    __tablename__ = 'value'
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey('building.id'))
    floor_id = Column(Integer, ForeignKey('floor.id'))
    room_id = Column(Integer, ForeignKey('room.id'))
    modality_id = Column(Integer, ForeignKey('modality.id'))
    unit_id = Column(Integer, ForeignKey('unit.id'))
    value = Column(Float)
    time_stamp = Column(DateTime)
    unit = relationship("Unit", back_populates='values')
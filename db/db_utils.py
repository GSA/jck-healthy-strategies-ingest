from .db import Building, Floor, Room, Modality, Unit, Value, Base
from contextlib import contextmanager
from sqlalchemy import create_engine, func, case, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database
import logging
import pandas as pd
import os
import sys


def get_db_url():
    '''
    Return the db connection string depending on the environment
    '''
    if os.getenv('TEST_DB_URL'):
        db_string = os.getenv('TEST_DB_URL')
    else:
        db_string = "postgresql+psycopg2://localhost/test"    
    
    return db_string


class DataAccessLayer:

    def __init__(self, conn_string):
        self.engine = None
        self.conn_string = conn_string

    def connect(self):
        local = self._create_local_postgres()
        if not local:
            self.engine = create_engine(self.conn_string)
        try:
            Base.metadata.create_all(self.engine)
        except Exception as e:
            logging.critical(f"Exception occurred creating database metadata with uri:  \
                               {self.conn_string}. Full traceback here:  {e}", exc_info=True)
            sys.exit(1)
        self.Session = sessionmaker(bind = self.engine)

    def _create_local_postgres(self):
        test_conn_string = self.conn_string == "postgresql+psycopg2://localhost/test"
        if test_conn_string:
            self.engine = create_engine(self.conn_string)
            if not database_exists(self.engine.url):
                create_database(self.engine.url)
                return True
        else:
            return

    def drop_local_postgres_db(self):
        test_conn_string = self.conn_string == "postgresql+psycopg2://localhost/test"
        if database_exists(self.engine.url) and test_conn_string:
            drop_database(self.engine.url)

dal = DataAccessLayer(conn_string = get_db_url())

@contextmanager
def session_scope(dal):
    """Provide a transactional scope around a series of operations."""
    session = dal.Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logging.critical(f"Exception occurred during database session, causing a rollback:  \
                           {e}", exc_info=True)
    finally:
        session.close()

def fetch_building_id(building_name, session):
    try:
        building_id = session.query(Building.id).filter(Building.building_name==building_name).first().id
    except AttributeError:
        return None
    return building_id

def fetch_floor_id(floor_name, session):
    try:
        floor_id = session.query(Floor.id).filter(Floor.floor_name==floor_name).first().id
    except AttributeError:
        return None
    return floor_id

def fetch_room_id(room_number, session):
    try:
        room_id = session.query(Room.id).filter(Room.room_number==room_number).first().id
    except AttributeError:
        return None
    return room_id

def fetch_modality_id(modality_name, session):
    try:
        modality_id = session.query(Modality.id).filter(Modality.modality_name==modality_name).first().id
    except AttributeError:
        return None
    return modality_id

def fetch_unit_id(unit_name, session):
    try:
        unit_id = session.query(Unit.id).filter(Unit.unit_name==unit_name).first().id
    except AttributeError:
        return None
    return unit_id

def get_building(building_id, session):
    building = session.query(Building).get(building_id)
    return building

def get_floor(floor_id, session):
    floor = session.query(Floor).get(floor_id)
    return floor

def get_room(room_id, session):
    room = session.query(Room).get(room_id)
    return room

def get_modality(modality_id, session):
    modality = session.query(Modality).get(modality_id)
    return modality
    
def get_unit(unit_id, session):
    unit = session.query(Unit).get(unit_id)
    return unit
    
def insert_data(df_melted, session):
    buildings = df_melted['building'].unique()
    for b in buildings:
        building_df = df_melted[df_melted['building'] == b]
        building_id = fetch_building_id(b, session)
        if not building_id:
            building = Building(building_name = b) 
        else:
            building = get_building(building_id, session)
        floors = building_df['floor'].unique()
        for f in floors:
            floor_df = building_df[building_df['floor'] == f]
            floor_id = fetch_floor_id(f, session)
            if not floor_id:
                floor = Floor(floor_name = f) 
            else:
                floor = get_floor(floor_id, session)
            rooms = floor_df['room_number'].unique()
            for r in rooms:
                room_df = floor_df[floor_df['room_number'] == r]
                room_id = fetch_room_id(r, session)
                if not room_id:
                    room_type = list(room_df['room_type'].unique())[0]
                    room = Room(room_number = r, room_type = room_type) 
                else:
                    room = get_room(room_id, session)
                modalities = room_df['modality'].unique()
                for m in modalities:
                    modality_df = room_df[room_df['modality'] == m]
                    modality_id = fetch_modality_id(m, session)
                    if not modality_id:
                        modality = Modality(modality_name = m) 
                    else:
                        modality = get_modality(modality_id, session)
                    units = modality_df['unit'].unique()
                    for u in units:
                        unit_df = modality_df[modality_df['unit'] == u]
                        unit_id = fetch_unit_id(u, session)
                        if not unit_id:
                            unit = Unit(unit_name = u)
                        else:
                            unit = get_unit(unit_id, session)
                        for _, row in unit_df.iterrows():
                            time_stamp = pd.to_datetime(row['Timestamp']).to_pydatetime()
                            val = row['value']
                            value = Value(value = val, time_stamp = time_stamp)
                            unit.values.append(value)
                        modality.units.append(unit)
                    room.modalities.append(modality)
                floor.rooms.append(room)
            building.floors.append(floor)
        session.add(building)


def fetch_last_update(session):
    try:
        last_update = session.query(Value).order_by(desc('time_stamp')).first()
    except AttributeError:
        return None
    try:
        last_update = last_update.time_stamp
    except AttributeError:
        return None
    return last_update
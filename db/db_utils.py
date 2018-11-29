from .db import Building, Location, Indicator, Unit, Value
from sqlalchemy import create_engine, desc
import pandas as pd
from sqlalchemy_utils import database_exists, create_database
import os

def insert_data(grouped_df, session):
    buildings = grouped_df['building_name'].unique()
    for b in buildings:
        building_df = grouped_df[grouped_df['building_name'] == b]
        building_id = fetch_building_id(b, session)
        if not building_id:
            building = Building(building_name = b) 
        else:
            building = session.query(Building).get(building_id)
        locations = building_df['location'].unique()
        for l in locations:
            location_df = building_df[building_df['location'] == l]
            location_id = fetch_location_id(l, session)
            if not location_id:
                location = Location(location_name = l) 
            else:
                location = session.query(Location).get(location_id)
            indicators = location_df['modality_indicator'].unique()
            for i in indicators:
                indicator_df = location_df[location_df['modality_indicator'] == i]
                indicator_id = fetch_indicator_id(i, session)
                if not indicator_id:
                    indicator = Indicator(indicator_name = i) 
                else:
                    indicator = session.query(Indicator).get(indicator_id)
                units = indicator_df['unit'].unique()
                for u in units:
                    unit_df = indicator_df[indicator_df['unit'] == u]
                    unit_id = fetch_unit_id(u, session)
                    if not unit_id:
                        unit = Unit(unit_name = u) 
                    else:
                        unit = session.query(Unit).get(unit_id)
                    for _, row in unit_df.iterrows():
                        time_stamp = pd.to_datetime(row['timestamp']).to_pydatetime()
                        val = row['value']
                        value = Value(value = val, time_stamp = time_stamp)
                        unit.values.append(value)
                    indicator.units.append(unit)
                location.indicators.append(indicator)
            building.locations.append(location)
        session.add(building)

def fetch_building_id(building_name, session):
    try:
        building_id = session.query(Building.id).filter(Building.building_name==building_name).first().id
    except AttributeError:
        return None
    return building_id

def fetch_location_id(location_name, session):
    try:
        location_id = session.query(Location.id).filter(Location.location_name==location_name).first().id
    except AttributeError:
        return None
    return location_id

def fetch_indicator_id(indicator_name, session):
    try:
        indicator_id = session.query(Indicator.id).filter(Indicator.indicator_name==indicator_name).first().id
    except AttributeError:
        return None
    return indicator_id

def fetch_unit_id(unit_name, session):
    try:
        unit_id = session.query(Unit.id).filter(Unit.unit_name==unit_name).first().id
    except AttributeError:
        return None
    return unit_id

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
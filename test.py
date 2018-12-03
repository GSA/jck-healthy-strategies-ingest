import requests
import httpretty
import unittest
import os
import json
import datetime
import pandas as pd
from sqlalchemy import func
from skyspark import SkySparkAPI
from db.db import DataAccessLayer, Building, Location, Indicator, Unit, Value
from db.db_utils import insert_data
from main import main



#test env
if os.getenv('TEST_DB_URL'):
	DB_URL = os.getenv('TEST_DB_URL').replace('postgresql', 'postgresql+psycopg2')
#prod env
else:
	DB_URL = 'sqlite:///:memory:'

dal = DataAccessLayer()

def join_temp_path(file_name):
    dummy_path = os.path.join(os.getcwd(), 'temp', file_name)
    
    return dummy_path


def exceptionCallback(request, uri, headers):
    '''
    Create a callback body that raises an exception when opened. This simulates a bad request.
    '''
    raise requests.ConnectionError('Raising a connection error for the test. You can ignore this!')


class SkySparkAPITestCase(unittest.TestCase):

    def setUp(self):
        self.ss = SkySparkAPI(date = '2018-10-01')

    def tearDown(self):
        self.ss = None

    def test_download_data(self):
        self.ss.ftp_url = 'ftp://ftp.fbo.gov/FBOFeed20180101'
        result = self.ss.download_data()
        expected = join_temp_path('jck_sensor_data_2018-10-01.csv')
        self.assertEqual(result, expected)

    def test_create_data_frame(self):
        result = self.ss.create_data_frame('fixtures/dummy.csv')
        expected = pd.read_csv('fixtures/dummy.csv')
        pd.testing.assert_frame_equal(result, expected)


class DBTestCase(unittest.TestCase):      
    
    @classmethod
    def setUpClass(cls):
        building_names = ['Chicago Air Quality',
                          'Chicago Air Quality',
                          'Chicago Air Quality',
                          'Chicago Air Quality',
                          'Chicago Air Quality']
        locations = ['South Office',
                     'North Office',
                     'Outdoor Reading',
                     'South Office',
                     'Outdoor Reading']
        modality_indicators = ['Total Volatile Organic Compounds (TVOC)-tvoc',
                               'Humidity-humidity',
                               'Outside Particle Polution-pm2p5',
                               'CO2-co2',
                               'Outside Particle Polution-pm2p5']
        units = ['µg/m³', '%RH', 'µg/m³', 'ppm', 'µg/m³']
        timestamps = ['2018-11-25T08:00:00-06:00',
                      '2018-11-25T01:00:00-06:00',
                      '2018-11-25T23:00:00-06:00',
                      '2018-11-25T21:00:00-06:00',
                      '2018-11-25T13:00:00-06:00']
        values = [60.0, 32.060001373291016, 1.0, 388.0, 2.0]
        test_df = pd.DataFrame([building_names,
                                locations,
                                modality_indicators,
                                units,
                                timestamps,
                                values]).transpose()
        test_df.columns = ['building_name',
                           'location',
                           'modality_indicator',
                           'unit',
                           'timestamp',
                           'value'
                            ]
        cls.test_df = test_df
        cls.dal = dal
        cls.dal.connect()
        
    @classmethod
    def tearDownClass(cls):
        cls.test_df = None
        cls.dal = None

    def test_insert_data(self):
        session = dal.Session()
        insert_data(DBTestCase.test_df, session)
        session.commit()
        session = DBTestCase.dal.Session()
        building_rows = session.query(func.count(Building.id)).scalar()
        self.assertEqual(building_rows,1)
        location_rows = session.query(func.count(Location.id)).scalar()
        self.assertEqual(location_rows,3)
        indicator_rows = session.query(func.count(Indicator.id)).scalar()
        self.assertEqual(indicator_rows,4)
        unit_rows = session.query(func.count(Unit.id)).scalar()
        self.assertEqual(unit_rows,4)
        value_rows = session.query(func.count(Value.id)).scalar()
        self.assertEqual(value_rows,5)

    def test_insert_data_dupe_parents(self):
        session = dal.Session()
        test_df = DBTestCase.test_df
        # insert additional data where the only differences are in the values and times. 
        # This should NOT add new rows to the other tables.
        test_df['timestamp'] = ['2018-11-25T07:00:00-06:00',
                                '2018-11-25T02:00:00-06:00',
                                '2018-11-25T22:00:00-06:00',
                                '2018-11-25T20:00:00-06:00',
                                '2018-11-25T12:00:00-06:00']
        test_df['value'] = [1, 2, 3, 4, 5]
        insert_data(test_df, session)
        building_rows = session.query(func.count(Building.id)).scalar()
        self.assertEqual(building_rows,1)
        location_rows = session.query(func.count(Location.id)).scalar()
        self.assertEqual(location_rows,3)
        indicator_rows = session.query(func.count(Indicator.id)).scalar()
        self.assertEqual(indicator_rows,4)
        unit_rows = session.query(func.count(Unit.id)).scalar()
        self.assertEqual(unit_rows,4)
        value_rows = session.query(func.count(Value.id)).scalar()
        self.assertEqual(value_rows,10)




    


        

    

    

    


import requests
import httpretty
import unittest
import os
import datetime
import pandas as pd
from sqlalchemy import func
from skyspark import SkySparkAPI
from db.db import Building, Floor, Room, Modality, Unit, Value
from db.db_utils import insert_data, dal, session_scope
from main import main


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
        result = self.ss.create_data_frame('fixtures/2019yr1mo3day8h.csv')
        expected = pd.read_csv('fixtures/2019yr1mo3day8h.csv')
        pd.testing.assert_frame_equal(result, expected)


class DBTestCase(unittest.TestCase):      
    
    @classmethod
    def setUpClass(cls):
        building_names = ['JCK',
                          'JCK',
                          'JCK',
                          'JCK',
                          'JCK']
        floors = ['1',
                  '2',
                  '3',
                  '4',
                  '4']
        room_types = ['a','b','c','c','c']
        room_numbers = ['1','2','3','4','5']
        modalities = ['temp',
                      'temp',
                      'co',
                      'co',
                      'co']
        units = ['°C', '°C', 'ppm', 'ppm', 'ppm']
        timestamps = ['2018-11-25T08:00:00-06:00',
                      '2018-11-25T01:00:00-06:00',
                      '2018-11-25T23:00:00-06:00',
                      '2018-11-25T21:00:00-06:00',
                      '2018-11-25T13:00:00-06:00']
        values = [1,2,3,4,5]
        test_df = pd.DataFrame([building_names,
                                floors,
                                room_types,
                                room_numbers,
                                modalities,
                                units,
                                timestamps,
                                values]).transpose()
        test_df.columns = ['building',
                           'floor',
                           'room_type',
                           'room_number',
                           'modality',
                           'unit',
                           'Timestamp',
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
        with session_scope(dal) as session:
            insert_data(test_df, session)
        
        with session_scope(dal) as session:
            building_rows = session.query(func.count(Building.id)).scalar()
            self.assertEqual(building_rows,1)
            floor_rows = session.query(func.count(Floor.id)).scalar()
            self.assertEqual(floor_rows,4)
            room_rows = session.query(func.count(Room.id)).scalar()
            self.assertEqual(room_rows,5)
            modality_rows = session.query(func.count(Modality.id)).scalar()
            self.assertEqual(modality_rows,2)
            unit_rows = session.query(func.count(Unit.id)).scalar()
            self.assertEqual(unit_rows,2)
            value_rows = session.query(func.count(Value.id)).scalar()
            self.assertEqual(value_rows,5)

    def test_insert_data_dupe_parents(self):
        test_df = DBTestCase.test_df
        for i in range(5):
            test_df.at[i, 'value'] = 100
        with session_scope(dal) as session:
            insert_data(test_df, session)
        
        with session_scope(dal) as session:
            building_rows = session.query(func.count(Building.id)).scalar()
            self.assertEqual(building_rows,1)
            floor_rows = session.query(func.count(Floor.id)).scalar()
            self.assertEqual(floor_rows,4)
            room_rows = session.query(func.count(Room.id)).scalar()
            self.assertEqual(room_rows,5)
            modality_rows = session.query(func.count(Modality.id)).scalar()
            self.assertEqual(modality_rows,2)
            unit_rows = session.query(func.count(Unit.id)).scalar()
            self.assertEqual(unit_rows,2)
            #the only difference should be here
            value_rows = session.query(func.count(Value.id)).scalar()
            self.assertEqual(value_rows,10)




    


        

    

    

    


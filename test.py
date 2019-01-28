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


def join_temp_path(file_name):
    dummy_path = os.path.join(os.getcwd(), 'temp', file_name)
    
    return dummy_path

def get_expected_df():
    expected_df = pd.read_csv('fixtures/expected.csv')
    expected_df['Timestamp'] = pd.to_datetime(expected_df['Timestamp'])
    return expected_df

def exceptionCallback(request, uri, headers):
    '''
    Create a callback body that raises an exception when opened. This simulates a bad request.
    '''
    raise requests.ConnectionError('Raising a connection error for the test. You can ignore this!')


class SkySparkAPITestCase(unittest.TestCase):

    def setUp(self):
        self.ss = SkySparkAPI(date = '2018-10-01')
        self.expected_df = get_expected_df()

    def tearDown(self):
        self.ss = None
        self.expected_df = None

    def test_download_data(self):
        self.ss.ftp_url = 'ftp://ftp.fbo.gov/FBOFeed20180101'
        result = self.ss.download_data()
        expected = join_temp_path('jck_sensor_data_2018-10-01.csv')
        self.assertEqual(result, expected)

    def test_create_data_frame(self):
        result = self.ss.create_data_frame('fixtures/2019yr1mo3day8h.csv').shape
        expected = self.expected_df.shape
        self.assertTupleEqual(result, expected)


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
            insert_data(DBTestCase.test_df, session)
        result = 0
        with session_scope(dal) as session:
            building_rows = session.query(func.count(Building.id)).scalar()
            result += building_rows == 1
            floor_rows = session.query(func.count(Floor.id)).scalar()
            result += floor_rows == 4
            room_rows = session.query(func.count(Room.id)).scalar()
            result += room_rows == 5
            modality_rows = session.query(func.count(Modality.id)).scalar()
            result += modality_rows == 5
            unit_rows = session.query(func.count(Unit.id)).scalar()
            result += unit_rows == 5
            value_rows = session.query(func.count(Value.id)).scalar()
            result += value_rows == 5
        expected = 6
        self.assertEqual(result, expected)

    def test_insert_data_dupe_parents(self):
        test_df = DBTestCase.test_df
        for i in range(5):
            test_df.at[i, 'value'] = 100
        with session_scope(dal) as session:
            insert_data(test_df, session)
        
        result = 0
        with session_scope(dal) as session:
            building_rows = session.query(func.count(Building.id)).scalar()
            result += building_rows == 1
            floor_rows = session.query(func.count(Floor.id)).scalar()
            result += floor_rows == 4
            room_rows = session.query(func.count(Room.id)).scalar()
            result += room_rows == 5
            modality_rows = session.query(func.count(Modality.id)).scalar()
            result += modality_rows == 5
            unit_rows = session.query(func.count(Unit.id)).scalar()
            result += unit_rows == 5
            value_rows = session.query(func.count(Value.id)).scalar()
            result += value_rows == 10
        expected = 6
        self.assertEqual(result,expected)




    


        

    

    

    


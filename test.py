import requests
import httpretty
import unittest
import json
from skyspark import SkySparkAPI

def exceptionCallback(request, uri, headers):
    '''
    Create a callback body that raises an exception when opened. This simulates a bad request.
    '''
    raise requests.ConnectionError('Raising a connection error for the test. You can ignore this!')

class SkySparkAPITestCase(unittest.TestCase):

    
    def setUp(self):
        with open('fixtures/dummy_data.json') as f:
            dummy_data = json.loads(f.read())
        key = '123'
        last_update = '2018/11/28'
        self.ss = SkySparkAPI(key=key, last_update=last_update)
        self.dummy_data = dummy_data

    def tearDown(self):
        self.ss = None
        self.dummy_data = None

    @httpretty.activate
    def test_get_data_conn_error(self):
        uri = self.ss.uri
        httpretty.register_uri(httpretty.GET, 
                               uri=uri, 
                               body=exceptionCallback,
                               status=200)
        with self.assertRaises(SystemExit) as cm:
            _ = self.ss.get_data(uri)
        self.assertEqual(cm.exception.code, 1)

    @httpretty.activate
    def test_get_data_non200(self):
        uri = self.ss.uri
        httpretty.register_uri(httpretty.GET, 
                               uri=uri, 
                               body="doesn't matter",
                               status=404)
        with self.assertRaises(SystemExit) as cm:
            _ = self.ss.get_data(uri)
        self.assertEqual(cm.exception.code, 1)

    @httpretty.activate
    def test_get_data(self):
        uri = self.ss.uri
        dummy_data = json.dumps(self.dummy_data)
        httpretty.register_uri(httpretty.GET, 
                               uri=uri, 
                               body=dummy_data,
                               status=200)
        result = self.ss.get_data(uri)
        expected = self.dummy_data
        self.assertDictEqual(result, expected)

    @httpretty.activate
    def test_json_to_parsed_df(self):
        uri = self.ss.uri
        dummy_data = json.dumps(self.dummy_data)
        httpretty.register_uri(httpretty.GET, 
                               uri=uri, 
                               body=dummy_data,
                               status=200)
        df = self.ss.json_to_parsed_df()
        result = df.shape
        expected = (312, 14)
        self.assertTupleEqual(result, expected)

        

    


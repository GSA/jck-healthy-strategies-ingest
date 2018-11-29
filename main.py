from db.db import DataAccessLayer
from db.db_utils import insert_data, fetch_last_update
from skyspark import SkySparkAPI
import os
import sys

#test env
if os.getenv('TEST_DB_URL'):
	DB_URL = os.getenv('TEST_DB_URL').replace('postgresql', 'postgresql+psycopg2')
#prod env
else:
	DB_URL = 'sqlite:///:memory:'

if os.getenv('SKYSPARK_KEY'):
	KEY = os.getenv('SKYSPARK_KEY')
else:
    KEY = '123'

dal = DataAccessLayer()

def main():
    dal.connect()
    session = dal.Session()
    last_update = fetch_last_update(session)
    session.commit()
    if not last_update:
        ss = SkySparkAPI(KEY)
        grouped_df = ss.json_to_parsed_df()
        session = dal.Session()
        insert_data(grouped_df, session)
        session.commit()
    else:
        pass
    
   
from db.db import DataAccessLayer
from db.db_utils import insert_data
from skyspark import SkySparkAPI
from datetime import datetime
import os
import sys

#test env
if os.getenv('TEST_DB_URL'):
	DB_URL = os.getenv('TEST_DB_URL').replace('postgresql', 'postgresql+psycopg2')
#prod env
else:
	DB_URL = 'sqlite:///:memory:'

dal = DataAccessLayer()

def main():
    date = datetime.now()
    ss = SkySparkAPI(date = date)
    file_name = ss.download_data()
    df = ss.create_data_frame(file_name)
    session = dal.Session()
    insert_data(df, session)
    session.commit()

    
   
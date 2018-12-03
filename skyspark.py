import logging
import pandas as pd
import os
import urllib.request
from contextlib import closing
import shutil
import sys

class SkySparkAPI:

    def __init__(self, date):
        self.date = str(date)
        self.ftp_url = 'ftp://ftp.skyspark.com/test/{self.date}'

    @staticmethod
    def _make_out_path(out_path):
        if not os.path.exists(out_path):
            os.makedirs(out_path)

    @staticmethod
    def _remove_file(file_name):
        os.remove(file_name)
    
    
    def download_data(self):
        '''
        Downloads a csv FTP file, converts to pandas DataFrame, then removes file.

        Returns:
            df (pandas DataFrame): the csv as a pandas DataFrame
        '''
        
        file_name = f'jck_sensor_data_{self.date}.csv'
        out_path = os.path.join(os.getcwd(), "temp")
        SkySparkAPI._make_out_path(out_path)
        
        try:
            with closing(urllib.request.urlopen(self.ftp_url)) as r:
                file_name = os.path.join(out_path,file_name)
                with open(file_name, 'wb') as f:
                    shutil.copyfileobj(r, f)
        except Exception as err:
            logging.critical(f"Exception occurred trying to access {self.ftp_url}:  \
                             {err}", exc_info=True)
            sys.exit(1)
                
        SkySparkAPI._remove_file(file_name)
        
        return file_name
    
    
    def create_data_frame(self, file_name):
        df = pd.read_csv(file_name)
        #TODO add logic to group the df, arriving at columns for timestamp, building_name, 
        # location, modality_indicator, unit, value
        return df

    
    
        
        

                

        
    


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
                file_name = os.path.join(out_path, file_name)
                with open(file_name, 'wb') as f:
                    shutil.copyfileobj(r, f)
        except Exception as err:
            logging.critical(f"Exception occurred trying to access {self.ftp_url}:  \
                             {err}", exc_info=True)
            sys.exit(1)
                
        SkySparkAPI._remove_file(file_name)
        
        return file_name
    
    @staticmethod
    def _extract_digits(value):
        '''
        Extracts digits from the values of a pandas Series
        '''
        value_str = str(value)
        value = "".join(s for s in value_str if s.isdigit() or s == '.')
        measure = "".join(s for s in value_str if s not in value)
        if len(value) > 0:
            value = float(value)
        else:
            value = None
        return value, measure
    
    @staticmethod
    def _parse_col_name(col_name):
        '''
        Extracts data from a pandas Series
        '''
        col_name = str(col_name)
        col_name_split = col_name.split("-")
        building = col_name_split[0]
        floor = "".join(s for s in col_name_split[1] if s.isdigit())
        room = col_name_split[2]
        room_number = "".join(s for s in room if s.isdigit())
        room_type = "".join(s for s in room if s not in room_number)
        modality = col_name_split[3]
        
        return building, floor, room_type, room_number, modality
    
    
    def create_data_frame(self, file_name):
        '''
        Reads in the csv as a pandas dataframe and then melts it for simpler aggregations
        '''
        df = pd.read_csv(file_name)
        value_vars = [x for x in df.columns if x != 'Timestamp']
        df_melted = pd.melt(df, id_vars = ['Timestamp'], value_vars = value_vars)
        df_melted['Timestamp'] = pd.to_datetime(df_melted['Timestamp'].str.replace(' Chicago',''))
        df_melted[['value', 'unit']] = df_melted['value'].apply(SkySparkAPI._extract_digits).apply(pd.Series)
        df_melted['building'] = 0
        df_melted['floor'] = 0
        df_melted['room_type'] = 0
        df_melted['room_number'] = 0
        df_melted['modality'] = 0
        location_cols = df_melted['variable'].apply(SkySparkAPI._parse_col_name).apply(pd.Series)
        df_melted[['building','floor','room_type','room_number','modality']] = location_cols
        df_melted = df_melted.drop(labels='variable', axis = 1)
        
        return df_melted

    
    
        
        

                

        
    


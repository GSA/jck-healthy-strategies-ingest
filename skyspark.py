import requests
import logging
import pandas as pd
import sys
import re

class SkySparkAPI:

    def __init__(self, key, last_update=None):
        self.key = key
        self.last_update = last_update
        key_param = f'&api_key={key}'
        if last_update:
            last_update_param = f'&last_update={last_update}'
        else:
            last_update_param = ''
        self.uri = 'https://test.api.com/api/v1/data'+ key_param + last_update_param


    @staticmethod
    def get_data(uri):
        try:
            r = requests.get(uri)
        except Exception as e:
            logging.critical(f"Exception occurred making GET request to {uri}:  \
                             {e}", exc_info=True)
            sys.exit(1)
        if r.status_code != 200:
            logging.critical(f"Non-200 GET request made to {uri}", exc_info=True)
            sys.exit(1)
        else:
            data = r.json()
            
            return data

    
    def json_to_parsed_df(self):
        data = SkySparkAPI.get_data(self.uri)
        cols = data['cols']
        rows = data['rows']
        digit_re = re.compile(r'\d+\.?\d*')
        parsed_data = {}
        for row in rows:
            col_counter = 0
            for k in row:
                if col_counter == 0:
                    row_value = row[k]
                    timestamp_str = row_value.split()[0].replace("t:",'')
                    parsed_data[timestamp_str] = []
                else:
                    values_dict = {}
                    row_value = row[k]
                    value = digit_re.findall(row_value)[0]
                    if len(value) == 0:
                        value = None
                    values_dict['value'] = float(value)
                    column = cols[col_counter]
                    for col_elem in column:
                        col_value = column[col_elem]
                        values_dict[col_elem] = col_value
                    parsed_data[timestamp_str].append(values_dict)
                col_counter += 1
        
        dfs = []
        for timestamp in parsed_data:
            _df = pd.DataFrame(parsed_data[timestamp])
            _df['timestamp'] = timestamp
            _df = _df.set_index('timestamp')
            dfs.append(_df)
        cols_to_keep = ['timestamp','equipRef','groupRef','hisEnd','hisRollup', 'hisRollupDis', 
                        'hisRollupInterval', 'indicator', 'navName','regionRef','siteRef','unit',
                        'value']    
        df = pd.concat(dfs)
        df = df.reset_index()
        df = df[cols_to_keep]
        df['modality_indicator'] = df['navName'] + "-" + df['indicator']
        df['building_name'] = df['groupRef'].str.split(' ',1).apply(lambda x: x[1])
        df['location'] = df['equipRef'].str.split(' ',1).apply(lambda x: x[1])
        building_names = df['building_name'].unique()
        locations = df['location'].tolist()
        clean_locations = []
        for location in locations:
            for building_name in building_names:
                clean_location = location.replace(building_name,'').strip()
                clean_locations.append(clean_location)
        df['location'] = clean_locations

        # use mean to agg as there should only be one value per timestamp
        # and if there isn't then we mind as well average
        grouped_df = pd.DataFrame(df.groupby(by = ['building_name', 
                                                   'location', 
                                                   'modality_indicator', 
                                                   'unit', 
                                                   'timestamp'])['value'].mean()).reset_index()
        
        return grouped_df

                

        
    


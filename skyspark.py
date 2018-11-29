import requests
import logging
import pandas as pd
import sys
import re

class SkySparkAPI:

    def __init__(self, key, last_update):
        self.key = key
        self.last_update = last_update
        key_param = f'&api_key={key}'
        last_update_param = f'&last_update={last_update}'
        self.uri = f'https://test.api.com/api/v1/data'+ key_param + last_update_param


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
            for i, k in enumerate(row):
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
                    values_dict['value'] = value
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
        cols_to_keep = ['equipRef','groupRef','hisEnd','hisRollup', 'hisRollupDis', 'hisRollupInterval', 
                        'indicator', 'navName','regionRef','siteRef','unit']    
        df = pd.concat(dfs)[cols_to_keep]
        df['modality_indicator'] = df['navName'] + "-" + df['indicator']
        
        return df

                

        
    


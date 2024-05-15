# %%


import requests
import datetime
import time
import json
import pandas as pd

# %%

class Collector:
    def __init__(self, url, instance_name):
        self.url = url
        self.instance_name = instance_name

    def get_content(self, **kwargs):
        resp = requests.get(self.url, params=kwargs)
        return resp

    def save_parquet(self, data):
        now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.%f')
        df = pd.DataFrame(data)
        df.to_parquet('./data/{self.instance_name}/parquet/{}.parquet'.format(now), index=False)

    def save_json(self, data):
        now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.%f')
        with open(f'./data/{self.instance_name}/json/{now}.json', 'w') as open_file:
            json.dump(data, open_file, indent=4)

    def save_data(self, data, format='json'):
        if format == 'json':    self.save_json(data)
        if format == 'parquet': self.save_parquet(data)

    def get_and_save(self, save_format='json', **kwargs):
        resp = self.get_content(**kwargs)

        if resp.status_code == 200:
            self.save_data(resp.json(), save_format)
            data = resp.json()
        else:
            print(f'Unsuccessfull request: {resp.status_code}', resp.json())
            data = None
        return data
    
    def auto_exec(self, save_format='json', stop_date='2000-01-01'):
        page = 1
        while True:
            print(page)
            data = self.get_and_save(save_format=save_format, 
                                     page = page, per_page=1000)
            
            if data == None : 
                print('Error in data collection... waiting...')
                time.sleep(60 * 5)
            else : 
                last_date = pd.to_datetime(data[-1]['published_at']).date()
                if last_date < pd.to_datetime(stop_date).date():
                    break
                elif len(data)<1000:
                    break
                time.sleep(5)
        
                page += 1

# %%
url = 'https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/'
collector = Collector(url, 'episodes')

# collector.get_content().json()

collector.auto_exec()
# %%

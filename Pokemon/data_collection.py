#%%

import requests
import datetime
import json

class Collector:
    def __init__(self, url):
        self.url = url
        self.instance = url.strip('/').split('/')[-1]

    def get_endpoint(self, **kwargs):
        # url = 'https://pokeapi.co/api/v2/pokemon/'
        resp = requests.get(self.url, params=kwargs)
        return resp
    
    def save_data(self, data):
        now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.%f')
        data['ingestion_date'] = now
        filename = f'/dbfs/mnt/datalake/pokemon/{self.instance}/{now}.json'
        with open(filename, 'w') as open_file:
            json.dump(data, open_file)

    def get_and_save(self, **kwargs):
        resp = self.get_endpoint(**kwargs)
        if resp.status_code == 200:
            data = resp.json()
            self.save_data(data)
            return data
        else:
            return {}

    def auto_exec(self, limit=100):
        offset = 0
        while True:
            print(offset)
            data = self.get_and_save(limit=limit, offset=offset)
            if data['next'] == None:
                break
            offset += limit

# %%
# dbutils.fs.mkdirs('/mnt/datalake/Pokemon/pokemon')
url = 'https://pokeapi.co/api/v2/pokemon/'

collector = Collector(url)
collector.auto_exec()

# %%

# dbutils.fs.ls('/mnt/datalake/Pokemon/pokemon')
# df = spark.read.json('/mnt/datalake/Pokemon/pokemon')
# df.display()
# df.createOrReplaceTempView('pokemon')

# %sql
# SELECT *
# FROM pokemon # each line is a file

# SELECT count,
        # ingestion_date,
        # explode(results) as pokemon
# FROM pokemon # each line is a pokemon

# SELECT 
        # ingestion_date,
        # poke.* # "un-normalise" the dict/table, i.e., create new columns
# FROM pokemon AS t1 # each line is a pokemon
# LATERAL VIEW explode(results) AS poke
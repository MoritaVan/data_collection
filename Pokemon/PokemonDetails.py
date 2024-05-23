import requests
import datetime
import json
from multiprocessing import Pool

urls = (spark.table('bronze.pokemon.pokemon')
                .select('url')
                .distinct()
                .toPands()['url']
                .tolist())

url = 'https://pokeapi.co/api/v2/pokemon/1/'


def save_pokemon(data):
    now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.%f')
    data['date_ingestion'] = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.%f')
    filename = f'/dbfs/mnt/datalake/Pokemon/pokemon_details/{data['id']}_{now}.json'
    with open(filename, 'w') as open_file:
        json.dump(data, open_file)

def get_and_save(url):
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        save_pokemon(data)
    else:
        print('Error in getting data from url')

#%%
# doing it for one pokemon:
# get_and_save(url)
# df = spark.read.json('/mnt/datalake/Pokemon/pokemon_details')
# df.display()

#%%
# doing in parallel for all urls:
with Pool(5) as p: # 5 threads
    p.map(get_and_save, urls)

# %%

import requests
import datetime
import json
import time
import pandas as pd
# %%

def get_response(**kwargs):
    url = 'https://www.tabnews.com.br/api/v1/contents/'
    resp = requests.get(url, params=kwargs)

    return resp

def save_data(data, option='json'):
    now=datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.%f' )
    if option=='json':
        with open(f'./data/contents/json/{now}.json', 'w') as open_file:
            json.dump(data, open_file, indent=4)

    elif option=='dataframe':
        df = pd.DataFrame(data)
        df.to_parquet('./data/contents/parquet/{}.parquet'.format(now), index=False)

# %%
# getting all data
page = 1
while True:
    print(page)
    resp = get_response(page=page, per_page=100, strategy='new')
    if resp.status_code == 200:
        data = resp.json()
        save_data(data)
        # save_data(data, 'dataframe')

        if len(data) < 100: # last page
            break

        page += 1
        time.sleep(1)
    else: # probably hit the limit of calls to the API
        print(resp.status_code)
        print(resp.json())
        time.sleep(60 * 5)

# %%
# getting data of the day
page = 1
date_stop = pd.to_datetime('2024-05-01').date()
while True:
    print(page)
    resp = get_response(page=page, per_page=100, strategy='new')
    if resp.status_code == 200:
        data = resp.json()
        save_data(data)
        # save_data(data, 'dataframe')

        date = pd.to_datetime(data[-1]['updated_at']).date()

        if (len(data) < 100) or (date < date_stop): # last page or until date
            break

        page += 1
        time.sleep(1)
    else: # probably hit the limit of calls to the API
        print(resp.status_code)
        print(resp.json())
        time.sleep(60 * 10) # 10 min
# %%
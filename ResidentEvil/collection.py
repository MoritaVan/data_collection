# %%

import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

# %%
# cannot directly load, so go to website -> inspect -> network
# first folder accessed: copy as curl (bash)
# google curl converter to python

# url = 'https://www.residentevildatabase.com/ada-wong/'
def get_content(url):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.7',
        'referer': 'https://www.residentevildatabase.com/personagens/',
        'sec-ch-ua': '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'sec-gpc': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }

    return requests.get(url, headers=headers)


def get_basicInfo(soup):
    div_page = soup.find('div', class_='td-page-content')
    paragraph = div_page.find_all('p')[1]
    emphasis = paragraph.find_all('em')

    data = {}
    for em in emphasis:
        key, value, *_ = em.text.split(':')
        data[key.strip(' ')] = value.strip(' ')

    return data

def get_apparitions(soup):
    list = (soup.find('div', class_='td-page-content')
                .find('h4')
                .findNext()
                .find_all('li'))

    return [x.text for x in list]

def get_characterInfo(url):
    resp = get_content(url)

    if resp.status_code != 200:
        print('Error! could not load the page content')
        return {}
    else:
        soup = BeautifulSoup(resp.text)
        data = get_basicInfo(soup)
        data['Aparicoes'] = get_apparitions(soup)
        return data
    

def get_links():
    url = 'https://www.residentevildatabase.com/personagens/'
    headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.7',
                'referer': 'https://www.residentevildatabase.com/personagens/ada-wong/',
                'sec-ch-ua': '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'sec-gpc': '1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            }
    resp = requests.get(url, headers=headers)
    soup_charac = BeautifulSoup(resp.text)

    links = (soup_charac.find('div', class_='td-page-content')
                        .find_all('a'))

    return [i['href'] for i in links]

# %%
links = get_links()
data = []
for link in tqdm(links):
    d = get_characterInfo(link)
    d['link'] = link

    name = link.strip('/').split('/')[-1].replace('-', ' ').title()
    d['Nome'] = name

    data.append(d)

# %%
df = pd.DataFrame(data)
# df.to_csv('data_re.csv', index=False, sep=';')
df.to_parquet('data_re.parquet', index=False) # binary format
df.to_pickle('data_re.pkl') # binary format, serialised object is saved to the disk

# %%
df_new = pd.read_parquet('data_re.parquet') 
df_new

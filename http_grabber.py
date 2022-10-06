from functools import reduce
from logging import basicConfig, debug, info, warn, error, DEBUG, INFO, WARN, ERROR
import pandas as pd
from requests import get
from pathlib import Path
from pprint import pprint, pformat
from typing import List
from urllib.parse import parse_qs, parse_qsl, urlparse
from collections import namedtuple

'''
All functions neccesairy to get data from discomap.eea.eu:
Build url for request
Mkdir
Download and save request

'''

def make_filename_from_query_url(url:str, as_path:bool=False, cut_off:int=3):
    x=parse_qsl(urlparse(str(url)).query)
    if as_path:
        return Path(*[y[1] for y in x[0:3]])
    else:
        s=[y[1] for y in x[0:cut_off]]
        return reduce(lambda a, b : a+ "_" +str(b), s)

def my_valid_filename(url:str, max_length:int=None, substitute_str:str='+'):
     return str( url.replace('.','.') \
                .replace('?',substitute_str) \
                .split('/')[-1][0:max_length] \
                + '')

def download_file_chunked(url, folder:Path=''):
    
    mksubdir(folder)
    local_filename = Path(folder) / my_valid_filename(url)
    if local_filename.is_file():
        info(f'already exists: {local_filename}')
    else:
        info(f'downloading and saving to {local_filename}')
        # NOTE the stream=True parameter below
        with get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*100): 
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    #if chunk: 
                    f.write(chunk)
                    print('.',end='')
                print('')
    return local_filename

def mksubdir(my_dir:Path):
    try:
        #Path('/tmp/sub1/sub2')
        Path(my_dir).mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        debug(f"Folder >{my_dir}< is already there")
    else:
        info(f"Folder >{my_dir}< was created")
    finally:
        pass #return?

def build_discomap_urls(country_code:str='AT', city_name:str='Wien', pollutants:List[int]=[8,38,9]):
    urls = list()
    for pollutant in pollutants:
        url =   'https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw?' \
                f'CountryCode={country_code}&CityName={city_name}&Pollutant={pollutant}&Year_from=2013&Year_to=2022' \
                '&Station=&Samplingpoint=&Source=All&Output=TEXT&UpdateDate=&TimeCoverage=Year'
        urls.append(url)
    return urls

def download_bulk_from_txt(file:Path, my_path:Path='csvs', my_encoding:str='utf-8-sig'):
    # removing the new line characters
    if file.is_file():
        with open(file, encoding=my_encoding) as f:
            lines = [line.rstrip() for line in f]
            for line in lines:
                download_file_chunked(line, my_path)
            debug('files downloaded...\r\n'+pformat(lines))
    else:
        error(f'trying to open file that does not exist: {file}')

def main():
    basicConfig(level=INFO)
    info('Started')
    
    datadir = Path('data')
    country = 'CH'
    city = 'Basel'
    urls = build_discomap_urls(country, city)
    info(pformat(urls))

    for url in urls:
        entry = download_file_chunked(url, datadir / 'src')
        download_bulk_from_txt(entry, datadir/make_filename_from_query_url(url,True))
    
    info('Finished')

    my_path= Path().absolute() / datadir / country / city / '8'
    info(my_path)
    all_files = sorted( my_path.glob('**/*.csv'))


    #pprint(all_files)

    fields = ['Concentration', 'DatetimeBegin', 'DatetimeEnd']
    parse_dates = fields[1:2]
    df_from_each_file = (pd.read_csv(f, sep=",", usecols=fields, parse_dates=parse_dates, encoding="utf-8") for f in all_files)
    concatenated_df   = pd.concat(df_from_each_file, ignore_index=True)

    pprint(concatenated_df)
def main2():
    basicConfig(level=INFO)
    
        
if __name__ == '__main__':
    main()
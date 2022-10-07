from functools import reduce
from logging import basicConfig, debug, info, warn, error, DEBUG, INFO, WARN, ERROR
import sqlite3
from matplotlib import pyplot as plt
import pandas as pd
from requests import get
from pathlib import Path
from pprint import pprint, pformat
from typing import List, Dict
from urllib.parse import parse_qs, parse_qsl, urlparse
from collections import namedtuple
from sqlalchemy import create_engine

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

def export_df_to_sqlite_db(df:pd.core.frame.DataFrame, db_name:str='default', folder:Path=Path('data')/'dbs'):
    # Einlesen in eine SQLITE DB
    try:
        # Erstellen einer SQLITE-DB im SQLITE-DB Verzeichnis
        mksubdir(folder)
        db_file_loc=str(str(Path(folder).absolute() / str(db_name+'.db')))
        info(db_file_loc)
        cnx = sqlite3.connect(db_file_loc)
        # Schreiben der Daten
        df.to_sql(name=db_name, con=cnx, if_exists="replace")
        
    except sqlite3.Error as e:
        error(f"Error...{e}")
        return False
        
    finally:
        if 'cnx' in locals():
            if cnx:
                cnx.close()
                print("SQLite Connection Closed!")
                return True

def import_df_from_sqlite_db(db_name:str='default', folder:Path=Path('data')/'dbs') -> pd.core.frame.DataFrame:
    db_file_loc = str('sqlite:///'+str(Path(folder).absolute() / str(db_name+'.db')))
    info(db_file_loc)
    engine = create_engine(db_file_loc)
    promt = str('select * from '+db_name)
    info(promt)
    return pd.read_sql(promt, engine)

def all_csv_in_to_df(all_files, usecols:List[str], dtypes, parse_dates:List[str]):
    df_from_each_file = (pd.read_csv(f, sep=",", usecols=usecols, dtype=dtypes, parse_dates=parse_dates, encoding="utf-8") for f in all_files)
    concatenated_df   = pd.concat(df_from_each_file, ignore_index=True)
    #concatenated_df['diff'] = concatenated_df['DatetimeEnd'] - concatenated_df['DatetimeBegin']
    #concatenated_df = concatenated_df.drop('DatetimeEnd', axis=1)
    return concatenated_df

def main():
    basicConfig(level=INFO)
    info('Started')
    
    datadir = Path('data')
    country = 'CH'
    city = 'Basel'
    urls = build_discomap_urls(country, city)
    info(pformat(urls))

    for url in urls:
        text_with_csv_urls = download_file_chunked(url, datadir / 'src')
        download_bulk_from_txt(text_with_csv_urls, datadir/make_filename_from_query_url(url,True))
    
    info('Finished')

    my_path= Path().absolute() / datadir / country / city / '8'
    info(my_path)
    all_files = sorted( my_path.glob('**/*.csv'))

    #pprint(all_files)
    fields = ['DatetimeBegin', 'Concentration']
    parse_dates = fields[::2]
    print(parse_dates)
    dtypes = {'DatetimeBegin': 'str', 'Concentration': 'float', 'DatetimeEnd': 'str'}
    concat_csv_file = datadir / 'concat' / 'all.csv'
    db_folder = Path('data') / 'dbs'
    db = 'NO2'

    my_df = None
    if not concat_csv_file.is_file():
        mksubdir(concat_csv_file.parent)
        info(f'not found {concat_csv_file}... generating it')
        my_df = all_csv_in_to_df(all_files, fields, dtypes, parse_dates)
        my_df.to_csv(concat_csv_file, sep=',', encoding='utf-8', index=False)
    elif not (db_folder / str(db+'.db')).is_file():
        info(f'not found db:{db} but found {concat_csv_file}... loading it')
        my_df = pd.read_csv(concat_csv_file, sep=",", usecols=fields, dtype=dtypes, parse_dates=parse_dates, encoding="utf-8")

    
    if not (db_folder / str(db+'.db')).is_file():
        info(f'db not found {db}')
        my_df.set_index(['DatetimeBegin'],inplace=True)
        my_df = my_df.apply(pd.to_numeric, errors='coerce')
        my_df.interpolate(method='linear', limit_direction='forward', axis=1)
        my_df.info()
        if (export_df_to_sqlite_db(my_df, 'NO2')):
            my_df = import_df_from_sqlite_db('NO2')
    else:
        info(f'db found {db}, loading...')
        my_df = import_df_from_sqlite_db('NO2')
    
    my_df.set_index(['DatetimeBegin'],inplace=True)
    my_df.sort_index(inplace=True)
    #concatenated_df.plot(concatenated_df["DatetimeBegin"], concatenated_df["Concentration"])
    my_df.interpolate(method='linear', limit_direction='forward', axis=0, inplace=True)
    my_df.info()
    my_df.plot()
    plt.show()

    #fig, ax1 = plt.subplots()
    #ax2 = ax1.twinx()
    #h1, = ax1.plot(concatenated_df["DatetimeBegin"], concatenated_df["Concentration"], color='r', label='avg ppm')
    #h2, = ax2.plot(co2["decimal"], co2["increase since 1800"], color='g', label='+ppm since 1800')
    #plt.title('Average ppm CO2 in Atmosphere 1974 - 2022');
    #plt.grid(True);
    #plt.legend(handles=[h1, h2])
    #plt.legend(handles=[h1])
    #ax1.set_xlabel('Year')
    #ax1.set_ylabel('CO2 [ppm]')
    #ax2.set_ylabel('CO2 [ppm] increase since 1800')


def main2():
    basicConfig(level=INFO)
    
        
if __name__ == '__main__':
    main()
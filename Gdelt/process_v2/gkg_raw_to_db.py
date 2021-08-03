# Processes raw GKG files and uploads to Postgres database

# raw GKG file headers:
# [
#     'gkg_id', 'date', 'source', 'source_name', 'doc_id', 'v1counts',
#     'v2counts', 'v1themes', 'v2themes', 'v1locations', 'v2locations',
#     'v1persons', 'v2persons', 'v1org', 'v2org', 'tone', 'mention_dates',
#     'gcam', 'image1', 'image2', 'image3', 'video1', 'allnames',
#     'amounts', 'translation', 'extra'
# ]

import io, zipfile
import re
import concurrent.futures

from datetime import datetime
from operator import itemgetter
from itertools import repeat
from collections import namedtuple

import requests
import psycopg2
from psycopg2 import extras

import postgres_config

io.DEFAULT_BUFFER_SIZE = 8192*4
DB_TABLE = "gdelt_raw"
CONNECTION_DETAILS = (
    f"host={postgres_config.HOST} "
    "dbname=gdelt "
    "user=postgres "
    f"password={postgres_config.PASSWORD}"
)


def check_dt_in_db(datetime):
    """Returns boolean indicating whether the database contains any rows with 
    the given datetime (format=YYYYmmddHHMMSS; 14-long bigint)
    """
    exists_query = f"""
    SELECT exists 
    (SELECT 1 FROM gdelt_raw WHERE datetime = '{datetime}' LIMIT 1)
    """
    with psycopg2.connect(CONNECTION_DETAILS) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(exists_query)
                dt_in_db = cur.fetchone()[0]
            except Exception as e:
                print(f"Exception executing Select Query: {e}")
                print(f"Exception type: {type(e)}")
    
    return dt_in_db


def get_loc_item(loc_item_str):
    loc_dtypes = [int, str, str, str, float, float, str]
    parts = loc_item_str.split('#')
    converted_parts = [
        new_dtype(item) if item != '' else None 
        for item, new_dtype in zip(parts, loc_dtypes)
    ]
    loc_item = Loc_Item(*converted_parts)
    return loc_item


def get_gkg_files(update_master_list=True):
    master_list_fpath = '../data/gdelt2_master.txt'
    
    if update_master_list:
        master_list = 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
        r = requests.get(master_list, stream=True)
        with open(master_list_fpath, mode='wb') as localfile:
            for line in r.iter_lines():
                if b'gkg.csv.zip' in line:
                    localfile.write(line + b'\n')
                    
    with open(master_list_fpath) as f:
        gkg_files = [line.split(' ')[2][:-1] for line in f]
                    
    # flip order, so goes from most recent to least recent
    return gkg_files[::-1]


def write_processed_to_db(processed):
    num_columns = len(processed[0])
    insert_query = (
        "INSERT INTO gdelt_raw VALUES " +
        ','.join(['%s'] * len(processed))
    )
    with psycopg2.connect(CONNECTION_DETAILS) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            extras.register_composite('location_item', cur)
            try:
                cur.execute(insert_query, processed)
            except Exception as e:
                print(f"Exception executing Insert Query: {e}")
                print(f"Exception type: {type(e)}")
        

def process_gkg(file_url, countries_of_interest):
    """
    Returns
    - True if processed and uploaded to DB successfully
    - False if contents of raw file is already in DB
    - None if Exception thrown
    """
    codes = [
        'c3.1', 'c3.2',
        *sorted(['c4.' + str(i) for i in range(1, 29)]),
        'c41.1', 'c41.2', 'c41.3', 
        'c6.4', 'c6.5', 'c6.6', 
        'c7.1', 'c7.2', 
        'v10.1', 'v10.2', 'v11.1'
    ]
    codes_l = len(codes)
    
    processed = []
    filename = file_url.split('/')[-1][:-4]
    file_datetime = datetime.strptime(
        re.match(r'(\d{14}).gkg.csv', filename).group(1),
        '%Y%m%d%H%M%S'
    )
    if check_dt_in_db(file_datetime):
        # database already has rows with datetime matching this raw GKG file
        return False
    print(filename)
    r = requests.get(file_url, stream=True)
    
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            with zf.open(filename, 'r') as infile:
                for raw in io.TextIOWrapper(infile, encoding='latin-1'):
                    line = raw.split('\t')
                    if len(line) < 10: continue
    
                    # Keep stories which reference any *country of interest*
                    locs = line[9].split(';')
                    inc_nz = False
                    if locs == ['']: 
                        continue  # no locations detected by gdelt
                    
                    relevant_codes = []
                    for loc in locs:
                        country_code = loc.split('#')[3]
                        if country_code in countries_of_interest:
                            relevant_codes.append(country_code)
                    if len(relevant_codes) == 0: 
                        continue  # no countries of interest mentioned
    
                    # Extract the relevant codes
                    gcam = line[17].split(',')
                    code_i = 0
                    code_v = codes[code_i]
                    code_l = len(code_v)
                    out = []
                    # skip gcam[0] as is word count, not gcam code
                    for el in gcam[1:]:
                        gcode, val = el.split(':')
    
                        while code_v < gcode:
                            out.append(None)
                            code_i += 1
                            if (code_i == codes_l - 1): break
                            code_v = codes[code_i]
    
                        if code_v == gcode:
                            out.append(val)
                            code_i += 1
                            if (code_i == codes_l - 1): break
                            code_v = codes[code_i]
    
                    if (len(codes) == len(out) + 1): 
                        out.append(None)
    
                    # Construct row for database table
                    row = [
                        *itemgetter(0, 1, 2, 3, 4, 7, 9, 11, 13)(line),  # article info.
                        relevant_codes,                                  # countries
                        *line[15].split(','),                            # V1.5TONE
                        *out                                             # V2GCAM codes
                    ]
                    # Reshaping fields into appropriate data types
                    row[1] = datetime.strptime(row[1], '%Y%m%d%H%M%S')
                    row[5] = [x for x in row[5].split(';') if x] if row[5] else []
                    row[6] = [
                        get_loc_item(loc) for loc in row[6].split(';')
                    ] if row[6] else []
                    row[7] = [x for x in row[7].split(';') if x] if row[7] else []
                    row[8] = [x for x in row[8].split(';') if x] if row[8] else []
                    # Convert 'row' from list to tuple for psycopg2 function
                    processed.append(tuple(row))
                
                if len(processed) > 0:
                    write_processed_to_db(processed)
                    
        return True
    except:
        return None
    

if __name__ == '__main__':
    countries_of_interest = ['NZ', 'AS', 'CA', 'UK']
    
    # Construct namedtuple that will be cast into location_item for database
    Loc_Item = namedtuple(
        'location_item',
        'type full_name country_code ADM1_code lat long feature_id'
    )
    class Loc_Item_Adapter:
        def __init__(self, x):
            self.adapted = psycopg2.extensions.SQL_IN(x)
        def prepare(self, conn):
            self.adapted.prepare(conn)
        def getquoted(self):
            return self.adapted.getquoted() + b'::location_item'
    psycopg2.extensions.register_adapter(Loc_Item, Loc_Item_Adapter)
    
    gkg_files = get_gkg_files()
    print(f"Processing up to {len(gkg_files)} files.")
    
    start = datetime.now()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        processed = list(
            executor.map(process_gkg, gkg_files, repeat(countries_of_interest))
        )
    num_processed = sum([x for x in processed if x])  # filter out None
    print(f"Actually processed {sum(processed)} files. Time taken:", 
          datetime.now() - start)

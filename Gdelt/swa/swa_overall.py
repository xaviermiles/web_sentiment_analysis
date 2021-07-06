import itertools
from operator import itemgetter
import requests, zipfile, io, csv
# from pathos.pools import ProcessPool
import random
import os.path
import concurrent.futures
from functools import partial
import boto3
import botocore
import s3fs
import pandas as pd
import glob
import numpy as np
from datetime import datetime

def read_from_s3(filename,c):
    
    session = boto3.Session(profile_name='kandavar_processing')
    s3 = session.client('s3')
    bucket_name = 'statsnz-covid-kandavar'
    
    
    obj = s3.get_object(Bucket = bucket_name, Key = 'G_from_2015/merged/'+filename)
    

    d = pd.read_csv(obj['Body'], parse_dates = ['date'], usecols = [1,3,5,9])

     
    d['date'] = pd.to_datetime(d['date'], errors='coerce')
#     d = d.loc[d.date >=datetime(2015,3,1)]
    d['date'] = d.date.dt.date

    d['source_name'].dropna(inplace=True)
    d['themes'].dropna(inplace=True)
    d['source_name'] = [x if f'.{c}' in str(x) else None for x in d.source_name]
    d['tone_about_c']=[d.tone[x] if i == None else np.NAN for x,i in enumerate(d.source_name)]
    d['tone_from_c']=[d.tone[x] if i != None else np.NAN for x,i in enumerate(d.source_name)]
    d.drop(['themes'], axis =1, inplace = True)

    d['source_from_c']=[i if i != None else np.NAN for x,i in enumerate(d.source_name)]
    
    d['source_about_c']=['Other' if i == None else np.NAN for x,i in enumerate(d.source_name)]
    
    
    print(f"Read - {filename}")
#     print(d.info())
    return d

def process(d,c):
    d['date'] = pd.to_datetime(d['date'], errors='coerce')

    d1 = d.resample('M', on = 'date').agg({'tone_about_c': 'mean','tone_from_c':'mean', 'source_from_c':'count', 'source_about_c':'count'}).reset_index()        # average 
    d1['country'] = c

    d2 = d.groupby(['date'], as_index=False).agg({'tone_about_c': 'mean','tone_from_c':'mean', 'source_from_c':'count', 'source_about_c':'count'}).reset_index(drop = True)     # rolling mean
    d2['rolling_7_from_c'] = d2['tone_from_c'].rolling(7).mean()
    d2['rolling_7_about_c'] = d2['tone_about_c'].rolling(7).mean()
    d2['country'] = c
    
    d3 = d.resample('M', on = 'date').agg({'tone_about_c': np.std,'tone_from_c':np.std, 'source_from_c':'count', 'source_about_c':'count'}).reset_index()    # std
    d3['country'] = c    
    
 
    d4 = d.groupby(['date'], as_index=False).agg({'tone_about_c': np.std,'tone_from_c':np.std, 'source_from_c':'count', 'source_about_c':'count'}).reset_index(drop = True)   # std-rolling
    d4['rolling_7_from_c'] = d4['tone_from_c'].rolling(7).std()
    d4['rolling_7_about_c'] = d4['tone_about_c'].rolling(7).std()
    d4['country'] = c
    
    
    
    return d1,d2,d3,d4 

def upload_to_s3(d1,d2,d3,d4,c,y,i):
    s3 = s3fs.core.S3FileSystem(anon=False, profile='kandavar_processing')
    if i == 0:
        with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_insights/swa_monthly_{c}_{y}.csv','w') as f:              # stored seperately for each country year wise
            d1.to_csv(f, index=False)
        with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_insights/swa_rolling_{c}_{y}.csv','w') as f:
            d2.to_csv(f, index=False)
        with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_insights/swa_std_monthly_{c}_{y}.csv','w') as f:
            d3.to_csv(f, index=False)
        with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_insights/swa_std_rolling_{c}_{y}.csv','w') as f:
            d4.to_csv(f, index=False)
    else:                                                                                                                      # final merging for each of the methods
        with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_insights/merged/swa_monthly.csv','w') as f:                
            d1.to_csv(f, index=False)        
        with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_insights/merged/swa_rolling.csv','w') as f:
            d2.to_csv(f, index=False)
        with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_insights/merged/swa_monthly_std.csv','w') as f:
            d3.to_csv(f, index=False)        
        with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_insights/merged/swa_rolling_std.csv','w') as f:
            d4.to_csv(f, index=False)   
    print("uploaded")
    
    
    
    
    
    
if __name__ == '__main__':
    
    Year = [i for i in range(2015, 2022)]

    from swa_monthly_final import keyss,merge2                                   # importing this from swa_monthly_final script
    
    for c in ['nz','ca','uk','au']:
        for y in Year:
            d = read_from_s3(f'gdelt_{c}_{y}.csv', c)
            d1,d2,d3,d4 = process(d,c) 
            del d
            upload_to_s3(d1,d2,d3,d4,c,y,0)
            del d1,d2,d3,d4

    keys = keyss('swa_insights','swa_monthly')
    d1 = merge2(keys)
    keys = keyss('swa_insights','swa_rolling')
    d2 = merge2(keys)
    keys = keyss('swa_insights','swa_std_monthly')
    d3 = merge2(keys)
    keys = keyss('swa_insights','swa_std_rolling')
    d4 = merge2(keys)
    upload_to_s3(d1,d2,d3,d4,0,0,1)
    del d1,d2,d3,d4
    
    
print('Finished')
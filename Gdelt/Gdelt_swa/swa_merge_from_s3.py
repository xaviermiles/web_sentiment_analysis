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
import time
col = ['gkg_id', 'date', 'source', 'source_name', 'doc_id', 
        'themes', 'locations', 'persons', 'orgs', 
        'tone', 'pos', 'neg', 'polarity', 'ard', 'srd',
        'wc', 
        'lexicode_neg', 'lexicode_pos'
]

Year = [i for i in range(2015, 2022)]
country = ['au', 'nz', 'ca','uk']
session = boto3.Session(profile_name='kandavar_processing')
s3 = session.client('s3')
bucket_name = 'statsnz-covid-kandavar'


def get_all_s3_objects(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')

def keyss(c, y):
    keys=[]
    for file in get_all_s3_objects(s3, Bucket=bucket_name, Prefix=f'G_from_2015/{c}/'):
        if file['Key'].split('/')[-1][:4] == f'{y}':
            n = "s3://" + bucket_name + "/"+ file['Key']
            keys.append(n)
    
    print(len(keys))
    return keys



def merge2(keys):
    s3 = s3fs.core.S3FileSystem(anon=False, profile='kandavar_processing')# session=session)
#     data = pd.concat((pd.read_csv((s3.open(k)), header=None) for k in keys))
    def print_and_return_df(key):
        print(key.split('/')[-1])
        return pd.read_csv((s3.open(key)), header=None, usecols=[*range(0,18)])
    
    data = pd.concat((print_and_return_df(k) for k in keys))
    data.columns = col
    
    return data


def upload_to_s3(data,c,y):
    s3 = s3fs.core.S3FileSystem(anon=False, profile='kandavar_processing')
    with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/merged/gdelt_{c}_{y}.csv','w') as f:
        data.to_csv(f, index=False)
    
    del data
    print("uploaded")
    
if __name__ == '__main__':
    
    for c in country:
        for y in Year:
            start = time.time()

            keys = keyss(c, y)

            data = merge2(keys)

            end = time.time()
            print(f"data merged for the year: {y} which took {round(end-start,2)} seconds")
            upload_to_s3(data,c,y)

print('finished')

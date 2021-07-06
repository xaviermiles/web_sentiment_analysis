import boto3
import botocore
import s3fs
import pandas as pd
import glob
import time

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

def keyss():
    keys=[]
    for file in get_all_s3_objects(s3, Bucket=bucket_name, Prefix=f'G_from_2015/monthly/m_df'):
#         print(file['Key'])
#         if file['Key'].split('/')[-1][5:9] == f'{y}':
        n = "s3://" + bucket_name + "/"+ file['Key']
#             print(n)
        keys.append(n)
    
    print(len(keys))
    return keys



def merge2(keys):
    s3 = s3fs.core.S3FileSystem(anon=False, profile='kandavar_processing')# session=session)
#     data = pd.concat((pd.read_csv((s3.open(k)), header=None) for k in keys))
    def print_and_return_df(key):
        print(key.split('/')[-1])
        return pd.read_csv((s3.open(key)))
    
    data = pd.concat((print_and_return_df(k) for k in keys))
#     data.columns = col
    print(data.info())
    
    return data

def upload_to_s3(data):
    s3 = s3fs.core.S3FileSystem(anon=False, profile='kandavar_processing')
    with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/swa_gdelt_final_monthly.csv','w') as f:
        data.to_csv(f,index=False)

if __name__ == '__main__':
    start = time.time()
    keys = keyss()
    data = merge2(keys) # careful here :)
    
    upload_to_s3(data)
    


    end = time.time()
    print(f"All data merged - which took {round(end-start,2)} seconds")

print('finished')
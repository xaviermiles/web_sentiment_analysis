import boto3
import botocore
import s3fs
import pandas as pd

# local:
import s3_utils
import s3_config


def keyss(country, year, bucket):
    keys=[]
    for file in (s3, Bucket=bucket, Prefix=f'G_from_2015/{country}/'):
        if file['Key'].split('/')[-1][:4] == f'{y}':
            n = "s3://" + bucket_name + "/"+ file['Key']
            keys.append(n)
    
    print(len(keys))
    return keys


def merge(keys, profile):
    s3 = s3fs.core.S3FileSystem(anon=False, profile=profile)# session=session)
    def print_and_return_df(key):
        print(key.split('/')[-1])
        return pd.read_csv((s3.open(key)), header=None)
    
    data = pd.concat((print_and_return_df(k) for k in keys))
    data.columns = col
    
    return data


def upload_to_s3(data, country, year, bucket, profile):
    s3 = s3fs.core.S3FileSystem(anon=False, profile=profile)
    s3_path = f's3://{bucketed}/gdelt/merged_/{country}_{year}.csv'
    with s3.open(s3_path,'w') as f:
        data.to_csv(f, index=False)
    
    del data
    print("uploaded")
    

if __name__ == '__main__':    
    headers = [
        'gkg_id', 'date', 'source', 'source_name', 'doc_id', 
        'themes', 'locations', 'persons', 'orgs', 
        'tone', 'pos', 'neg', 'polarity', 'ard', 'srd',
        'wc', 
        'lexicode_neg', 'lexicode_pos', # c3.*
        'MACROECONOMICS', 'ENERGY', 'FISHERIES', 
        'TRANSPORTATION', 'CRIME', 'SOCIAL_WELFARE',
        'HOUSING', 'FINANCE', 'DEFENCE', 'SSTC',
        'FOREIGN_TRADE', 'CIVIL_RIGHTS', 
        'INTL_AFFAIRS', 'GOVERNMENT_OPS',
        'LAND-WATER-MANAGEMENT', 'CULTURE',
        'PROV_LOCAL', 'INTERGOVERNMENTAL',
        'CONSTITUTIONAL_NATL_UNITY', 'ABORIGINAL',
        'RELIGION', 'HEALTHCARE', 'AGRICULTURE',
        'FORESTRY', 'LABOUR', 'IMMIGRATION',
        'EDUCATION', 'ENVIRONMENT',
        'finstab_pos', 'finstab_neg', 'finstab_neutral',
        'finsent_neg', 'finsent_pos', 'finsent_unc',
        'opin_neg', 'opin_pos',
        'sent_pos', 'sent_neg', 'sent_pol'
    ]
    
    years = list(range(2015, 2022))
    countries = ['nz']
    bucket = 'statnz-covid-xmiles'
    profile = s3_config.normal_role  # normal_role or processing_role

    for country in countries:
        for year in years:
            start = time.time()
#             year_to_keys = get_year_keys
            keys = list_all_objects_s3(bucket, f"processed_gdelt_{country}", profile)
            full_keys = [f"s3://{bucket}/{key}" for key in keys]
            data = merge2(keys, profile)
            end = time.time()
            print(f"data merged for the year: {year} which took {round(end-start, 2)} seconds")
            
            upload_to_s3(data, country, year)
            print("Uploaded")

print('finished')

# Merges the CSV outputs from gkg_gdelt2_process.py

import re
import s3fs
import pandas as pd

import s3_config
import s3_utils
from Gdelt import gdelt_utils


def get_gdelt_raw_keys(country_code, year, bucket):
#     keys = [
#         key for key in s3_utils.list_all_objects_s3(
#             bucket, f"processed_gdelt_{country_code}", S3_PROFILE
#         )
#         if re.search(str(year) + r'\d{10}.gkg.csv$', key)
#     ]
    prefix = f"processed_gdelt_{country_code}/{year}"
    keys = s3_utils.list_all_objects_s3(bucket, prefix, S3_PROFILE)
    return sorted(keys)


def merge_gdelt_raw(bucket, inkeys, outkey, header):
    fs = s3fs.S3FileSystem(profile=S3_PROFILE)
    merged = pd.concat((
        pd.read_csv(fs.open(f"s3://{bucket}/{key}", header=None))
        for key in inkeys
    ))
    merged.columns = header
    
    print(merged.head())
    
    with fs.open(f"s3://{bucket}/{outkey}", 'w') as f:
        merged.to_csv(f, index=False)
    return merged


def clean_merged_gdelt_raw(merged, bucket, outkey):
    daily = merged.groupby(
        date,
        pos, neg, wc  # to try to control for syndicated articles
    )
    
    with fs.open(f"s3://{bucket}/{outkey}", 'w') as f:
        daily.to_csv(f, index=False)
    return daily
    

if __name__ == "__main__":
    S3_PROFILE = s3_config.normal_role
    raw_gdelt_headers = gdelt_utils.RAW_GDELT_HEADERS
    s3_bucket = 'statsnz-covid-xmiles'
    # Change as necessary:
    countries = [
        'nz',
#         'au',
#         'ca',
    ]
    years = [2020] #list(range(2015, 2022))
    
#     process_gdelt_raw(country, year)
    # Process (per-country, per-year) to keep the required memory lower
    for country in countries:
        print(f"Merging raw GDELT files for {country}")
        for year in years:
            print(f"{year}:", end='', flush=True)

            keys = get_gdelt_raw_keys(country, year, s3_bucket)
            print(f" {len(keys)} keys", end='', flush=True)
               
            merged_outkey = f'gdelt_merged/merged-{country}-{year}.csv'
            merged = merge_gdelt_raw(s3_bucket, keys, raw_gdelt_headers)
            print(", merged", end='', flush=True)
            
            daily_outkey = f'gdelt_daily/daily-{country}-{year}.csv'
            daily_agg_gdelt(merged, s3_bucket, daily_outkey)
            print(", daily-aggregated >>")

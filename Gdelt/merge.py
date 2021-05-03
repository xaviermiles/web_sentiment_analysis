# Merges the CSV outputs from gkg_gdelt2_process.py

import os
import re
import boto3
    

def read_file_from_s3(bucket, key):
    s3 = sess.client('s3')
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read().decode()
    
    return content


def list_all_objects_s3(bucket, prefix):
    """
    Necessary since the list_objects_v2() function only lists the first 1000 
    objects, and requires a continuation token to get the next 1000 objects.
    """
    s3 = sess.client('s3')
    keys = []
    truncated = True
    next_cont_token = ""

    while truncated:
        if next_cont_token:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, 
                                      ContinuationToken=next_cont_token)
        else:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        keys += [x['Key'] for x in resp['Contents'] 
                 if ".ipynb_checkpoints" not in x['Key']]

        truncated = resp['IsTruncated']
        if truncated:
            next_cont_token = resp['NextContinuationToken']
            
    return keys


def merge_csvs_from_s3(mergefile, inkeys, bucket, headers, overwrite=False):
    if os.path.exists(mergefile) and not overwrite:
        return
    
    # Clears existing CSV
    headers_str = ','.join(headers) + '\n'
    print(headers_str)
    with open(mergefile, 'w') as f:
        f.write(headers_str)
    
    print("Keys:")
    inkeys_l = len(inkeys)
    for i, key in enumerate(inkeys[:1]):
        print(f"{i + 1} / {inkeys_l}")
        content = read_file_from_s3(bucket, key)
        with open(mergefile, mode='a') as f:
            f.write(content)
            

def get_20_to_21_keys(country):
    keys = [key
            for key in list_all_objects_s3("statsnz-covid-xmiles", 
                                           country_to_prefix[country])
            if re.search(r'202[0-1]\d{10}.gkg.csv', key)
            ]
    return sorted(keys)
            

def get_18_to_21_keys(country):
    keys = [key
            for key in list_all_objects_s3("statsnz-covid-xmiles", 
                                           country_to_prefix[country])
            if re.search(r'(201[8-9]|202[0-1])\d{10}.gkg.csv', key)
            ]
    return sorted(keys)


if __name__ == "__main__":
    sess = boto3.Session(profile_name="xmiles")
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
    country_to_prefix = { 
        'nz': "processed_gdelt_nz/",
        'au': "processed_gdelt_au/",
        'ca': "processed_gdelt_ca/"
    }
    countries = ['nz', 'au', 'ca']
    
    # Change as needed
    COUNTRY = "nz"
    
    keys_18_21 = get_18_to_21_keys(COUNTRY)
    print(f"got {len(keys_18_21)} keys")

    merge_csvs_from_s3(f'gdelt-{COUNTRY}-18-21.csv', keys_18_21, "statsnz-covid-xmiles", headers)
    print("finished")

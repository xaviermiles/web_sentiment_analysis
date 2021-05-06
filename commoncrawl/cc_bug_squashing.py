# There is a problem where some of the retrieved indices cannot be converted to 
# dictionary/JSON. The purpose of this script was to determine the cause
# of these incomplete JSON-style strings (ie. whether it is the AWS result or
# some product of the processing steps).

# Finding: the payloads returned by 'select_object_content' are truncated at 
# the start. This is a problem with boto3/AWS so not something able to be 
# prevented on the recieving end (as far as I can tell).

# UPDATE: the response chunks are split by newline and the last element is 
# dropped, as I thought there would be a newline at the end of each chunk,
# thus creating an empty-string element when splitting (WHICH WAS WRONG).
# They should be joined into one string before being split by newline (and
# then drop the one final empty-string element).

import os, shutil, subprocess
import csv, json, gzip
import concurrent.futures, itertools
import math

from functools import partial
from multiprocessing import Pool, Value

import botocore, boto3
import warcio
import newspaper

from datetime import datetime


def process_parquet_key(crawl_batch, key):
    """
    Returns:
    - list[string] if '.gz.parquet' file could be processed. Will be an empty
    list if the file does not contains any '.nz' indices.
    - None if any Errors were thrown when trying to process the 
    """
    
    sql_str = """
        SELECT * FROM S3Object s
        WHERE s.url_host_tld='nz'
    """
    s3_client = boto3.session.Session(profile_name="xmiles").client('s3')

    resp = s3_client.select_object_content(
        Bucket='commoncrawl',
        Key=key,
        Expression=sql_str,
        ExpressionType='SQL',
        InputSerialization={'Parquet': {}},
        OutputSerialization={'JSON': {}}
    )

    resp_info = []
    record_num = 0
    payload_fail_flag = False
    previous_final_resp = None
    for event in resp['Payload']:
        if 'Records' in event:            
            record_num += 1

            payload = event['Records']['Payload'].decode()
            new_resp_info = payload.split('\n')[:-1]
            resp_info += new_resp_info
            good = [x.startswith("{") for x in new_resp_info]

            print(f"Record: {record_num}, Added: {len(new_resp_info)}, Total: {len(resp_info)}, Bad: {len(new_resp_info) - sum(good)}")

            resp_fail = None
            for json_str in new_resp_info:
                try:
                    zoo = json.loads(json_str)
                except:
                    resp_fail = json_str
                    break
            
            if json_str:
                print("FAIL JSON CONVERSION. Is it the first element of the payload?", resp_fail == new_resp_info[0])
                print(previous_final_resp)
                print()
#                 print(resp_fail)
                print(event['Records']['Payload'][:100])
                print()
#                 assert xx_fail = xxi

            previous_final_resp = new_resp_info[-1]
    
    
def process_parquet_key_fix(crawl_batch, key):
    """
    Returns:
    - list[string] if '.gz.parquet' file could be processed. Will be an empty
    list if the file does not contains any '.nz' indices.
    - None if any Errors were thrown when trying to process the 
    """
    
    sql_str = """
        SELECT * FROM S3Object s
        WHERE s.url_host_tld='nz'
    """
    s3_client = boto3.session.Session(profile_name="xmiles").client('s3')

    resp = s3_client.select_object_content(
        Bucket='commoncrawl',
        Key=key,
        Expression=sql_str,
        ExpressionType='SQL',
        InputSerialization={'Parquet': {}},
        OutputSerialization={'JSON': {}}
    )

#     resp_info = []
    records = ""
    record_num = 0
    payload_fail_flag = False
    previous_final_resp = None
    for event in resp['Payload']:
        if 'Records' in event:            
            record_num += 1

            payload = event['Records']['Payload'].decode()
            records += payload
            
        if record_num > 34:
            break
            
    resp_info = records.split('\n')
    for i, infoi in enumerate(resp_info):
        try:
            zoo = json.loads(infoi)
        except:
            print("FAIL JSON CONVERSION. Is it the final response?", i == (len(resp_info) - 1))
        


if __name__ == "__main__":
    pkey = "cc-index/table/cc-main/warc/crawl=CC-MAIN-2021-10/subset=warc/part-00108-dbb5a216-bcb2-4bff-b117-e812a7981d21.c000.gz.parquet"
    process_parquet_key_fix("CC-MAIN-2021-10", pkey)
    
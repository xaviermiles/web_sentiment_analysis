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


# S3 Functions
def is_s3_key_valid(bucket, key):
    """
    Return boolean indicating whether given s3 key is valid for the given s3 
    bucket. A "valid s3 key" means that there is a file saved with that
    key in the given bucket
    """
    s3_client = boto3.Session(profile_name="xmiles_processing").client('s3') 
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return False  # object does not exist
        else:
            raise e
            
            
def write_to_txt_s3(txt_list, bucket, key):
    """
    txtlist: list[str], where each element corresponds to a line in the txt
        file.
    """
    s3_client = boto3.Session(profile_name="xmiles_processing").client('s3')
    txt_str = '\n'.join(txt_list) + '\n'
    s3_client.put_object(Body=txt_str, Bucket=bucket, Key=key)
    
            
def write_to_csv_s3(csv_list, bucket, key):
    """
    outlist: list[list[?]], where each inner list will correspond to a row in the
        CSV file. (The elements of the inner lists will joined by commas and 
        the inner lists will then be joined by newlines.)
    """
    s3_client = boto3.Session(profile_name="xmiles_processing").client('s3') 
    csvlist_str_elements = [
        [str(x) for x in row]
        for row in csv_list
    ]
    
    csv_rows = [','.join(row) for row in csvlist_str_elements]
    csv_str = '\n'.join(csv_rows) + '\n'
    s3_client.put_object(Body=csv_str, Bucket=bucket, Key=key)
    
    
def read_txt_from_s3(bucket, key):
    s3_client = boto3.Session(profile_name="xmiles_processing").client('s3')
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    # Python 3.8/3.9 can't download files over 2GB via HTTP, so file is 
    # streamed just in case
    txt_content = ''.join([
        chunk.decode() for chunk in resp['Body'].iter_chunks()
    ])
            
    return txt_content


# Logging functions
def create_logfile(crawl_batch):
    """
    Creates log file for the given crawl batch. Clears pre-existing logfile.
    """
    fpath = os.path.join("logs", crawl_batch + ".txt")
    with open(fpath, 'w') as f:
        f.write("")


def log(crawl_batch, message):
    """
    Logs message to the .txt file for the given crawl batch. Also prints 
    message to console.
    """
    fpath = os.path.join("logs", crawl_batch + ".txt")
    with open(fpath, 'a') as f:
        f.write(message + "\n")
    print(message)
    

# Functions directly related to data retrieval and processing
def get_ccmain_batches(years):
    """
    Return list[string] of all CC-Main batches in the given years
    """
    print(f"Fetching CCMAIN URLs for years: {years}\n")
    index = subprocess.check_output(["aws", "s3", "ls", "--no-sign-request", 
                                     "s3://commoncrawl/cc-index/table/cc-main/warc/"])
    index = [x.split("crawl=")[1] for x in index.decode().split('\n')[:-1]]
    
    # Subselect based on years provided
    index_subset = [i for i in index if i.split('-')[2] in years]
    
    return index_subset


def get_parquet_keys(crawl_batch):
    """
    FILL OUT
    """
    search_str = f"s3://commoncrawl/cc-index/table/cc-main/warc/" \
                 f"crawl={crawl_batch}/subset=warc/"
    terminal_output = subprocess.check_output(["aws", "s3", "ls", "--no-sign-request",
                                               search_str])
    parquet_keys = [x.split(' ')[-1] for x in terminal_output.decode().split('\n')[:-1]]
    
    return parquet_keys


def process_parquet_key(crawl_batch, key):
    """
    Returns:
    - list[string] if '.gz.parquet' file could be processed. Will be an empty
    list if the file does not contains any '.nz' indices.
    - None if any Errors were thrown when trying to process
    """
    
    sql_str = """
        SELECT * FROM S3Object s
        WHERE s.url_host_tld='nz'
    """
    s3_client = boto3.Session(profile_name="xmiles_processing").client('s3')
    
    try:
        resp = s3_client.select_object_content(
            Bucket='commoncrawl',
            Key=key,
            Expression=sql_str,
            ExpressionType='SQL',
            InputSerialization={'Parquet': {}},
            OutputSerialization={'JSON': {}}
        )
        
        resp_str = ""
        end_event_received = False
        continuation_events = 0
        for event in resp['Payload']:
            if 'Records' in event:
                payload = event['Records']['Payload'].decode()
                resp_str += payload
            elif 'Cont' in event:
                continuation_events += 1
        
        resp_info = resp_str.split('\n')[:-1]
        log(crawl_batch, 
            f"Processed ({len(resp_info): >3}): {key.split('/')[-1]}"
            f", ce={continuation_events}")
        
        # Return resp_info regardless of whether the file contained any '.nz' articles
        return resp_info
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'OverMaxParquetBlockSize':
            fail_code = "OMPBS"
        else:
            fail_code = "CE ?"
    except ConnectionResetError:
        fail_code = "CRE"
    except Exception as e:
        fail_code = type(e).__name__
        
    log(crawl_batch, f"FAILED ({fail_code: >5}): {key.split('/')[-1]}")
    # Returns None if any Errors thrown
    
    
def process_index_str(crawl_batch, idx_ref_str):
    """
    idx_ref_str: Index Reference String, a string corresponding to a single
        entry in the CC columnar index (parquet files)
    """
    s3_client = boto3.Session(profile_name="xmiles_processing").client('s3')    
    
    try:
        idx_ref = json.loads(idx_ref_str)
    except:
        # string is not able to be compiled into a dictionary
        log(crawl_batch, f"Index not JSON-compatible: {idx_ref_str[:100]}...")
        return
    
    url = idx_ref['url']
    fetch_time = idx_ref['fetch_time']
    
    range_str = "bytes={}-{}".format(
        idx_ref['warc_record_offset'], 
        idx_ref['warc_record_offset'] + idx_ref['warc_record_length']
    )
    try:
        warc_subset = s3_client.get_object(
            Bucket="commoncrawl",
            Key=idx_ref['warc_filename'],
            Range=range_str
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "InvalidRequestException":
            log(crawl_batch, "SSO Fail: " + idx_ref['url'])
        else:
            log(crawl_batch, f"{type(e).__name__}: {idx_ref['url']}")
        return
    
    try:
        # the last byte is omitted since it causes an error
        warc_contents = gzip.decompress(warc_subset['Body'].read()[:-1]).decode()
    except UnicodeDecodeError:
        # Exclude webpage if WARC contents cannot be decoded
        log(crawl_batch, "Cannot decode: " + idx_ref['url'])
        return
    
    # Extract HTML from WARC contents
    html_start = warc_contents.lower().find('<!doctype html')
    html_end = warc_contents.find('</html>') + len('</html>')
    if html_start == -1 or html_end == -1: 
        return  # No HTML found
    html_contents = warc_contents[html_start:html_end]
    
    # Get text from HTML
    article = newspaper.Article(url='')
    article.set_html(html_contents)
    try:
        article.parse()
    except:
        log(crawl_batch, "Cannot parse: " + idx_ref['url'])
        return
    text = article.text
    if text == "": 
        # newspaper3k package cannot parse any text from the HTML
        with no_text_counter.get_lock():
            no_text_counter.value += 1
        return
    
    return [fetch_time, url, text]
    

def process_batch(crawl_batch, bucket):
    """
    FILL OUT
    """
    log(crawl_batch, f"{crawl_batch} LOGS\n")
    
    parquet_subkeys = get_parquet_keys(crawl_batch)
    parquet_keys = [
        f"cc-index/table/cc-main/warc/crawl={crawl_batch}/subset=warc/{subkey}"
        for subkey in parquet_subkeys
    ]
    
    unprocessed_idxs_key = f"commoncrawl/unprocessed_ccmain_idxs/{crawl_batch}_NZ.txt"
    processed_parquets_flag = False
    if is_s3_key_valid(bucket, unprocessed_idxs_key):
        log(crawl_batch, "Loading unprocessed indices from txt file")
        valid_parquet_idxs_flat = [
            idx_str.rstrip() 
            for idx_str in read_txt_from_s3(bucket, unprocessed_idxs_key).split('\n')[:-1]
        ]    
    else:
        num_parquet_keys = len(parquet_keys)
        print(f"Processing {num_parquet_keys} Parquet files")
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            parquet_idxs = list(
                executor.map(process_parquet_key, itertools.repeat(crawl_batch), parquet_keys)
            )

        # Remove empty lists and None elements
        valid_idxs = list(filter(None, parquet_idxs))
        # Flatten 2D list to 1D list
        valid_idxs_flat = list(itertools.chain.from_iterable(valid_idxs))
        write_to_txt_s3(valid_idxs_flat, bucket, unprocessed_idxs_key)
        
        processed_parquets_flag = True
    
    num_webpages = len(valid_parquet_idxs_flat)
    bunch_size = 10000
    num_bunches = math.ceil(num_webpages / bunch_size)
    num_fail_webpages = 0
    
    log(crawl_batch, f"\nProcessing {num_webpages} webpages in {num_bunches} bunches")
    start = datetime.now()
    for i in range(num_bunches):
        bunch_key = f"commoncrawl/processed_ccmain_bunches/{crawl_batch}/" \
                    f"{crawl_batch}_NZ-{i + 1:04}.csv"
        if is_s3_key_valid(bucket, bunch_key):
            continue # Do not overwrite existing output/bunches
        
        log(crawl_batch, f"Bunch number {i + 1:04}")
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            output = list(
                executor.map(process_index_str, itertools.repeat(crawl_batch), 
                             valid_parquet_idxs_flat[(bunch_size*i):(bunch_size*(i+1)-1)])
            )
        
        headers = ["Datetime", "URL", "Text"]
        good_output = [headers] + list(filter(None, output))
        num_fail_webpages += len(output) - (len(good_output) - 1)
        
        write_to_csv_s3(good_output, bucket, bunch_key)
        
    end = datetime.now()
    log(crawl_batch, "Processing took " + str(end - start))
    
    # Print out summary
    log(crawl_batch,
        f"\nNumber of '.nz' webpages: {num_webpages}\n"
        f"Failed '.nz' webpages: {num_fail_webpages}\n"
        f"(Failed '.nz' webpages - no text extracted: {no_text_counter.value})")
    if processed_parquets_flag:
        num_parquet_with_nz = len([x for x in valid_parquet_idxs if len(x) > 0])
        num_fails = len([x for x in parquet_idxs if x is None])
        log(crawl_batch,
            f"\nNumber of Parquet files with '.nz' webpages: {num_parquet_with_nz} / {num_parquet_keys}\n"
            f"Failed Parquet files: {num_fails} / {num_parquet_keys}")

    
if __name__ == "__main__":
    global no_text_counter
    no_text_counter = Value('i', 0)
    
#     sess = boto3.Session(profile_name="xmiles_processing")
    
    batch = "CC-MAIN-2021-10"
    create_logfile(batch)
    process_batch(batch, "statsnz-covid-xmiles")
    
#     batches_20_and_21 = get_ccmain_batches(["2020", "2021"])
#     for batch in batches_20_and_21:
#         create_logfile(batch)
#         process_batch(batch)

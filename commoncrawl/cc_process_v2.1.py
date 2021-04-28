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
    - None if any Errors were thrown when trying to process the 
    """
    
    sql_str = """
        SELECT * FROM S3Object s
        WHERE s.url_host_tld='nz'
    """
    s3_client = boto3.session.Session(profile_name="xmiles").client('s3')
    
    try:
        resp = s3_client.select_object_content(
            Bucket='commoncrawl',
            Key=key,
            Expression=sql_str,
            ExpressionType='SQL',
            InputSerialization={'Parquet': {}},
            OutputSerialization={'JSON': {}}
        )
        
        resp_info = []
        end_event_received = False
        continuation_events = 0
        for event in resp['Payload']:
            if 'Records' in event:
                payload = event['Records']['Payload'].decode()
                x = payload.split('\n')[:-1]
                print(len(x))
                break
            elif 'Cont' in event:
                continuation_events += 1
        
        log(crawl_batch, 
            f"Processed ({len(resp_info): >3}): {key.split('/')[-1]}"
            f", ce={continuation_events}")
        
        # Return resp_info regardless of whether the Parquet file contained '.nz' indices
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
    s3_client = boto3.session.Session(profile_name="xmiles").client('s3')
        
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
    # Exclude webpage if no HTML tag found
    if html_start == -1 or html_end == -1:
#         log(crawl_batch, "Cannot find HTML: " + idx_ref['url'])
        # Don't need to be alerted - too much spam
        print("no html")
        return
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
    # Exclude webpage if newspaper3k package cannot parse any text
    if text == "": 
        # Don't need to be alerted - intrinsic to webpage
        with no_text_counter.get_lock():
            no_text_counter.value += 1
        return
    
    return [fetch_time, url, text]
    

def process_batch(crawl_batch):
    """
    FILL OUT
    """
    log(crawl_batch, f"{crawl_batch} LOGS\n")
    
    parquet_subkeys = get_parquet_keys(crawl_batch)
    parquet_keys = [
        f"cc-index/table/cc-main/warc/crawl={crawl_batch}/subset=warc/{subkey}"
        for subkey in parquet_subkeys
    ]
    
    unprocessed_idxs_fpath = os.path.join("unprocessed_ccmain_idxs", f"{crawl_batch}.txt")
    processed_parquets_flag = False
    if not os.path.exists(unprocessed_idxs_fpath):
        num_parquet_keys = len(parquet_keys)
        print(f"Processing {num_parquet_keys} Parquet files")
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            parquet_idxs = list(
                executor.map(process_parquet_key, itertools.repeat(crawl_batch), parquet_keys)
            )

        # Remove empty lists and None elements
        valid_parquet_idxs = list(filter(None, parquet_idxs))
        # Flatten 2D list to 1D list
        valid_parquet_idxs_flat = list(itertools.chain.from_iterable(valid_parquet_idxs))
        with open(unprocessed_idxs_fpath, "w") as f:
            f.writelines(idx_str + "\n" for idx_str in valid_parquet_idxs_flat)
        
        processed_parquets_flag = True
    else:
        log(crawl_batch, "Loading unprocessed indices from txt file")
        with open(unprocessed_idxs_fpath, "r") as f:
            valid_parquet_idxs_flat = [idx_str.rstrip() for idx_str in f.readlines()]
    num_webpages = len(valid_parquet_idxs_flat)
    
    bunch_size = 20000
    num_bunches = math.ceil(num_webpages / bunch_size)
    bunch_folder = os.path.join("processed_ccmain_bunches", crawl_batch)
    if os.path.exists(bunch_folder):
        shutil.rmtree(bunch_folder)  # clear existing data
    os.makedirs(bunch_folder)
    num_fail_webpages = 0
    
    log(crawl_batch, f"\nProcessing {num_webpages} webpages in {num_bunches} bunches")
    start = datetime.now()
    for i in range(num_bunches):
        log(crawl_batch, f"Bunch number {i + 1}")
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            output = list(
                executor.map(process_index_str, itertools.repeat(crawl_batch), 
                             valid_parquet_idxs_flat[(bunch_size*i):(bunch_size*(i+1)-1)])
            )
        
        headers = ["Datetime", "URL", "Text"]
        good_output = [headers] + list(filter(None, output))
        num_fail_webpages += len(output) - (len(good_output) - 1)
        
        bunch_fpath = os.path.join(bunch_folder, f"{crawl_batch}_NZ-{i + 1}.csv")
        with open(bunch_fpath, "w") as f:
            writer = csv.writer(f)
            writer.writerows(good_output)
        
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
    
    batch = "CC-MAIN-2021-10"
    create_logfile(batch)
    process_batch(batch)
    
#     batches_20_and_21 = get_ccmain_batches(["2020", "2021"])
#     for batch in batches_20_and_21:
#         create_logfile(batch)
#         process_batch(batch)

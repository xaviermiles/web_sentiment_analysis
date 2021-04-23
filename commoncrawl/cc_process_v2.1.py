import botocore, boto3
import os, subprocess
import csv, json, gzip
import warcio
import newspaper
import concurrent.futures
import itertools, collections


def create_logfile(crawl_batch):
    """
    Overwrites pre-existing logfiles
    """
    fpath = os.path.join("logs", crawl_batch + ".txt")
    with open(fpath, 'w') as f:
        f.write(crawl_batch + " LOGS \n")


def add_to_logfile(crawl_batch, message):
    fpath = os.path.join("logs", crawl_batch + ".txt")
    with open(fpath, 'a') as f:
        f.write(message + "\n")
    print(message)


def get_parquet_keys(crawl_batch):
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
                resp_info += payload.split('\n')[:-1]
            elif 'Cont' in event:
                continuation_events += 1
        
        add_to_logfile(crawl_batch, 
                       f"Processed ({len(resp_info): >3}): {key.split('/')[-1]}"
                       f", ce={continuation_events}")
        
        # Return resp_info regardless of whether the Parquet file contained '.nz' indices
        return resp_info
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'OverMaxParquetBlockSize':
            fail_code = "OMPBS"
        else:
            fail_code = "CE ?"
    except ConnectionResetError as e:
        fail_code = "CRE"
    except:
        fail_code = "?"
        
    add_to_logfile(crawl_batch, f"FAILED ({fail_code: >5}): {key.split('/')[-1]}")
    # Returns None if any Errors thrown
    
    
def process_index_str(idx_ref_str, s3_client):
    """
    idx_ref_str: Index Reference String, a string corresponding to a single
        entry in the CC columnar index (parquet files)
    """
    idx_ref = json.loads(idx_ref_str)
    url = idx_ref['url']
    fetch_time = idx_ref['fetch_time']
    
    range_str = "bytes={}-{}".format(
        idx_ref['warc_record_offset'], 
        idx_ref['warc_record_offset'] + idx_ref['warc_record_length']
    )
    warc_subset = s3_client.get_object(
        Bucket="commoncrawl",
        Key=idx_ref['warc_filename'],
        Range=range_str
    )
    
    try:
        # the last byte is omitted since it causes an error
        warc_contents = gzip.decompress(warc_subset['Body'].read()[:-1]).decode()
    except:
        # Exclude webpage if WARC contents cannot be decoded
        print("Cannot decode:", idx_ref['url'])
        return
    
    # Extract HTML from WARC contents
    html_start = warc_contents.lower().find('<!doctype html>')
    html_end = warc_contents.find('</html>') + len('</html>')
    # Exclude webpage if no HTML tag found
    if html_start == -1 or html_end == -1: return
    html_contents = warc_contents[html_start:html_end]
    
    # Get text from HTML
    article = newspaper.Article(url='')
    article.set_html(html_contents)
    try:
        article.parse()
    except:
        print("Cannot parse:", idx_ref['url'])
        return
    text = article.text
    # Exclude webpage if newspaper3k package cannot parse any text
    if text == "": return
    
    return [fetch_time, url, text]
    

def process_batch(crawl_batch):
    print(f"On batch: {crawl_batch}")
    outfolder = os.path.join("ccindex_nz", crawl_batch)
    if not os.path.exists(outfolder):
        os.makedirs(outfolder)
    
    parquet_subkeys = get_parquet_keys(crawl_batch)
    parquet_keys = [
        f"cc-index/table/cc-main/warc/crawl={crawl_batch}/subset=warc/{subkey}"
        for subkey in parquet_subkeys
    ]
    
    subset_parquet_keys = parquet_keys[100:110]
    num_parquet_keys = len(subset_parquet_keys)
    print(f"Processing {num_parquet_keys} Parquet files")
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        parquet_idxs = list(
            executor.map(process_parquet_key, itertools.repeat(crawl_batch), subset_parquet_keys)
        )
    
    # Remove empty lists and None elements
    valid_parquet_idxs = list(filter(None, parquet_idxs))
    # Flatten 2D list to 1D list
    valid_parquet_idxs_flat = list(itertools.chain.from_iterable(valid_parquet_idxs))
    
    num_webpages = len(valid_parquet_idxs_flat)
    print(f"\nProcessing {num_webpages} webpages")
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        output = list(
            executor.map(process_index_str, valid_parquet_idxs_flat)
        )

    headers = ["Datetime", "URL", "Text"]
    good_output = headers + list(filter(None, output))
    with open(crawl_batch + "_NZ.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(good_output)
    
    # Print out summary
    num_fail_webpages = len(output) - (len(good_output) - 1)
    num_parquet_with_nz = len([x for x in valid_parquet_idxs if len(x) > 0])
    num_fails = len([x for x in parquet_idxs if x is None])
    add_to_logfile(crawl_batch,
                   f"\nNumber of '.nz' webpages: {num_webpages}\n"
                   f"Failed '.nz' webpages: {num_fail_webpages}"
                   f"Number of Parquet files with '.nz' webpages: {num_parquet_with_nz} / {num_parquet_keys}\n"
                   f"Failed Parquet files: {num_fails} / {num_parquet_keys}")

    
if __name__ == "__main__":
    
    batch = "CC-MAIN-2021-10"
    create_logfile(batch)
    process_batch(batch)

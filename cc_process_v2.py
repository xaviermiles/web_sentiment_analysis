"""
The original cc_process script supports retrieval of both CC-MAIN and CC-NEWS.
Since this uses the columnar index, which only supports CC-MAIN, this V2
script only supports retrieval of CC-MAIN.

This should be faster than the original by utilising Amazon S3 Select to search
for relevant webpages (ie. top-level domain=.nz), and then extracting the HTML
for these from the appropriate locations in the WARC files.
"""

import boto3
import subprocess
import json
import gzip
import warcio
import newspaper


def get_parquet_keys(crawl_batch):
    search_str = f"s3://commoncrawl/cc-index/table/cc-main/warc/" \
                 f"crawl={crawl_batch}/subset=warc/"
    terminal_output = subprocess.check_output(["aws", "s3", "ls", "--no-sign-request",
                                               search_str])
    parquet_keys = [x.split(' ')[-1] for x in terminal_output.decode().split('\n')[:-1]]
    
    return parquet_keys


def process_parquet_key(key, s3_client):
    parquet_fname = key.split('/')[-1]
    print("Processing:", parquet_fname)
    
    sql_str = """
        SELECT * FROM S3Object s
        limit 10
    """
    # WHERE s.url_host_tld='nz'
    
    resp = s3_client.select_object_content(
        Bucket='commoncrawl',
        Key=key,
        Expression=sql_str,
        ExpressionType='SQL',
        InputSerialization={'Parquet': {}},
        OutputSerialization={'JSON': {}}
    )

    end_event_received = False
    for event in resp['Payload']:
        if 'Records' in event:
            payload = event['Records']['Payload'].decode()
            info = payload.split('\n')[:-1]
        elif 'End' in event:
            end_event_received = True

    if not end_event_received:
        raise Exception("End event not received, request incomplete.")
        
    return info


def process_idx_ref(idx_ref_str, s3_client):
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
    # the last byte is omitted since it causes an error
    warc_contents = gzip.decompress(warc_subset['Body'].read()[:-1]).decode()
    
    # Extract HTML from WARC contents
    html_start = warc_contents.lower().find('<!doctype html>')
    html_end = warc_contents.find('</html>') + len('</html>')
    html_contents = warc_contents[html_start:html_end]
    # Exclude webpage if no HTML tag found
    if html_start == -1 or html_end == -1: return
    
    # Get text from HTML
    article = newspaper.Article(url='')
    article.set_html(html_contents)
    article.parse()
    text = article.text
    # Exclude webpage if newspaper3k package cannot parse any text
    if text == "": return
    
    return fetch_time, url, text
    

def process_batch(crawl_batch, s3_client):
    parquet_keys = get_parquet_keys(batch)
    
    full_parquet_keys = [
        f"cc-index/table/cc-main/warc/crawl={batch}/subset=warc/{key}"
        for key in parquet_keys
    ]
    
    idx_refs = []
    for key in full_parquet_keys[:2]:
        idx_refs += process_parquet_key(key, s3_client)
    
    infos = []
    for x in idx_refs:
        infos.append(process_idx_ref(idx_refs, s3_client))
        
    print(infos)

    
if __name__ == "__main__":
    sess = boto3.session.Session(profile_name="xmiles")
    s3 = sess.client('s3')
    
    batch = "CC-MAIN-2021-10"
    process_batch(batch, s3)
    
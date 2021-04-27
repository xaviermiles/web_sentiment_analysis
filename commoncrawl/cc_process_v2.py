import botocore, boto3
import os, subprocess
import csv, json, gzip
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
#     parquet_fname = key.split('/')[-1]
#     print("Processing:", parquet_fname)
    
    sql_str = """
        SELECT * FROM S3Object s
        WHERE s.url_host_tld='nz'
    """
    
    resp = s3_client.select_object_content(
        Bucket='commoncrawl',
        Key=key,
        Expression=sql_str,
        ExpressionType='SQL',
        InputSerialization={'Parquet': {}},
        OutputSerialization={'JSON': {}}
    )
    
    info = []
    end_event_received = False
    for event in resp['Payload']:
        if 'Records' in event:
            payload = event['Records']['Payload'].decode()
            info += payload.split('\n')[:-1]
        elif 'End' in event:
            end_event_received = True

    if not end_event_received:
        raise Exception("End event not received, request incomplete.")
        
    return info


def get_index_strs(parquet_key, s3_client):
    try:
        index_str = process_parquet_key(parquet_key, s3_client)
        print(f"Processed ({len(index_str)}):  {key.split('/')[-1]}")
        return index_str
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'OverMaxParquetBlockSize':
            print(f"FAILED (OMPBS): {parquet_key.split('/')[-1]}")
#             fails += 1
        else:
            print(f"Unexpected error: {e}")
    except ConnectionResetError as e:
        print(f"FAILED (CRE):   {parquet_key.split('/')[-1]}")
#         fails += 1
            

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
    try:
        article.parse()
    except:
        print(html_start, html_end, html_contents)
    text = article.text
    # Exclude webpage if newspaper3k package cannot parse any text
    if text == "": return
    
    return [fetch_time, url, text]
    

def process_batch(crawl_batch, s3_client):
    print(f"On batch: {crawl_batch}")
    outfolder = os.path.join("ccindex_nz", crawl_batch)
    if not os.path.exists(outfolder):
        os.makedirs(outfolder)
    
    parquet_keys = get_parquet_keys(batch)
    
    full_parquet_keys = [
        f"cc-index/table/cc-main/warc/crawl={batch}/subset=warc/{key}"
        for key in parquet_keys
    ]
    
    parquet_fails = 0
    index_ref_strs = []
    for full_key in full_parquet_keys[107:109]:
        fname = full_key.split('/')[-1].replace('.gz.parquet', '.txt')
        fpath = os.path.join(outfolder, fname)
        if os.path.exists(fpath):
            with open(fpath, 'r') as f:
                new_index_strs = [index_str.rstrip() for index_str in f.readlines()]
        else:
            new_index_strs = get_index_strs(full_key, s3_client)
            # returns None if no '.nz' indices
        
        if new_index_strs is None:
            continue
        
        index_ref_strs += new_index_strs
        with open(fpath, 'w') as f:
            f.writelines(f"{index_str}\n" for index_str in new_index_strs)
    
#     output = []
#     for x in idx_ref_strs:
#         output.append(process_index_str(x, s3_client))
    
#     good_output = list(filter(None, output))  # removes None entries
    
#     #
#     with open("ccmain-test.csv", "w") as f:
#         writer = csv.writer(f)
#         writer.writerows(good_output)
    
    # Print out summary
    print()
    print("Number of unique articles:", len(good_output))
    print(f"Failed Parquet files: {parquet_fails} / {len(subset_parquet_keys)}")

    
if __name__ == "__main__":
    sess = boto3.session.Session(profile_name="xmiles")
    s3 = sess.client('s3')
    
    batch = "CC-MAIN-2021-10"
    process_batch(batch, s3)
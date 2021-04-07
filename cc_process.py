import os
import csv
import subprocess
import requests
import warcio
import newspaper
import itertools
import concurrent.futures

import numpy as np
import pandas as pd

from datetime import date, timedelta


def get_ccnews_urls():
    """
    Return list[string] of URL links to all Common Crawl News datasets
    """
    url_prefix = "https://commoncrawl.s3.amazonaws.com/"
    
    ccnews_bucket = subprocess.check_output(["aws", "s3", "ls", "--recursive", "--no-sign-request", 
                                             "s3://commoncrawl/crawl-data/CC-NEWS/"])
    ccnews_bucket_list = ccnews_bucket.decode().split('\n')[:-1]
    ccnews_urls = [url_prefix + dataset.split(' ')[-1] 
                   for dataset in ccnews_bucket_list]
    
    return ccnews_urls


def get_date_range(start_date, end_date):
    """
    Return list[string] of dates between start_date & end_date (inclusive)
    """
    date_range = end_date - start_date
    dates = []
    for i in range(date_range.days + 1):
        date = (start_date + timedelta(days=i)).strftime("%Y%m%d")
        dates.append(date)
    return dates


def process_ccnews_file(url):
    """
    Requests and processes a CC-NEWS URL. Writes all articles that have a URL
    ending in '.nz' to a CSV. Returns a list of lists which includes info
    about the articles in the downloaded '.warc.gz' file.
    """
    print(f"Processing: {url.split('/')[-1]}")
    
    articles_list = []
    r = requests.get(url, stream=True)
    
    for record in warcio.archiveiterator.ArchiveIterator(r.raw):
        # rec_type='warcinfo' is metadata for the entire WARC batch
        # rec_type='request' is the information sent from client to server

        if record.rec_type == 'response':
            target_url = record.rec_headers.get_header('WARC-Target-URI')
            # Only take New Zealand URLs
            # -> Is there a better way to verify the source of the articles?
            if '.nz/' not in target_url: continue

            content_type = record.http_headers.get_header('Content-Type')
            # default encoding assumed to be utf-8
            if content_type is None:
                content_type = "text/html; charset=utf-8"
            elif ";" not in content_type:
                content_type = content_type + "; charset=utf-8"

            #
            try:
                doctype, raw_encoding = content_type.split(';')[:2]
                encoding = raw_encoding.split('charset=')[1]
            except:
                print("Unable to be parsed:", content_type)

            # Process article - can only process HTML 
            if not doctype == 'text/html': continue

            try:
                html = record.content_stream().read().decode(encoding)
            except:
                print(f"Unable to be decoded: {target_url}")
            article = newspaper.Article(url='')
            article.set_html(html)
            article.parse()
            text = article.text
            # Exclude webpage if newspaper3k package cannot parse any text
            if text == "": continue

            datetime = record.rec_headers.get_header('WARC-Date')
            articles_list.append([
                datetime,
                target_url,
                text
            ])

    return articles_list


def process_ccnews_files(urls):
    """
    Requests and processes a list[string] of CC-NEWS URLs. Writes all articles 
    that have a URL ending in '.nz' to a CSV.
    """
    print(f"Processing {len(urls)} CC-NEWS URLs")
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        articles_raw = list(executor.map(process_ccnews_file, urls))

    # Flatten articles_raw to get list of lists
    articles_list = list(itertools.chain.from_iterable(articles_raw))

    articles_df = pd.DataFrame.from_records(articles_list,
                                            columns = ['Datetime', 'URL', 'Text'])
    
    articles_df.to_csv('cc-nz-articles.csv', index=False)

    
if __name__ == "__main__":
    ccnews_urls = get_ccnews_urls()
    
    # Subset URLs based on dates
    dates = get_date_range(date(2021, 3, 4), date(2021, 3, 6))
    ccnews_urls_subset = [url for url in ccnews_urls 
                          for date in dates if date in url]
    
    # Process one file (testing)
#     g = process_ccnews_file(ccnews_urls_subset[0])
#     with open('test.csv', 'w') as f:
#         writer = csv.writer(f)
#         writer.writerows(g)
        
    # Process multiple files
    process_ccnews_files(ccnews_urls_subset)

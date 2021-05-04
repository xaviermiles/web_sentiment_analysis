import os
import csv
import subprocess
import requests
import urllib
import gzip
import itertools
import warcio
import newspaper
import concurrent.futures

from datetime import date, timedelta


def get_warc_urls(warc_paths_gz_url):
    """
    Recieves a URL to a warc.paths.gz file and returns the contents as a list[string].
    """
    with urllib.request.urlopen(warc_paths_gz_url) as gzf:
        with gzip.open(gzf) as f:
            file_content = f.read()
            paths = [
                "https://commoncrawl.s3.amazonaws.com/" + path
                for path in file_content.decode().split('\n')[:-1]
            ]
            
    return paths


def get_ccmain_urls(years):
    """
    Return list[string] of URL links to all Common Crawl Main datasets in the given year-week batches
    """
    print(f"Fetching CCMAIN URLs for years: {years}")
    index = subprocess.check_output(["aws", "s3", "ls", "--no-sign-request", 
                                     "s3://commoncrawl/cc-index/table/cc-main/warc/"])
    index = [x.split("crawl=")[1] for x in index.decode().split('\n')[:-1]]
    
    # Subselect based on yearweek range provided
#     start_year, start_week = int(start_yearweek.split('-')[0]), int(start_yearweek.split('-')[1])
#     end_year, end_week = int(end_yearweek.split('-')[0]), int(end_yearweek.split('-')[1])
#     print(start_year, start_week, end_year, end_week)
#     [i[:-1].split('-')[2:4] for i in index]

    # Subselect based on years provided
    index_subset = [i for i in index if i.split('-')[2] in years]
    
    warc_paths_gz_urls = [
        "https://commoncrawl.s3.amazonaws.com/crawl-data/" + batch.split(' ')[-1] + "warc.paths.gz"
        for batch in index_subset
    ]
    warc_urls_raw = [get_warc_urls(url) for url in warc_paths_gz_urls]
    # flatten to 1-D list
    warc_urls = list(itertools.chain.from_iterable(warc_urls_raw))
    
    return warc_urls


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


def process_cc_file(url, mode):
    """
    Requests and processes a CC URL. Writes all articles that have a URL
    ending in '.nz' to a CSV. Returns a list of lists which includes info
    about the NZ articles in the downloaded '.warc.gz' file.
    
    - url: string
    - mode: string, "ccnews" OR "ccmain"
    """
    if mode not in ["ccnews", "ccmain"]: 
        raise TypeError("mode argument must be 'ccnews' or 'ccmain'")
        
    fname_in = url.split('/')[-1]
    fname_out = f"{mode}-nz-{fname_in.replace('CC-NEWS-', '').replace('CC-MAIN-', '').replace('.warc.gz', '')}.csv"
    fpath_out = os.path.join(f"processed_{mode}", fname_out)
    # Skip if already processed
    if os.path.exists(fpath_out): 
        print(f"Processed: {fname_in}")
        return
    else:
        print(f"Processing: {fname_in}")
    
    articles_list = [
        ['Datetime', 'URL', 'Text']
    ]
    r = requests.get(url, stream=True)
    
    for record in warcio.archiveiterator.ArchiveIterator(r.raw):
        # rec_type='warcinfo' is metadata for the entire WARC batch
        # rec_type='request' is the information sent from client to server

        if record.rec_type == 'response':
            target_url = record.rec_headers.get_header('WARC-Target-URI')
            # Only take New Zealand URLs
            # -> Is there a better way to verify the source of the articles?
            if '.nz/' not in target_url: continue

            content_type = record.http_headers.get_header('Content-Type').lower()
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

    # Save NZ articles to CSV
    with open(fpath_out, 'w', newline="") as f:
        writer = csv.writer(f)
        writer.writerows(articles_list)

    
if __name__ == "__main__":
    # CC-NEWS
    ccnews_urls = get_ccnews_urls()

    dates = get_date_range(date(2020, 1, 1), date(2021, 3, 6))
    ccnews_urls_subset = [url for url in ccnews_urls 
                          for date in dates if "CC-NEWS-" + date in url]
    
    print(f"Processing {len(ccnews_urls_subset)} CC-NEWS URLs")
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        executor.map(process_cc_file, ccnews_urls_subset, itertools.repeat("ccnews"))
    
#     ## CC-MAIN
#     years = ['2020', '2021']
#     ccmain_urls = get_ccmain_urls(years)
#     print(ccmain_urls[:10])
    
#     MODE = "ccmain"
#     print(f"Processing {len(ccmain_urls)} {MODE} URLs")
#     with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
#         executor.map(process_cc_file, ccmain_urls[:100], itertools.repeat(MODE))

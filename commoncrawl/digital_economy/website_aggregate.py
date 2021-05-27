# Cannot process apply gensim.utils.simple_preprocess() to the 'Text'
# column of websites as it will exceed RAM and kill python script.
# ie. cannot:
# $ websites['Words'] = websites['Text'].map(gensim.utils.simple_preprocess)

import os, subprocess
import re
import csv, json, pickle
import math
import boto3, botocore
from urllib import parse
from collections import Counter
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from more_itertools import chunked
from wordcloud import WordCloud
# from memory_profiler import profile

import nltk
import gensim
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans, MiniBatchKMeans

    
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
            

def read_csv_from_s3(bucket, key, header=True):
    s3 = sess.client('s3')
    resp = s3.get_object(Bucket=bucket, Key=key)
    # Python 3.8/3.9 can't download files over 2GB via HTTP, so file is 
    # streamed just in case
    csv_str = b''.join([
        chunk for chunk in resp['Body'].iter_chunks()
    ]).decode()
    
    # keepends=True to preserve newlines within Text fields
    csv_lines = csv_str.splitlines(keepends=True)
    csv_list = list(csv.reader(csv_lines, quotechar='"'))
    if header:
        df = pd.DataFrame(csv_list[1:], columns=csv_list[0])
    else:
        df = pd.DataFrame(csv_list)
    
    return df


def write_to_csv_s3(csv_list, bucket, key):
    """
    csv_list: list[list[?]], where each inner list will correspond to a row in the
        CSV file. (The elements of the inner lists will joined by commas and 
        the inner lists will then be joined by newlines.)
    """
    s3 = sess.client('s3') 
    # double-quotes are used to enclose fields, so any double-quotes are 
    # changed to single-quotes
    csvlist_str_elements = [
        ['"' + str(x).replace('"', "'") + '"' for x in row]
        for row in csv_list
    ]
    
    csv_rows = [','.join(row) for row in csvlist_str_elements]
    csv_str = '\n'.join(csv_rows) + '\n'
    s3.put_object(Body=csv_str, Bucket=bucket, Key=key)
    

# General helper functions
def check_memory():
    """
    Calls 
    $ cat meminfo | grep Mem
    
    The call/pipe is separated into two steps as recommended by:
    https://stackoverflow.com/questions/13332268/how-to-use-subprocess-command-with-pipes
    """
    meminfo = subprocess.Popen(('cat', '/proc/meminfo'), stdout=subprocess.PIPE)
    filtered_meminfo = subprocess.check_output(('grep', 'Mem'), stdin=meminfo.stdout)
    print(filtered_meminfo.decode())


# Main-process functions
def get_word_counts(text):
    words = gensim.utils.simple_preprocess(text)
    word_counts = Counter(words)

    return dict(word_counts)


# ORIGINAL METHOD
# def get_avg_word_freqs(df, stops):
#     unique_words = set().union(*(d.keys() for d in df['Counts']))
#     filt_unique_words = unique_words - stops # remove stopwords
#     print(f"Includes {len(filt_unique_words)} unique words, ", end="", flush=True)
#     df['Num_words'] = df['Counts'].apply(len)
    
#     avg_word_freqs = {
#         word: np.array([
#             countsi.get(word, 0) / num_wordsi for countsi, num_wordsi in zip(df['Counts'], df['Num_words'])
#         ]).mean()
#         for word in filt_unique_words
#     }
#     print("Found avg_word_freqs, ", end="", flush=True)
    
#     return avg_word_freqs


def load_websites_df(batch, bucket):
    s3 = sess.client('s3')
    
    site_agg_key = f"commoncrawl/website_aggregated/{batch}_NZ-websites.csv"
    if is_s3_key_valid(bucket, site_agg_key):
        websites = read_csv_from_s3(bucket, site_agg_key)
        print("Loaded websites from S3")
    else:
        output_keys = [
            x['Key'] for x in
            s3.list_objects_v2(Bucket="statsnz-covid-xmiles", Prefix=f"commoncrawl/processed_ccmain_bunches/{batch}/")['Contents']
        ]

        all_output = pd.concat((
            read_csv_from_s3("statsnz-covid-xmiles", key) for key in output_keys
        )).reset_index(drop=True)
        all_output['Netloc'] = [parse.urlsplit(url).netloc for url in all_output['URL']]
        print("Collected webpages")

        websites = pd.DataFrame(
            all_output.groupby('Netloc')['Text'].apply(' | '.join)
        ).reset_index()
        print("Collated websites")
        websites['Counts'] = websites['Text'].map(get_word_counts)
        print("Word counts obtained")
        
#         csv_list = [websites.columns.tolist()] + websites.values.tolist()
#         write_to_csv_s3(csv_list, bucket, site_agg_key)
        print(websites.head())
    
    check_memory()
    return websites

    
def get_cluster_wclouds(batch, bucket, n_clusters, seed):
    if n_clusters < 1 or n_clusters > 128:
        # Since cluster labels are stored as uint8 (possible values=0,1,...,127)
        # and "zero clusters" doesn't make sense.
        raise ValueError("n_clusters should be an integer between 1 & 127 (inclusive)")
    
    output_folder = os.path.join("site_agg_output", f"clusters{n_clusters}-seed{seed}")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    clustered_sites_fpath = os.path.join(output_folder, "clustered_websites.csv")
    if os.path.exists(clustered_sites_fpath):
        websites = pd.read_csv(clustered_sites_fpath,
                               dtype={'KM_cluster': 'uint8'})
        # Convert dict-style string into dict object
        websites['Counts'] = [json.loads(counts_str.replace("'", '"'))
                              for counts_str in websites['Counts']]
        print("Loaded clustered websites from EC2")
        
        vectorizer = TfidfVectorizer(stop_words='english') # for getting cluster wclouds
        X = vectorizer.fit_transform(websites['Text'])
        print("Tfidf values obtained")
    else:
        websites = load_websites_df(batch, bucket)

        vectorizer = TfidfVectorizer(stop_words='english')
        X = vectorizer.fit_transform(websites['Text'])
        print("Tfidf values obtained")
        km = KMeans(n_clusters=n_clusters, init='k-means++', random_state=seed).fit(X)
        websites['KM_cluster'] = km.labels_.astype('uint8')
        print("Clustered websites")
        websites.to_csv(clustered_sites_fpath)
    
    vectorizer_words = np.array(vectorizer.get_feature_names())
    print(websites.head())
    check_memory()
    
    current_dt = (datetime.now() + timedelta(hours=12)).strftime("%Y%m%d%H%M%S") # add 12hrs for timezone
    stops = TfidfVectorizer(stop_words='english').get_stop_words()
    print("Got stopwords")
    # Produce wordcloud for each cluster
    fig, axes = plt.subplots(math.ceil(n_clusters / 3), 3, 
                             figsize=(24, 6 * math.ceil(n_clusters / 3)))
    fig.suptitle(f"Clustering of websites with KMeans(n_clusters={n_clusters}, seed={seed})")
    ax = axes.ravel()
    for i in range(len(ax)):
        if i < n_clusters:
            ax[i].set(xticks=[], yticks=[])
        else:
            ax[i].set_visible(False)  # make unused axes invisible
            
    for i in range(n_clusters):
        cluster_tstart = datetime.now()
        print(f"Wordcloud #{i + 1}, ", end="", flush=True)
#         cluster_websites = websites[websites['KM_cluster'] == i]
#         print(f"Includes {cluster_websites.shape[0]} websites, ", end="", flush=True)
        
        # ORIGINAL METHOD using each word's frequency averaged across documents - VERY SLOW
#         # avg_word_freqs are saved in pickle format for later retrieval
#         avg_word_freqs_fpath = os.path.join("data", f"avg_word_freqs_cluster{i + 1}.p")
#         if os.path.exists(avg_word_freqs_fpath):
#             with open(avg_word_freqs_fpath, 'rb') as fp:
#                 avg_word_freqs = pickle.load(fp)
#             print(f"Includes {len(avg_word_freqs)} unique words, ", end="", flush=True)
#         else:
#             avg_word_freqs = get_avg_word_freqs(cluster_websites, stops)
#             with open(avg_word_freqs_fpath, 'wb') as fp:
#                 pickle.dump(avg_word_freqs, fp, protocol=pickle.HIGHEST_PROTOCOL)
#         cluster_wcloud = WordCloud(width=1800, height=1200, collocations=False).generate_from_frequencies(avg_word_freqs)

        # NEW METHOD using each word's tfidf values
        TOP_N = 100
        cluster_idx = websites[websites['KM_cluster'] == i].index.tolist()
        norm_factor = X[cluster_idx, :].sum()
        tfidf_per_word = X[cluster_idx, :].sum(axis=0) / norm_factor
        # Get indices of the top 'n' tfidf values:
        top_n_tfidf_idxs = np.argpartition(tfidf_per_word, -TOP_N)[:, -TOP_N:].tolist()[0]
        word_to_tfidf = {
            word: tfidf for word, tfidf in 
            zip(vectorizer_words[top_n_tfidf_idxs], tfidf_per_word[:, top_n_tfidf_idxs].tolist()[0])
        }
        cluster_wcloud = WordCloud(width=1800, height=1200).generate_from_frequencies(word_to_tfidf)
        
        ax[i].imshow(cluster_wcloud)
        ax[i].set(title=f"KMeans Cluster #{i + 1} - {len(cluster_idx)} website(s)")
        
        cluster_tdelta = datetime.now() - cluster_tstart
        print(f"Plotted, Time taken: {cluster_tdelta}")
        if i < (n_clusters - 1):
            plt.savefig(os.path.join(output_folder, f"km_wclouds-WIP{i + 1}.jpeg"))
            
        else:
            plt.savefig(os.path.join(output_folder, "km_wclouds-FINAL.jpeg"))
            print("Finished")

            
if __name__ == "__main__":
    sess = boto3.Session(profile_name="xmiles")
    
    get_cluster_wclouds("CC-MAIN-2021-10", "statsnz-covid-xmiles", 5, 777)

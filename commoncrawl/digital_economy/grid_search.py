# Using more parameters in the grid search will give better exploring 
# power but will increase processing time in a combinatorial way.
#
# EXAMPLES:
# Using Pipeline + GridSearchCV:
# https://scikit-learn.org/stable/auto_examples/model_selection/grid_search_text_feature_extraction.html
# Using Doc2Vec model with GridSearchCV:
# https://stackoverflow.com/questions/50278744/pipeline-and-gridsearch-for-doc2vecs
# Statistical comparison of model results (goes far beyond this script):
# https://scikit-learn.org/stable/auto_examples/model_selection/plot_grid_search_stats.html


import os
import csv, json
import boto3
from time import time
from pprint import pprint
from urllib import parse
from collections import Counter

import numpy as np
import pandas as pd

from sklearn import metrics
from sklearn.base import BaseEstimator
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import SGDClassifier, LogisticRegression

import gensim
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

import lightgbm as lgb


class Doc2VecWrapper(BaseEstimator):

    def __init__(self, vector_size=100, min_count=5, epochs=10, window=5, 
                 min_alpha=0.001):
        self.vector_size = vector_size
        self.min_count = min_count
        self.epochs = epochs
        self.window = window
        self.min_alpha = min_alpha

    def fit(self, text_series, target_series):
        self.d2v_model = Doc2Vec(
            vector_size=self.vector_size,
            min_count=self.min_count,
            epochs=self.epochs,
            window=self.window,
            min_alpha=self.min_alpha
        )
        
        words_series = text_series.map(gensim.utils.simple_preprocess)
        tagged_words_series = pd.Series([
            TaggedDocument(words=wordsi, tags=[targeti]) 
            for wordsi, targeti in zip(words_series, target_series)
        ])
        
        self.d2v_model.build_vocab(tagged_words_series)
        self.d2v_model.train(tagged_words_series, 
                             total_examples=self.d2v_model.corpus_count, 
                             epochs=self.d2v_model.epochs)
        return self

    def transform(self, text_series):
        """
        Return prediction for data
        """
        words_series = text_series.map(gensim.utils.simple_preprocess)
        X = pd.DataFrame(
            [self.d2v_model.infer_vector(wordsi) for wordsi in words_series],
            index=words_series.index
        )
        return X

    def fit_transform(self, text_series, target_series):
        """
        Fit Doc2Vec to data
        Return prediction for data
        """
        self.fit(text_series, target_series)
        return self.transform(text_series)

    
def get_netloc(url):
    return parse.urlsplit(url).netloc


def read_csv_from_s3(bucket, key):
    s3_client = sess.client('s3')
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    # Python 3.8/3.9 can't download files over 2GB via HTTP, so file is 
    # streamed just in case
    csv_str = b''.join([
        chunk for chunk in resp['Body'].iter_chunks()
    ]).decode()
    
    # keepends=True to preserve newlines within Text fields
    csv_lines = csv_str.splitlines(keepends=True)
    csv_list = list(csv.reader(csv_lines, quotechar='"'))
    df = pd.DataFrame(csv_list[1:], columns=csv_list[0])
    
    return df


def do_grid_search(websites_df, pipe, parameters):
    """
    Find the best parameters for both the feature extraction and the classifier
    
    Return None
    """
    model_name = "_".join(pipe.named_steps.keys())
    grid_search = GridSearchCV(pipe, parameters, n_jobs=-1, verbose=2,
                               scoring='roc_auc')
    
    print(f"Performing grid search for {model_name}...\n")
    print("pipeline:", [name for name, _ in pipe.steps])
    print("scoring:", grid_search.scoring)
    print("parameters:", parameters)
    
    t0 = time()
    grid_search.fit(websites_df['Text'], websites_df['target'])
    print(f"done in {time() - t0:.3f}s\n")

    print(f"Best score: {grid_search.best_score_:.3f}")
    print("Best parameters set:")
    best_parameters = grid_search.best_estimator_.get_params()
    for param_name in sorted(parameters.keys()):
        print("\t%s: %r" % (param_name, best_parameters[param_name]))
        
    full_results = pd.DataFrame(grid_search.cv_results_) \
                                .set_index('rank_test_score') \
                                .sort_index()
    # Reorder so the most important information is first
    important_cols = ['params', 'mean_test_score', 'std_test_score']
    reordered_columns = important_cols + (full_results.columns.drop(important_cols).tolist())
    full_results = full_results[reordered_columns]
    
    print("Top 10 results:")
    print(full_results[important_cols].head(10))
    
    # Save results to CSV
    results_fname = model_name + "_results.csv"
    full_results.to_csv(os.path.join("results", results_fname))
    

def load_ecom_labels():
    """
    Loads ecom labels from JSON file to dict
    Return dict[string -> bool] which indicates whether a given website is ecommerce
    """
    with open(os.path.join("data", "ecom_labels.json")) as f:
        ecom_labels = json.load(f)
        
    return ecom_labels


def get_sample_output(ecom_labels):
    sample_output_fpath = os.path.join("data", "sample_output.csv")
    if os.path.exists(sample_output_fpath):
        with open(sample_output_fpath) as f:
            sample_output = pd.read_csv(f)
    else:
        s3 = sess.client('s3')
        output_keys = [
            x['Key'] for x in
            s3.list_objects_v2(Bucket="statsnz-covid-xmiles", Prefix="commoncrawl/processed_ccmain_bunches/CC-MAIN-2021-10/")['Contents']
        ]
        all_output = pd.concat((
            read_csv_from_s3("statsnz-covid-xmiles", key) for key in output_keys
        ))
        all_output['netloc'] = all_output['URL'].map(get_netloc)
        
        sample_output = all_output[all_output['netloc'].isin(ecom_labels.keys())].reset_index(drop=True)
        sample_output.to_csv(sample_output_fpath)
        
    return sample_output


def summarise_by_website(output, ecom_labels):
    """
    Merge the webpages/rows for each given website
    """
    websites = pd.DataFrame(
        output.groupby('netloc')['Text'].apply(' | '.join)
    ).reset_index()
    websites['target'] = [int(ecom_labels[netloc]) for netloc in websites['netloc']]
    websites['words'] = websites['Text'].map(gensim.utils.simple_preprocess)
    websites['tagged_words'] = [
        TaggedDocument(words=words, tags=[target]) 
        for words, target in zip(websites['words'], websites['target'])
    ]
    
    return websites
    

if __name__ == "__main__":
    # Setup
    sess = boto3.Session(profile_name="xmiles")
    ecom_labels = load_ecom_labels()
    sample_output = get_sample_output(ecom_labels)
    websites = summarise_by_website(sample_output, ecom_labels)
    
    # Config - use comments to remove parameters
    all_models = {
        'tfidf': TfidfVectorizer(),
        'svm': SGDClassifier(),
        'd2v': Doc2VecWrapper(),
        'lgb': lgb.LGBMClassifier(objective='binary', verbose=-1)
    }
    all_params = {
        'tfidf': {
            'max_df': (0.5, 0.75, 1.0),
#           'max_features': (None, 5000, 10000, 50000),
            'ngram_range': ((1, 1), (1, 2)),  # (1, 1)=unigrams, (1, 2)=unigramsORbigrams
            'stop_words': ('english', None),
            'norm': ('l1', 'l2')
        },
        'd2v': {
#             'vector_size': (50, 75),
#             'min_count': (5, 10),
#             'window': (5, 10)
        },
        'svm': {
#             'max_iter': (40, ),
            'alpha': (1e-5, 1e-6),
            'penalty': ('l2', 'elasticnet'),
#           'max_iter': (10, 50, 80)
        },
        'lgb': {
            'num_leaves': (31, 63, 127),
            'min_data_in_leaf': (10, 20, 40),
            'max_bin': (127, 255, 511)
        }
    }
    
    # Which tests to run? - use comments to remove models
    combinations_to_try = [
        ('tfidf', 'svm'), 
#         ('d2v', 'svm'), 
#         ('tfidf', 'lgb'), 
#         ('d2v', 'lgb'),
    ]
    # Translate combinations into pipelines and parameter grids
    pipelines_to_try = [
        Pipeline([(step, all_models[step]) for step in steps])
        for steps in combinations_to_try
    ]
    params_to_try = [
        {
            f"{step}__{param}": all_params[step][param] 
            for step in steps for param in all_params[step]
        }
        for steps in combinations_to_try
    ]
    
    for pipe, params in zip(pipelines_to_try, params_to_try):
        do_grid_search(websites, pipe, params)

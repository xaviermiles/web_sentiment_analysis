import pandas as pd
import numpy as np
import os
import glob
path="gdelt-2020-AU/"

# pd.set_option('display.max_columns', 150)

from collections import Counter

import matplotlib.pyplot as plt
import seaborn as sns
import ciso8601
from datetime import datetime
from dateutil import parser
import concurrent.futures
import time
import csv
import re

headers = ['gkg_id', 'date', 'source', 'source_name', 'doc_id', 
        'themes', 'locations', 'persons', 'orgs', 
        'tone', 'pos', 'neg', 'polarity', 'ard', 'srd',
        'wc', 
        'lexicode_neg', 'lexicode_pos', # c3.*
        'MACROECONOMICS', 'ENERGY', 'FISHERIES', 
        'TRANSPORTATION', 'CRIME', 'SOCIAL_WELFARE',
        'HOUSING', 'FINANCE', 'DEFENCE', 'SSTC',
        'FOREIGN_TRADE', 'CIVIL_RIGHTS', 
        'INTL_AFFAIRS', 'GOVERNMENT_OPS',
        'LAND-WATER-MANAGEMENT', 'CULTURE',
        'PROV_LOCAL', 'INTERGOVERNMENTAL',
        'CONSTITUTIONAL_NATL_UNITY', 'ABORIGINAL',
        'RELIGION', 'HEALTHCARE', 'AGRICULTURE',
        'FORESTRY', 'LABOUR', 'IMMIGRATION',
        'EDUCATION', 'ENVIRONMENT',
        'finstab_pos', 'finstab_neg', 'finstab_neutral',
        'finsent_neg', 'finsent_pos', 'finsent_unc',
        'opin_neg', 'opin_pos',
        'sent_pos', 'sent_neg', 'sent_pol'
]

''' Tone--- (floating point number) This is the average “tone” of the document as a whole.
The score ranges from -100 (extremely negative) to +100 (extremely positive). Common
values range between -10 and +10, with 0 indicating neutral. '''

''' Positive Score--- (floating point number) This is the percentage of all words in the article
that were found to have a positive emotional connotation. Ranges from 0 to +100. '''

''' Negative Score--- (floating point number) This is the percentage of all words in the
article that were found to have a positive emotional connotation. Ranges from 0 to
+100. '''

''' Polarity--- (floating point number) This is the percentage of words that had matches in
the tonal dictionary as an indicator of how emotionally polarized or charged the text is.
If Polarity is high, but Tone is neutral, this suggests the text was highly emotionally
charged, but had roughly equivalent numbers of positively and negatively charged
emotional words. '''


''' c 3.4 --- negative positive ( positive word preceded by a negation (used to convey negative sentiment) and, 
    c 3.3 --- negative negative ( a negative word preceded by a negation, used to convey positive sentiment )'''

def merge_csv():
    start = time.time()
#     df_each = (pd.read_csv(f,names=headers,header=None) for f in files)
#     df_merge = pd.concat(df_each, ignore_index=True)
#     df = df_merge.copy()
#     end = time.time()
#     print(f"Processing the data took : {round(end-start,2)} seconds")
    
    header_written = False
    ''' Pandas method takes a longer timer for merge and hence have made it in pure python to boost up'''

    with open('gdelt_2020_au.csv', 'w', newline="") as fout:
        wout = csv.writer(fout, delimiter=',')
        files = [x for x in glob.glob("gdelt-2020-AU/2020*.csv") if x != 'gdelt_2020_au.csv']
        for file in files:
            print("processing {}".format(file))
            with open(file) as fin:
                cr = csv.reader(fin,delimiter=',')
                if not header_written:
                    wout.writerow(headers)
                    header_written = True
                wout.writerows(cr)    
    
    end = time.time()
    print(f"Processing the data took : {round(end-start,2)} seconds")
      


def date_process(dff):
    start = time.time()


    dff['date'].dropna(inplace=True)
    



    dff['date'] = pd.to_datetime(dff['date'], format="%Y%m%d%H%M%S")


       
    end = time.time()
    print(f"Processing the datetime took : {round(end-start,2)} seconds")

    
    return dff

merge_csv()

df = pd.read_csv("gdelt_2020_au.csv")

print(df.shape)
dff = df.copy()
df1 = date_process(dff)
print(df1.shape)
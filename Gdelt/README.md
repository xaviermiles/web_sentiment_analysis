# GDELT
The GDELT (Global Database of Events, Language and Tone) website provides the Global Knowledge Graph (GKG), Global Entity Graph (GEG) and various other databases related to global news coverage.

---
Contents:
1. Background: Global Knowledge Graph
2. Overview of files/subdirectories
3. Extra: Global Entity Graph
---


## 1. Background: Global Knowledge Graph
### What is GKG?
The GKG summarises the people, locations, organisations, themes, emotions, and events included in the global news. There is 2,300 emotions and themes measured in realtime.
The news articles are fed into GDELT Translingual to translate 65 languages into English in realtime, which means that non-English articles can be incorporated into the GKG results. This realtime component is important since it means that there is no lag between English and non-English articles being included in the GKG results, which would confuse/muddy any analysis on the development of news events in different parts of the world. This is not as important for this project since the goal is to extract articles from/about New Zealand, which will primarily tend to be in English.

The data is recorded in 15-minute batches, which seem to be published/available within about five minutes.

### Data Retrieval
Methods to retrieve this data: Google BigQuery, raw data files, gdeltPyR Python package.

Google BigQuery is a “serverless data analytics platform” which uses SQL syntax and charges for running queries. The GKG dataset is about 12.7 TB and is included as a publicly available dataset on BigQuery. BigQuery charges 5 USD/TB for on-demand pricing, and 2000 USD/month for 100 “slots” of processing capacity (it is not clear how many slots would be necessary to handle 12.7 TB relatively quickly).

The GKG raw data files are zipped tab-delimited files (.csv.zip extensions) that can be downloaded from HTTP URL sites. The list of all available files from GDELT is included at http://data.gdeltproject.org/gdeltv2/masterfilelist.txt. The structure of the GKG file sites is http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.gkg.csv.zip (using 24-hour date-time), and these are aggregated into 15-minute batches. For example, a GKG batch generated at 3:30PM on 03 February 2020 could be retrieved from http://data.gdeltproject.org/gdeltv2/20200203153000.gkg.csv.zip.

The gdeltPyR Python package offers a simple interface to request the GKG dataset (and the other GDELT datasets). This works by creating parallel HTTP GET requests to the raw data files, so it tends to be very quick. The requests are restricted to entire days (ie. specifying dates NOT datetimes), but every 15-minute batch for the specified dates are returned. If requesting up to the current date, then it will return the dataset up to the latest 15-minute batch. It will print a warning message for invalid URLs (datetimes for which GDELT is missing data) or if no data is returned by a valid URL (not sure why this happens).

### Data Structure (raw data files)
These are tab-delimited files with each line corresponding to a published article. The information/variables included in each of these is explained fully in http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.pdf.

The most important variables are
- V2DOCUMENTIDENTIFIER (5th field) – unique external identifier for the source document.
- V1LOCATIONS (10th field) – list of all locations found in the text, extract through Leetaru (2012) algorithm. (This has semicolon-delimited blocks for the different locations, with #-delimited fields for different formats of the locations.) This can be used to identify if New Zealand is mentioned in the text. The algorithm is run so that it doesn’t miss any locations (near-zero false negative rate), but this means it may incorrectly include unnecessary countries (slightly higher false positive rate).
- V1PERSONS (12th field) – list of all person names mentioned in the document. (This has semicolon-delimited blocks.) This could be used to investigate the sentiment over time towards public figures related to COVID-19/wellbeing in New Zealand.
- V1.5TONE (16th field) – six core emotional dimensions. (This has comma-delimited floating point numbers.) This reflects the emotional tone of the text (e.g. how positive it is, how emotionally charged it is) using GDELTs inbuilt sentiment analysis. This is “designed to offer a general-purpose tone indicator”, but it is not made clear what algorithms/processes are used to derive it. 
- V2GCAM (18th field) – the result of the GCAM system being run on the text. (See next section for explanation of structure.) This reflects what sentiments/themes are included in the text using a variety of references.

### V2GCAM Structure (raw data files)
(This has comma-delimited blocks, and colon-delimited key/value pairs.)

The first entry is always the word count eg. “wc:345” means there is 345 words in the article.

The next entries have keys structured by “DictionaryID.DimensionID” eg. SentiWordNet has DictionaryID of 10 and the Positive dimension has DimensionID of 1 so this would be represented by 10.1. There is a “c” or “v” to represent if the dictionary has counts (positive integers) or values (floating point), respectively. In general, the counts represent how many words in the text are associated with that dictionary’s dimension, and the values are an average of the numeric scores for each word in the article (this depends on what the dictionary/lexicon returns).

The absence of a key means that the count of words in the text associated with that dictionary/dimension is zero. This was done to save space.
The complete list of dictionary/dimension keys is available at http://data.gdeltproject.org/documentation/GCAM-MASTER-CODEBOOK.TXT. 

__Example__: “wc:125,c2.21:4,c10.1:40,v10.1:3.21111111”

This document has 125 words (wc:125). Four of the words were found in the Bodypt dimension of the General Inquirer lexicon (c2.21:4). 40 of the words were found in the Positive dimension of the SentiWordNet 3.0 lexicon (c10.1:40). The average numeric score of all the words found in the Positive dimension of the SentiWordNet lexicon was 3.21111111 (v10.1:3.21111111).


## 2. Overview of files/subdirectories
In the diagram below, anything with "[u]" was unversioned (as per gitignore) but not all unversioned files/subdirectories are included.
```
Gdelt
├── __init__.py
├── README.md
├── gkg_gdelt2_process.py
├── gdelt_utils.py
├── data/ [u]
│
├── eda_KA/
├── eda_XM/
│
├── swa
│   ├── LOOKUP-GKGTHEMES.csv
│   ├── SWA_themes_LM.txt
│   ├── themes_NLP_SWA.ipynb
│   │
│   ├── setup_process.md
│   ├── swa_gdelt_process.py
│   ├── swa_merge_from_s3.py
│   ├── swa_monthly_tone.py
│   ├── swa_monthly_final.py
│   ├── swa_overall.py
│   │
│   └── swa_insights.ipynb
│
├── Gdelt_swa/
│
├── quality_checks
│   ├── __init__.py
│   └── compare_indicators.ipynb
│
└── process_v2
    ├── __init__.py
    ├── create_gdelt_raw.sql
    ├── gkg_raw_to_db.py
    │
    ├── get_daily_averages.sql
    └── themes_queries.sql
```

The _gkg_gdelt2_process.py_ script was inherited from someone who had previously worked to download a subset of this dataset from the raw data files, into a collection of CSVs. The _gdelt_utils.py_ defines constants that are the headers of the output files from this original processing script.

The files in _eda_KM/_ and _eda_XM/_ were used to explore the retrieved data, including comparing the sentiment of different countries, and try to merge the output CSVs.

The _themes_NLP_SWA.ipynb_ notebook was used to get low-level themes related to high-level themes, which were then used to construct theme-specific sentiment indicators.
The five python scripts in _swa/_ comprise a full process to download the GDELT data for multiple countries, merge the output CSVs, aggregate the sentiment to monthly, filter the news articles based on themes mentioned, and calculate rolling averages and standard deviations. This process uses a S3 bucket to store CSVs and what each of the five python script does are explained in _swa/setup_process.md_.
The _swa_insights.ipynb_ notebook was used to get insights and plots after the monthly per-country data was collated.

TODO: explainer about _Gdelt_swa/_

The _quality_checks/compare_indicators.ipynb_ notebook was used to generate plots and perform statistical hypothesis testing related to trying to verify the quality of the sentiment indicator derived from GDELT.

The _process_v2/_ aimed to replicate the process in _swa/_, but with using a Postgres database (AWS RDS) as storage rather than CSVs in S3 bucket. This process also attempts to prevent any syndicated/republished news articles being counted towards the averages multiple times by first grouping together any articles with the same date, positive score, negative score, and word count.
The database table is created by _create_gdelt_raw.sql_, and the data is downloaded and inserted into the table by _gkg_raw_to_db.py_.
The SELECT query in _get_daily_averages.sql_ can be used to get daily per-country sentiment from the raw table, which is also broken down by whether the article was published in the country or overseas.
The SELECT queries in _themes_queries.sql_ work towards deriving the daily theme-specific sentiment from the raw table (while still breaking down by country mentioned and country of origin, and controlling for syndication).


## 3. Extra: Global Entity Graph
### What is GEG?
The Global Entity Graph (GEG) is a dataset developed by GDELT more recently than GKG but serves a very similar purpose. GEG uses Google’s Cloud Natural Language API to evaluate and quantify sentiment within news articles. The sentiments/tones included in GKG (both V1.5TONE and V2GCAM) are measured through “classical grammatical, statistical and machine learning algorithms”. This means that GEG and GKG both provide measures of sentiment within global news articles and differ in their process to derive sentiment.

Introduction to GEG: https://blog.gdeltproject.org/announcing-the-global-entity-graph-geg-and-a-new-11-billion-entity-dataset/.

For a comparison of the sentiments in GEG vs GKG, refer to https://blog.gdeltproject.org/geg-comparing-classical-bag-of-word-and-neural-sentiment-algorithms/ .

### Data Retrieval
Methods to retrieve this data: Google BigQuery, raw data files.

The raw data files are UTF-8 newline-delimited JSON files, which can be downloaded as g-zipped files from HTTP URL sites. The list of all available GEG files is included at http://data.gdeltproject.org/gdeltv3/geg_gcnlapi/MASTERFILELIST.TXT. The structure of the GEG file sites is http:///data.gdeltproject.org/gdeltv3/YYYYMMDDHHMMSS.geg-gcnlapi.json.gz (using 24-hour date-time), and these are aggregated into 1-minute batches. For example, a GEG batch generated at 3:34PM on 3 February 2015 could be retrieved from http:///data.gdeltproject.org/gdeltv3/20150203153400.geg-gcnlapi.json.gz.

### Differences from GKG
\*\*\* GEG only uses a random sample of the news coverage monitored by GDELT, while GKG uses all the news coverage. This is a big downside, since this will reduce the amount of news articles about New Zealand.

GKG is provided in 15-minute batches, while GEG is provided in 1-minute batches. This means that GEG can be process on a more ongoing basis, but a given GEG batch is more likely to not include any New Zealand articles.

Note from Intro to GEG webpage: _“Note that the URLs found within a given 15-minute file historically have aligned with those found in the GKG for the same period (though representing just a small subsample of them), but in future this will be increasingly decoupled as GDELT 3.0 launches, meaning that analyses looking across both GKG and GEG annotations will need to use a rolling window to match them.”_

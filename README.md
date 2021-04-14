# COVID19 SENTIMENT ANALYSIS DOCS


## GDELT - Global Knowledge Graph
The GDELT (Global Database of Events, Language and Tone) website provides the Global Knowledge Graph (GKG), Global Entity Graph (GEG) and various other databases related to global news coverage.

### What is GKG?
The GKG summarises the people, locations, organisations, themes, emotions, and events included in the global news. There is 2,300 emotions and themes measured in realtime.
The news articles are fed into GDELT Translingual to translate 65 languages into English in realtime, which means that non-English articles can be incorporated into the GKG results. This realtime component is important since it means that there is no lag between English and non-English articles being included in the GKG results, which would confuse/muddy any analysis on the development of news events in different parts of the world. This is not as important for this project since the goal is to extract articles from/about New Zealand, which will primarily tend to be in English.

The data is recorded in 15-minute batches, which seem to be published/available within about five minutes.

### Data Retrieval
Methods to retrieve this data: Google BigQuery, raw data files, gdeltPyR Python package.

Google BigQuery is a “serverless data analytics platform” which uses SQL syntax and charges for running queries. The GKG dataset is about 12.7 TB and is included as a publicly available dataset on BigQuery. BigQuery charges $5 / TB for on-demand pricing, and $1700-2000 / month for 100 “slots” of processing capacity (it is not clear how many slots would be necessary to handle 12.7 TB relatively quickly).

The GKG raw data files are zipped tab-delimited files (.csv.zip extensions) that can be downloaded from HTTP URL sites. The list of all available files from GDELT is included at http://data.gdeltproject.org/gdeltv2/masterfilelist.txt. The structure of the GKG file sites is http:///data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.gkg.csv.zip (using 24-hour date-time), and these are aggregated into 15-minute batches. For example, a GKG batch generated at 3:30PM on 03 February 2020 could be retrieved from http:///data.gdeltproject.org/gdeltv2/20200203153000.gkg.csv.zip.

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


## GDELT - Global Entity Graph
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


## Common Crawl
This is a Amazon dataset hosted in a public S3 bucket, but can also be retrieved via HTTPS links. Downloading from the HTTPS links means that the g-zipped files can be accessed without an AWS setup, but will lose the benefits of complimentary AWS services. 

The _cc_process.py_ script supports retrieval of both the original dataset (CC-MAIN) and the subset which only includes news articles (CC-NEWS). This script extracts the scraped webpages with nz top-level domain (e.g. ".co.nz" or ".org.nz") by downloading the CC datasets using HTTPS links and then searching the WARC files for the relevant webpages, extracting these webpage's text, and saving this information to CSV files (along with the publish-datetime and webpage's URL). There is a separate CSV file produced for each '.warc.gz' file. This process works okay for CC-NEWS as this dataset is small-to-medium size (\~6,500 files for Jan 2020-Feb 2021), but is infeasible for CC-MAIN due the number of files required to be downloaded (\~700,000 files for Jan 2020-Feb 2021). During initial testing in the AWS environment, it seemed like it took _cc_process.py_ about 30 minutes per 100 files, so processing Jan 2020-Feb 2021 would take about 34 hours for CC-NEWS and about 150 days for CC-MAIN. Each '.warc.gz' file is 1 GB (before unzipping).

A more efficient process would send the query to the data contained in the S3 bucket and only download the NZ webpages, since this would reduce the amount of data required to be downloaded and may provide more efficient processing. The two main options for querying data in S3 buckets is _S3 Select_ and _Amazon Athena_. Both:

- allow SQL-style queries to CSV, JSON or [Parquet](https://databricks.com/glossary/what-is-parquet) datasets.
- support compressed files. S3 Select: GZIP, BZIP2. Amazon Athena: GZIP, SNAPPY, ZLIB, LZO, BZIP2.

S3 Select is more designed for ad hoc queries and Athena is designed more for big data, so Athena will likely incur larger costs (and require activation). Either could be used for the commoncrawl files as these are stored in the public S3 bucket _commoncrawl_ in GZIP-ed Parquet format. __It seems like S3 Select should be sufficiently powerful for this use-case.__

### CC-MAIN Directory Structure
The complete data structure is described at https://commoncrawl.org/the-data/get-started/.

.warc = Web ARChive format. Used to store scraped information from webpages. These are gzipped (compressed) before being stored in the _commoncrawl_ S3 bucket.

The '.warc.gz' files are released in batches about every 5 weeks. The batches are listed at https://index.commoncrawl.org/collinfo.json and use the naming convention CC-MAIN-YYYY-WW (YYYY = year, WW = week number). The paths for the .warc.gz files which contain the scraped information in each batch can be found at https://commoncrawl.s3.amazonaws.com/{BATCH_NAME}/warc.paths.gz or s3://commoncrawl/{BATCH_NAME}/warc.paths.gz. For example, the paths for the .warc.gz files in the CC-MAIN-2021-10 batch is available at https://commoncrawl.s3.amazonaws.com/CC-MAIN-2021-10/warc.paths.gz or s3://commoncrawl/CC-MAIN-2021-10/warc.paths.gz.

An example of an individual filepath is https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-10/segments/1614178347293.1/warc/CC-MAIN-20210224165708-20210224195708-00000.warc.gz OR 
s3://commoncrawl/crawl-data/CC-MAIN-2021-10/segments/1614178347293.1/warc/CC-MAIN-20210224165708-20210224195708-00000.warc.gz. This is the first file in the _CC-MAIN-2021-10_ batch and contains 44,408 scraped webpages, of which 98 have URLs with '.nz' as their top-level domain.

## CC-NEWS Directory Structure
Information about this dataset can be found at https://commoncrawl.org/2016/10/news-dataset-available/.

The '.warc.gz' files are released every hour or two, which usually results in 1 GB files.
The individual files are available at https://commoncrawl.s3.amazonaws.com/crawl-data/CC-NEWS/YYYY/MM/ or s3://commoncrawl/crawl-data/CC-NEWS/YYYY/MM/ (with YYYY = year, MM = month).

An example of an individual filepath is https://commoncrawl.s3.amazonaws.com/crawl-data/CC-NEWS/2021/01/CC-NEWS-20210101014736-01421.warc.gz or s3://commoncrawl/crawl-data/CC-NEWS/2021/01/CC-NEWS-20210101014736-01421.warc.gz. This is the first file for January 2021 and contains 34,277 scraped webpages, of which 58 have URLs with '.nz' as their top-level domain.

## Hedonometer
The main implementation is at http://hedonometer.org/, which provides a daily index of the positivity of people’s tweets (Twitter). This is informed by the Twitter Decahose API, which provides a 10% random sample of all posts on Twitter (approx. 50 million tweets per day in 100GB of raw JSON).  The daily score reflects which words are being use most (about 200 million unique per day) and their associated positivity, and is constructed using a bag-of-words model. More formally, the weighted average level of happiness for a given text T is calculated as

h_avg (T)= ∑_(i=1)^N h_avg(w_i) p_i,

where h_avg(w_i) is their estimate of the average happiness of a given word w_i, p_i is the normalised frequency of that word w_i, and there is N unique words.

The methodology behind this hedonometer is described in _Temporal patterns of happiness and information in a global social network: Hedonometrics and Twitter_ (2011; Dodds et al.).

### How did they get “happiness scores” for each word?
Using Amazon Mechanical Turk, they paid users to rate how 10,222 different words made them feel on a (integer) scale from 1-9, obtaining 50 individual evaluations per word. They are confident in the derived happiness scores as they strongly agree with the scores obtained for 1034 words in the Affective Norms for English Words (ANEW) study (correlation coefficient r = 0.944 and p-value < 10^-10), despite the new scores being obtained from global users (Mechanical Turk) and the scores from ANEW being obtained exclusively from University of Florida students.
Their English happiness scores can be viewed on http://hedonometer.org/words/labMT-en-v2/ or downloaded from https://hedonometer.org/api/v1/words/?format=json&wordlist__title=labMT-en-v2.

### Combining the scores for each word.
The overall happiness score ignores words which have a happiness score between 4 and 6 (somewhat neutral). This exclusion range is a tuning parameter; a smaller range will provide more robustness to outlier/extreme events but less day-to-day sensitivity, and a wider range will provide more sensitivity but less robustness. This exclusion of somewhat neutral words results in only 3,686 of the original 10,222 words being used (23%).

### Other related 
The main indicator of public sentiment is the _Time Series of Happiness_, which is informed by posts on Twitter. There are other related projects on the site: _Happiness of Stories_ which is informed by books and movies, _Happiness of the News_ which is informed by the New York Times and CBS, and _Happiness of Outside_ which reflects a collection of articles on a blogging site about outdoor activities. These projects all present per word average happiness shift between two bodies of text (e.g. newspaper section, article), and do not include time series plots. The authors are strong proponents of the word shift presentation, as it allows for more information about why sentiment has changed between two time periods (or bodies of text). 

The NZ news sentiment lineplot/visualisation could include these word shifts as additional information like how hedonometer presents these as overlays. For a given day, the hedonometer uses these word shifts to show any words which are being used significantly more or less than the previous week. The hedonometer also uses Wikipedia to automatically detect important/significant events on any given day.

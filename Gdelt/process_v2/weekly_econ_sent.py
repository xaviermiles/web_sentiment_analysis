"""
Functions related to "Weekly Economic Sentiment" (Economic - Confidence) on the 
COVID-19 Data Portal. 
"""
import numpy as np
import pandas as pd
import psycopg2

import postgres_config

CONNECTION_DETAILS = (
    f"host={postgres_config.HOST} dbname=gdelt user=postgres "
    f"password={postgres_config.PASSWORD}"
)


def make_economic_theme():
    """
    Identifies which low-level themes are related to "economics" using 
    simple-string searching, which identifies which low-level themes include
    any of the substrings in `substrs_to_search` list. 
    
    There is different from the other (more experimental) indicators in 
    `./create_themes_ref.py`:
      - Those use a NLP model to identify low-level themes which are related
        to specified topics, whereas this uses sub-string searching.
      - Those using a count threshold to filter out low-level themes, whereas
        this does not. From testing, using threshold=100 removes ~0.5% of the
        economic-articles so it doesn't make *much* difference.
    """
    substrs_to_search = ['ECON','UNEMPLOY','GOVERNOR','AUSTER','DISEASE',
                         'CORONA','COVID','FINANC','MARKET']
    lookup = pd.read_csv("../data/LOOKUP-GKGTHEMES.csv", sep = "\t")
    low_level_themes = [
        theme_i for theme_i in lookup['theme']
        if any(substr in theme_i for substr in substrs_to_search)
    ]
    row = ('economic', low_level_themes)
    insert_query = f"INSERT INTO themes_ref VALUES %s;"
    with psycopg2.connect(CONNECTION_DETAILS) as conn:
        with conn.cursor() as cur:
            cur.execute(insert_query, (row,))
            
            
def export_nz_econ_sent():
    """
    Constructs the "Weekly Economic Sentiment" .csv data file.
    """
    select_query = """
    SELECT date, from_onshore, avg_tone, num_articles
    FROM daily_tone
    WHERE country = 'NZ' AND theme = 'economic'
    ORDER BY date
    """
    col_names = ['date', 'from_onshore', 'avg_tone', 'num_articles']
    with psycopg2.connect(CONNECTION_DETAILS) as conn:
        with conn.cursor() as cur:
            cur.execute(select_query)
            raw = cur.fetchall()
    econ = pd.DataFrame(raw, columns=col_names)
    econ['date'] = pd.to_datetime(econ['date'], format='%Y-%m-%d')
    econ['avg_tone'] = econ['avg_tone'].astype('float')
    
    # Don't want to adjust for whether an article was published in NZ/overseas,
    # so will use num_articles column to work backwards to a simple average of
    # all articles which mention NZ.
    econ['num_articles_weights'] = (
      econ['num_articles'] /
      econ.groupby('date')['num_articles'].transform('sum')
    )
    econ['weighted_avg_tone'] = econ['avg_tone'] * econ['num_articles_weights']
    simple_econ = econ.groupby('date') \
                      .agg({'weighted_avg_tone': 'sum',
                            'num_articles': 'sum'}) \
                      .rename({'weighted_avg_tone': 'avg_tone'}, axis=1)
    simple_econ.to_csv("../data/nz_daily_econ_sentiment.csv")
    
    # Aggregate to weekly - remove partial weeks at start/end
    idx_first_sunday = list(simple_econ.index.day_name()).index('Sunday')
    idx_last_saturday = len(simple_econ.index) - list(simple_econ.index.day_name()[::-1]).index('Saturday')
    filt_simple_econ = simple_econ.iloc[idx_first_sunday:idx_last_saturday]
    
    weekly_econ = filt_simple_econ.resample('W-Sat') \
                                  .agg({'avg_tone': 'mean', 
                                        'num_articles': 'sum'})
    weekly_econ.to_csv("../data/nz_weekly_econ_sentiment.csv")


if __name__ == "__main__":
    export_nz_econ_sent()

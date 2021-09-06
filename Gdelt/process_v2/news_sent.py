"""
Functions related to "News Sentiment" (Social - Life Satisfaction) on the 
COVID-19 Data Portal. 
"""
from datetime import timedelta

import numpy as np
import pandas as pd
import psycopg2

import postgres_config

CONNECTION_DETAILS = (
    f"host={postgres_config.HOST} dbname=gdelt user=postgres "
    f"password={postgres_config.PASSWORD}"
)


def export_monthly_news_sent(daily_wide):
    """
    Exports monthly News Sentiment.
    """
    # Drop dates from partial months at start/end
    start_first_whole_month = daily_wide.index.min() + pd.offsets.MonthBegin(0)
    end_last_whole_month = daily_wide.index.max() + timedelta(days=1) \
                                                  - pd.offsets.MonthEnd(1)
    filt_mask = np.logical_and(daily_wide.index >= start_first_whole_month,
                               daily_wide.index <= end_last_whole_month)
    daily_wide_filt = daily_wide[filt_mask]
    
    # Aggrgate to monthly and export
    agg_funcs = {
        c: 'mean' if c.startswith('avg_tone__')
        else 'sum' # c starts with 'num_articles__'
        for c in daily_wide_filt.columns
    }
    monthly_wide = daily_wide_filt.resample('M').agg(agg_funcs)
    monthly_wide.to_csv("../data/monthly_news_sentiment.csv")


def export_rolling_avg_news_sent(daily_wide):
    """
    Exports 7-day rolling average News Sentiment. Result is set to right edge of
    window and has daily frequency.
    """
    agg_funcs = {
        c: 'mean' if c.startswith('avg_tone__')
        else 'sum' # c starts with 'num_articles__'
        for c in daily_wide.columns
    }
    daily_wide_rolling = daily_wide.rolling(7).agg(agg_funcs).iloc[6:, ]
    daily_wide_rolling.to_csv("../data/rolling_7day_avg_news_sentiment.csv")


def get_daily_wide():
    """
    Extracts overall sentiment data from daily_tone table and pivots from long
    to wide format.
    """
    select_query = """
    SELECT date, country, from_onshore, avg_tone, num_articles
    FROM daily_tone
    WHERE theme = 'ALL'
    ORDER BY date, country, from_onshore
    """
    col_names = ['date', 'country', 'from_onshore', 'avg_tone', 'num_articles']
    with psycopg2.connect(CONNECTION_DETAILS) as conn:
        with conn.cursor() as cur:
            cur.execute(select_query)
            raw = cur.fetchall()
    daily = pd.DataFrame(raw, columns=col_names)
    daily['date'] = pd.to_datetime(daily['date'], format='%Y-%m-%d')
    daily['avg_tone'] = daily['avg_tone'].astype('float')
    
    # Pivot long to wide
    daily['from_onshore'] = [
        'onshore' if from_onshore_i else 'worldwide'
        for from_onshore_i in daily['from_onshore']
    ]
    daily_wide = daily.pivot(index='date',
                             columns=['country', 'from_onshore'],
                             values=['avg_tone', 'num_articles'])
    daily_wide.columns = ['__'.join(c) 
                          for c in daily_wide.columns.to_flat_index()]
    
    return daily_wide


def export_news_sent():
    """
    Exports both News Sentiment data files.
    """
    daily_wide = get_daily_wide()
    export_monthly_news_sent(daily_wide)
    export_rolling_avg_news_sent(daily_wide)
    

if __name__ == "__main__":
    export_news_sent()

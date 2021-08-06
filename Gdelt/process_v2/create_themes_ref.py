"""
If using this to replace data/relationships already stored in the "themes_ref"
table, then you need to DROP TABLE before running this script.
"""

import os

import pandas as pd
import psycopg2

import postgres_config

# Load Spacy NLP model
try:
    import en_core_web_lg
except ModuleNotFoundError:
    print(f"Downloading Spacy NLP model: en_core_web_lg\n"
          "**This is only necessary once**")
    from spacy.cli import download
    download('en_core_web_lg')
    import en_core_web_lg


def load_themes_lookup():
    source_url = ("http://data.gdeltproject.org/api/v2/guides/"
                  "LOOKUP-GKGTHEMES.TXT")
    fpath = "../data/LOOKUP-GKGTHEMES.csv"
    
    themes_lookup = pd.read_csv(source_url, delimiter='\t', 
                                names=['theme','count'])
    # Don't include strings that are empty or just numbers
    themes_lookup['theme_parts'] = [
        [x for x in theme.lower().split('_') if not x.isdecimal() and len(x) > 0]
        for theme in themes_lookup['theme']
    ]
    themes_lookup.to_csv(fpath, sep='\t', index=False)  # for later inspection
    return themes_lookup
    

def get_theme_mappings(high_level_themes, nlp_threshold, count_threshold):
    themes_lookup = load_themes_lookup()
    # Count is for all-time ie. from 2015 - present
    themes_lookup = themes_lookup[themes_lookup['count'] > count_threshold]
    nlp = en_core_web_lg.load()
    
    out = []
    for high_level in high_level_themes:
        high_level_nlp = nlp(high_level)
        
        # Includes rows/themes if any part of the (low-level) theme is similar 
        # enough to the high-level theme.
        mask = [
            any(
                nlp(theme_part).similarity(high_level_nlp) > nlp_threshold 
                for theme_part in theme_parts
            )
            for theme_parts in themes_lookup['theme_parts']
        ]
        low_level_themes = themes_lookup[mask]['theme'].tolist()
        print(f"{theme} => {low_level_themes}")
        out.append(
            (theme, low_level_themes)
        )
    return out


def write_themes_mappings_to_db(theme_mappings):
    create_table_query = """
    CREATE TABLE themes_ref (
        high_level    TEXT    PRIMARY KEY,
        low_levels    TEXT[]  NOT NULL
    );
    """
    insert_query = (
        "INSERT INTO themes_ref VALUES " + 
        ','.join(['%s'] * len(theme_mappings)) + ';'
    )
    connection_details = (
        f"host={postgres_config.HOST} dbname=gdelt user=postgres "
        f"password={postgres_config.PASSWORD}"
    )
    # Add row to get overall indicator:
    theme_mappings = ['ALL', []] + themes_mappings
    
    with psycopg2.connect(connection_details) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            cur.execute(insert_query, theme_mappings)
            
            
def get_theme_specific_sentiment():
    # perform sql query to get theme indicators
    pass


if __name__ == "__main__":
    count_threshold = 100
    nlp_threshold = 0.6
    high_level_themes = [
        'housing',
        'unemployment',
        'coronavirus',
    ]
    
    theme_mappings = get_theme_mappings(high_level_themes, 
                                        nlp_threshold, count_threshold)
    write_themes_mappings_to_db(theme_mappings)
    # get_theme_specific_sentiment()

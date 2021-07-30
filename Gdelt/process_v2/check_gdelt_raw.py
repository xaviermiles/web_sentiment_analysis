import pandas as pd
from sqlalchemy import create_engine

import postgres_config

def main():
    engine = create_engine(
      f"postgresql://postgres:{postgres_config.PASSWORD}@"
      f"{postgres_config.HOST}:5432/gdelt"
    )
    dat = pd.read_sql_query('select * from "gdelt_raw" limit 100', con=engine)
    print(dat.shape)
    print(dat.head())

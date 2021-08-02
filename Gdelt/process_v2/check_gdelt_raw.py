import pandas as pd
from sqlalchemy import create_engine

import postgres_config


def main():
    engine = create_engine(
      f"postgresql://postgres:{postgres_config.PASSWORD}@"
      f"{postgres_config.HOST}:5432/gdelt"
    )
    print("Number of rows:", 
          len(pd.read_sql_query('select datetime from "gdelt_raw"', con=engine)))
    dat = pd.read_sql_query('select * from "gdelt_raw" limit 100', con=engine)
    print("Sample data:")
    print(dat.head(10))
    
  
if __name__ == "__main__":
  main()

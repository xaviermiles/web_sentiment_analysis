import boto3
import botocore
import s3fs
import pandas as pd
import glob
from functools import reduce
import time

session = boto3.Session(profile_name='kandavar_processing')
s3 = session.client('s3')
bucket_name = 'statsnz-covid-kandavar'
Year = [i for i in range(2015, 2022)]
country = ['au','ca', 'uk', 'nz']

Housing = ['econ_housing_prices',
 'wb_612_housing_finance',
 'wb_817_land_and_housing',
 'wb_904_housing_markets',
 'wb_2186_social_housing',
 'wb_870_housing_construction',
 'wb_2187_rental_housing',
 'wb_1722_housing_policy_and_institutions',
 'wb_871_housing_subsidies',
 'wb_1594_housing_allowance',
 'wb_2188_affordable_housing_supply',
 'wb_2184_housing_finance_for_the_poor',
 'wb_869_housing_laws_and_regulations']

Unemployment =['unemployment',
 'wb_2747_unemployment',
 'wb_1649_unemployment_benefits',
 'wb_2678_cyclical_unemployment',
 'wb_2809_unemployment_insurance_reforms',
 'econ_cost_of_living']

Mental_health = ['crisislex_c03_wellbeing_health',
 'wb_1430_mental_health',
 'tax_disease_mental_illness',
 'tax_disease_mental_disorders',
 'tax_fncact_mental_health_counselor',
 'tax_disease_mental_fatigue',
 'tax_disease_mental_disorder_of_mother']

Wellbeing = ['crisislex_c03_wellbeing_health']

Vaccination =['health_vaccination', 'wb_1459_immunizations']

Coronavirus =['tax_disease_coronavirus', 'tax_disease_coronaviruses', 'tax_disease_coronavirus_infections']




def read_from_s3(filename,c):
    
    session = boto3.Session(profile_name='kandavar_processing')
    s3 = session.client('s3')
    bucket_name = 'statsnz-covid-kandavar'
    
    
    obj = s3.get_object(Bucket = bucket_name, Key = 'G_from_2015/merged/'+filename)
    

    d = pd.read_csv(obj['Body'], parse_dates = ['date'], usecols = [1,3,5,9])

     
    d['date'] = d.date.dt.date
    
    d['source_name'].dropna(inplace=True)
    d.dropna(subset = ['themes'], inplace = True)
    d['source_name'] = [x if f'.{c}' in str(x) else None for x in d.source_name]          # filtering only 'from country news' eg Newzealand '.nz' domains
    d = d.mask(d['source_name'].eq('None')).dropna().reset_index(drop=True)
    
    print(f"Read - {filename}")
    print(d.info())
    return d

def themes_filter(df):
    df.dropna(subset = ['themes'], inplace = True)
    df['themes'] = df['themes'].apply(lambda text: text.lower())
    df['themes'] = df['themes'].apply(lambda text: text.split(';'))

    
    return df

def themes_interest(df,t, t_name):
    df[f'themes_filtered_{t_name}'] = df['themes'].apply(lambda x: [f'{t_name}' for y in x if y in t])
    df[f'themes_filtered_{t_name}'] = df[f'themes_filtered_{t_name}'].map(lambda d: 'other' if len(d)<1 else f'{t_name}')

    return df

def match_themes(df):
    themes = {'Housing': Housing, 'Unemployment':Unemployment , 'Mental_health':Mental_health, 'Wellbeing':Wellbeing, 'Vaccination':Vaccination, 'Coronavirus': Coronavirus}
    print("Matching themes")
    for k,v in themes.items():
        df = themes_interest(df,v,k)
        
    df.drop('themes', axis=1, inplace=True)
    
    return df

def filter_merge(df,c):
    
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    t1 = df.query('themes_filtered_Housing == "Housing"').resample('M', on='date').agg({'tone':[('housing', 'mean')], 'source_name': [('no.articles_Housing', 'count')]}).reset_index()
    t1.columns = [col[1] for col in t1.columns.values]
    t1.columns.values[0] = 'date'
    
    t2 = df.query('themes_filtered_Unemployment == "Unemployment"').resample('M', on='date').agg({'tone':[('Unemployment', 'mean')], 'source_name': [('no.articles_Unemployment', 'count')]}).reset_index()
    t2.columns = [col[1] for col in t2.columns.values]
    t2.columns.values[0] = 'date'   
    
    t3 = df.query('themes_filtered_Mental_health == "Mental_health"').resample('M', on='date').agg({'tone':[('Mental_health', 'mean')], 'source_name': [('no.articles_Mental_health', 'count')]}).reset_index()
    t3.columns = [col[1] for col in t3.columns.values]
    t3.columns.values[0] = 'date'    
    
    t4 = df.query('themes_filtered_Wellbeing == "Wellbeing"').resample('M', on='date').agg({'tone':[('Wellbeing', 'mean')], 'source_name': [('no.articles_Wellbeing', 'count')]}).reset_index()
    t4.columns = [col[1] for col in t4.columns.values]
    t4.columns.values[0] = 'date'    
    
    t5 = df.query('themes_filtered_Vaccination == "Vaccination"').resample('M', on='date').agg({'tone':[('Vaccination', 'mean')], 'source_name': [('no.articles_Vaccination', 'count')]}).reset_index()
    t5.columns = [col[1] for col in t5.columns.values]
    t5.columns.values[0] = 'date'   
    
    t6 = df.query('themes_filtered_Coronavirus == "Coronavirus"').resample('M', on='date').agg({'tone':[('Coronavirus', 'mean')], 'source_name': [('no.articles_Coronavirus', 'count')]}).reset_index()
    t6.columns = [col[1] for col in t6.columns.values]
    t6.columns.values[0] = 'date'  
    
    dfs = [t1,t2,t3,t4,t5,t6]
    

    df = reduce(lambda left,right: pd.merge(left,right,on='date', how='outer'), dfs)
    print("Merged_themes")
    df['country'] = c
    return df

def upload_to_s3(data,c,y):
    s3 = s3fs.core.S3FileSystem(anon=False, profile='kandavar_processing')
    with s3.open(f's3://statsnz-covid-kandavar/G_from_2015/monthly/m_df_{c}_{y}.csv','w') as f:               # df here stands for domain filter
        data.to_csv(f, index=False)
    print(f"uploaded-{c}{y}")  

if __name__ == '__main__':
    
    for c in country:
        start = time.time()
        for y in Year:

            df = read_from_s3(f'gdelt_{c}_{y}.csv', c)
            
            df = themes_filter(df)
            df = match_themes(df)
            df = filter_merge(df,c)
            upload_to_s3(df,c,y)     
            del df
        
        end = time.time()
        print(f"data processed for the country: {c} which took {round(end-start,2)} seconds")

print('finished')
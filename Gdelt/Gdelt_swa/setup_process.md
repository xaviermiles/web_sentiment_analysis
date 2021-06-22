
Each script and their functions:


1) swa_gdelt_process.py :

    Script to extract the respective country csv's from GDELT using the master.txt file.
    
    which then stores every 15min interval csv's in the s3 folder 's3://statsnz-covid-kandavar/G_from_2015/au/' for australia and other countries respectively for every year from 2015.
    
2) swa_merge_from_s3.py :
    
    Script to merge all the CSV's for each year each country.
    
    the merged CSV will be stored under 's3://statsnz-covid-kandavar/G_from_2015/merged/' for every country year wise.
    
3) swa_monthly_tone.py :
    
    Script to filter only the country domain news and match the themes of interest and calculate monthly average tone on respective themes.
    
    which then uploaded to s3 folder path 's3://statsnz-covid-kandavar/G_from_2015/monthly/'

4) swa_monthly_final.py :

    Script to merge all the country monthly sentiment tone for each theme.
    
    which is then stored in 's3://statsnz-covid-kandavar/G_from_2015/swa_gdelt_final_monthly.csv'
    
5) swa_overall.py :
    
    script to get sentiment from the country and about the country for average, rolling average, standard deviation and rolling standard deviation
    
    the csv's are stored in 's3://statsnz-covid-kandavar/G_from_2015/swa_insights/' for each country year wise. 
    
    which is then merged and saved in 's3://statsnz-covid-kandavar/G_from_2015/swa_insights/merged/'
    
    
    
    
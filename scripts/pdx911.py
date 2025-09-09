import numpy as np
import pandas as pd
import os

import requests
import re

import pyarrow as pa
import pyarrow.parquet as pq

import time











### 6/26 OLD VERSION BEFORE ADDING RETRIES
# d=pd.read_parquet('./data/portland_crime_data.parquet', engine='pyarrow')
# url='https://www.portlandonline.com/scripts/911incidents.cfm'
# dtmp=pd.read_xml(url)
# dtmp=dtmp.loc[(dtmp.id.notnull())&(dtmp.title.notnull())].reset_index(drop=True)

# dtmp=dtmp[['summary','updated','point']]
# dtmp.columns=['TEXT','DATE','COORDS']

# dtmp[['CRIME','ADDRESS']] = dtmp['TEXT'].str.split('at',n=1, expand=True)
# dtmp[['ADDRESS','CRIME_ID']]=dtmp['ADDRESS'].str.split(' \[',n=1,expand=True)
# dtmp['ADDRESS']=dtmp['ADDRESS'].str.replace(', PORT',', PORTLAND').str.replace(', GRSM',', GRESHAM')

# dtmp['DATE'] = pd.to_datetime(dtmp['DATE'])
# dtmp['HOUR'] = dtmp['DATE'].dt.floor('h')
# dtmp['DAY'] = dtmp['DATE'].dt.floor('d')
# dtmp['DATE_CRIME'] = dtmp['DATE'].dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + dtmp['CRIME']

# dtmp[['LATITUDE','LONGITUDE']] = dtmp['COORDS'].str.split(' ',n=1, expand=True)

# d = pd.concat([d,dtmp])

# d=d.groupby(['DATE','TEXT','COORDS']).size().to_frame('cnt').reset_index().sort_values('DATE',ascending=True).reset_index(drop=True)
# del d['cnt']

# d[['CRIME','ADDRESS']] = d['TEXT'].str.split('at',n=1, expand=True)
# d[['ADDRESS','CRIME_ID']]=d['ADDRESS'].str.split(' \[',n=1,expand=True)
# d['ADDRESS']=d['ADDRESS'].str.replace(', PORT',', PORTLAND').str.replace(', GRSM',', GRESHAM')
# d['DATE']=d['DATE'].apply(lambda x: pd.to_datetime(x).tz_convert('US/Pacific'))
# d['DATE_CRIME'] = pd.to_datetime(d['DATE'], utc=False).dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + d['CRIME']
# d[['LATITUDE','LONGITUDE']] = d['COORDS'].str.split(' ',n=1, expand=True)
# # d['DATE'] = pd.to_datetime(d['DATE'])
# d['HOUR'] = d['DATE'].dt.floor('h',ambiguous=True)
# d['DAY'] = d['DATE'].dt.floor('d')
# d['DATE_CRIME'] = d['DATE'].dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + d['CRIME']


# table = pa.Table.from_pandas(d)
# pq.write_table(table, './data/portland_crime_data.parquet',compression='BROTLI',use_deprecated_int96_timestamps=True)








### 6/26 new version w/ retries
d=pd.read_parquet('./data/portland_crime_data.parquet', engine='pyarrow')


bad_data = d.loc[d['TEXT'].isna()]
print(f'bad data -- currently there is {bad_data.index.size} records of bad data')
# bad_data=bad_data[['summary', 'updated', 'point']]
# bad_data.columns=['TEXT','DATE','COORDS']

# bad_data[['CRIME','ADDRESS']] = bad_data['TEXT'].str.split('at',n=1, expand=True)
# bad_data[['ADDRESS','CRIME_ID']]=bad_data['ADDRESS'].str.split(' \[',n=1,expand=True)
# bad_data['ADDRESS']=bad_data['ADDRESS'].str.replace(', PORT',', PORTLAND').str.replace(', GRSM',', GRESHAM')

# bad_data['DATE'] = pd.to_datetime(bad_data['DATE'])
# bad_data['HOUR'] = bad_data['DATE'].dt.floor('h')
# bad_data['DAY'] = bad_data['DATE'].dt.floor('d')
# bad_data['DATE_CRIME'] = bad_data['DATE'].dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + bad_data['CRIME']

# bad_data[['LATITUDE','LONGITUDE']] = bad_data['COORDS'].str.split(' ',n=1, expand=True)
# d = pd.concat([d, bad_data])
# d=d.groupby(['DATE','TEXT','COORDS']).size().to_frame('cnt').reset_index().sort_values('DATE',ascending=True).reset_index(drop=True)
# del d['cnt']




url = 'https://www.portlandonline.com/scripts/911incidents.cfm'
max_retries = 3
retry_delay_seconds = 5
dtmp = None

for attempt in range(max_retries):
    try:
        print(f"Attempt {attempt + 1} of {max_retries} to fetch data...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        dtmp = pd.read_xml(response.text)
        print("Data fetched successfully.")
        new_data_size = dtmp.index.size
        print(f"new data size {new_data_size}")
        break  # Exit the loop if successful
    except requests.exceptions.RequestException as e:
        print(f"Attempt {attempt + 1} failed: {e}")
        if attempt < max_retries - 1:
            print(f"Retrying in {retry_delay_seconds} seconds...")
            time.sleep(retry_delay_seconds)
        else:
            print("Max retries reached. Giving up.")
            # You can choose to log the error and exit or continue without updating the data
            exit() # Exit the script
            # or pass
            
# If dtmp is None, it means all retries failed.
if dtmp is not None:
    # Rest of your data processing code for dtmp
    dtmp = dtmp.loc[(dtmp.id.notnull()) & (dtmp.title.notnull())].reset_index(drop=True)
    dtmp = dtmp[['summary', 'updated', 'point']]
    dtmp.columns=['TEXT','DATE','COORDS']
    
    dtmp[['CRIME','ADDRESS']] = dtmp['TEXT'].str.split('at',n=1, expand=True)
    dtmp[['ADDRESS','CRIME_ID']]=dtmp['ADDRESS'].str.split(' \[',n=1,expand=True)
    dtmp['ADDRESS']=dtmp['ADDRESS'].str.replace(', PORT',', PORTLAND').str.replace(', GRSM',', GRESHAM')
    
    dtmp['DATE'] = pd.to_datetime(dtmp['DATE'])
    dtmp['HOUR'] = dtmp['DATE'].dt.floor('h')
    dtmp['DAY'] = dtmp['DATE'].dt.floor('d')
    dtmp['DATE_CRIME'] = dtmp['DATE'].dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + dtmp['CRIME']
    
    dtmp[['LATITUDE','LONGITUDE']] = dtmp['COORDS'].str.split(' ',n=1, expand=True)
    
    print("Data updates successfully.")
    new_data_size = dtmp.index.size
    print(f"new data size {new_data_size}")

    
    # Then continue with the rest of your code
    d = pd.concat([d, dtmp])
    d=d.groupby(['DATE','TEXT','COORDS']).size().to_frame('cnt').reset_index().sort_values('DATE',ascending=True).reset_index(drop=True)
    del d['cnt']
    
    d[['CRIME','ADDRESS']] = d['TEXT'].str.split('at',n=1, expand=True)
    d[['ADDRESS','CRIME_ID']]=d['ADDRESS'].str.split(' \[',n=1,expand=True)
    d['ADDRESS']=d['ADDRESS'].str.replace(', PORT',', PORTLAND').str.replace(', GRSM',', GRESHAM')
    d['DATE']=d['DATE'].apply(lambda x: pd.to_datetime(x).tz_convert('US/Pacific'))
    d['DATE_CRIME'] = pd.to_datetime(d['DATE'], utc=False).dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + d['CRIME']
    d[['LATITUDE','LONGITUDE']] = d['COORDS'].str.split(' ',n=1, expand=True)
    # d['DATE'] = pd.to_datetime(d['DATE'])
    d['HOUR'] = d['DATE'].dt.floor('h',ambiguous=True)
    d['DAY'] = d['DATE'].dt.floor('d')
    d['DATE_CRIME'] = d['DATE'].dt.strftime('%-m/%-d %-I:%M%p').astype('str') + ' - ' + d['CRIME']
    # ... and so on

    
    # Finally, write the file
    table = pa.Table.from_pandas(d)
    pq.write_table(table, './data/portland_crime_data.parquet', compression='BROTLI', use_deprecated_int96_timestamps=True)
else:
    print("No new data was fetched. The existing file will not be updated.")

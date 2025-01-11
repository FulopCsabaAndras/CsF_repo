import requests
import pandas as pd 
import numpy as np 
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attributes = ['Name','MC_USD_Billion']
table_attributes_final = ['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
csv_path = 'Largest_banks_data.csv'
db_name = 'banks.db'
table_name = 'Largest_banks'
log_file = 'code_log'

def extract(url, table_attributes):
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(table_attributes)
    table = soup.find_all('tbody')
    rows = table[2].find_all('tr')
    
    
    for row in rows:
        data = row.find_all('td')
        bank_name = data[1].a.contents[0]['title']
        mark_cap = data[2].contents[0]
        data_dict = {
            "Name": bank_name,
            "MC_USD_Billion": mark_cap
        }
        df2=pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df2], ignore_index=True)
        
    return df
     
   
def transform(df,csv_path):
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df


def load_to_csv(df, output_path):
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attributes)

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df,'exchange_rate.csv')

log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')


log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE MC_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')
sql_connection.close()

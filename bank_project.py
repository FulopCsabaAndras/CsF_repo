from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import sqlite3
import glob
from datetime import datetime 


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributum = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

def extract(url, table_attributum):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attributum)
    tables = data.find_all('tbody')
    rows = tables[1].find_all('tr')
    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[0]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict =  {"Name":bank_name, "MC_USD_Billion":float(market_cap)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df


def transform(df,csv_path):
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
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
df = extract(url, table_attribs)

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

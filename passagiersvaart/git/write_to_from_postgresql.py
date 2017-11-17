# script with 2 ways to push to postgresql database
import pandas as pd
import psycopg2 
import logging
import configparser
from sqlalchemy import create_engine

# config parser 
config = configparser.RawConfigParser()
config.read('auth.conf')

# read from auth.conf file
OBJECTSTORE_PASSWORD = config['postgresql_dataservices']['PS_PASSWORD']

PS_ENGINE = {'dialect': config.get('postgresql_dataservices','PS_DIALECT'),
             'user': config.get('postgresql_dataservices','PS_USER'),
             'password': OBJECTSTORE_PASSWORD,
             'host': config.get('postgresql_dataservices','PS_HOST'),
             'port': config.get('postgresql_dataservices','PS_PORT'),
             'database': config.get('postgresql_dataservices', 'PS_DATABASE'),
             'schema' : config.get('postgresql_dataservices', 'PS_SCHEMA')
               }

# db_url to be put in to_sql
db_url = ('{dialect}://{user}:{password}@{host}:{port}/{database}'.
          format(dialect=PS_ENGINE['dialect'], 
                 user=PS_ENGINE['user'], 
                 password=PS_ENGINE['password'], 
                 host=PS_ENGINE['host'],
                 port=PS_ENGINE['port'],
                 database=PS_ENGINE['database']))

engine = create_engine(db_url)
## df.to_sql(OUTPUT_FILE, con=engine, schema='passagiersvaart', if_exists='replace')  

## write to postgresql with psycopg2 module
psycopg2_connect = (psycopg2.connect("dbname={} user={} host={} port ={} password={} sslmode = {}".
          format(PS_ENGINE['database'], 
                 PS_ENGINE['user'],
                 PS_ENGINE['host'],
                 PS_ENGINE['port'], 
                 PS_ENGINE['password'], 
                 'disable')))


def query_data_postgresql(sql):
    """
    input: sql statement
    output: Pandas dataframe
    """
    print("loading data ...")
    column_names = []
    data_rows = []

    with psycopg2_connect as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            column_names = [desc[0] for desc in cursor.description]
            for row in cursor:
                data_rows.append(row)
            df = pd.DataFrame(data_rows, columns=column_names)

    return df
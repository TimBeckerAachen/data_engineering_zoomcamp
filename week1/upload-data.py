#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    os.system(f'wget {url} -O output.csv')

    csv_name = 'output.csv'
    df = pd.read_csv(csv_name, nrows=10)

    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # create schema
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # fill data
    df_iter = pd.read_csv('output.csv', iterator=True, chunksize=100000)
    for df_temp in df_iter:
        t_start = time()
        df_temp['tpep_dropoff_datetime'] = pd.to_datetime(df_temp['tpep_dropoff_datetime'])
        df_temp['tpep_pickup_datetime'] = pd.to_datetime(df_temp['tpep_pickup_datetime'])

        df_temp.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('done with one chumk in %f.3' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest csv data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url from where to get the data')

    args = parser.parse_args()
    main(args)

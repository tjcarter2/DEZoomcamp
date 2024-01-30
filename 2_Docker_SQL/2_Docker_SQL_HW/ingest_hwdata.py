import os
import argparse
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import argparse
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    trips_table = params.trips_table
    zones_table = params.zones_table
    trips_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'
    zones_url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
    trips_csv = 'trips.csv'
    zones_csv = 'zones.csv'


    # download the CSV files
    os.system(f"wget {trips_url} -O {trips_csv}")
    os.system(f"wget {zones_url} -O {zones_csv}")

    # create db connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # insert trips data to database
    print('inserting trips to database')
    trips_df = pd.read_csv(trips_url)

    #trips_iter = pd.read_csv(trips_url, iterator=True, chunksize=100000)
    #trips_df = next(trips_iter)
    trips_df.lpep_pickup_datetime = pd.to_datetime(trips_df.lpep_pickup_datetime)
    trips_df.lpep_dropoff_datetime = pd.to_datetime(trips_df.lpep_dropoff_datetime)

    trips_df.head(n=0).to_sql(name=trips_table, con=engine, if_exists='replace')
    trips_df.to_sql(name=trips_table, con=engine, if_exists='append')
    print('finished inserting trips to database')
    
    # # iterate through chunks
    # for chunk in trips_iter:
    #     t_start = time()
    #     trips_df = chunk
    #     trips_df.lpep_pickup_datetime = pd.to_datetime(trips_df.lpep_pickup_datetime)
    #     trips_df.lpep_dropoff_datetime = pd.to_datetime(trips_df.lpep_dropoff_datetime)
    #     trips_df.to_sql(name=trips_table, con=engine, if_exists='append')
    #     t_end = time()
    #     print('inserted another chunk, took %.3f second' % (t_end - t_start))
    # print('finished inserting trips to database')

    # insert zones data to database
    print('inserting zones to database')
    zones_df =pd.read_csv(zones_url)
    zones_df.to_sql(name=zones_table, con=engine, if_exists='append')
    print('finished inserting zones to database')




if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest csv data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--trips_table', help='name of the table for trips')
    parser.add_argument('--zones_table', help='name of the table for zones')
    #parser.add_argument('--trips_url', help='url of the trips csv file')
    #parser.add_argument('--zones_url', help='url of the zones csv file')

    args = parser.parse_args()

    main(args)





import datetime
import sys
from functools import reduce
from os import environ
from time import sleep
from sqlalchemy import create_engine, exc
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
import pandas as pd
import ast

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:

    msqldb_uri = 'mysql+pymysql://root:password@localhost:3306/sqlalchemy'
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here


def query_to_df(query):
    df = None
    try:
        with psql_engine.connect() as connection:
            result = connection.execute(text(query))
            df = pd.DataFrame(result.fetchall())
            connection.close()
            return df

    except exc.SQLAlchemyError:
        sys.exit("Encountered SQLAlchemyError.!")

def df_to_db(df):
    while True:
        try:
            mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
            break
        except OperationalError:
            sleep(0.1)
    print('Connection to MySQL successful.')

    create_table_query = "CREATE TABLE dummy_table (hour TIMESTAMP ,temperature BIGINT, distance DOUBLE, size BIGINT)"
    mysql_engine.execute(text(create_table_query))
    print("Table created")
    df.to_sql(name='dummy_table', con=mysql_engine, if_exists='replace', index=False)
    print("Data added to DB")
    mysql_engine.close()
    psql_engine.dispose()


def group_by_hour(df):
    #function to bucket hourly slots
    df['hour'] = df.apply(lambda r: datetime.datetime(r['time'].year, r['time'].month, r['time'].day, r['time'].hour, 0), axis=1)
    df = df.drop('time', axis=1)
    grouped_df = df.groupby('hour',as_index=False,)
    return grouped_df

def get_max_temp(df):
    grouped_df = group_by_hour(df)
    max_temp_df = grouped_df.max('temperature')
    return max_temp_df

def get_device_distance(df):
    my_list = []
    for row in df['location']:
        row_dict = ast.literal_eval(row)
        for k, v in row_dict.items():
            row_dict[k] = float(v) #iterate through each object, convert to dictionary and values to floats
        total_distance = abs(row_dict['latitude']-row_dict['longitude'])
        my_list.append(total_distance) #get absoloute differences between long/lat
    my_series = pd.Series(my_list)

    df_with_distance_column = df
    df_with_distance_column.drop(['location','temperature'], axis=1, inplace=True)

    df_with_distance_column.reset_index(drop=True)
    df_with_distance_column['distance'] = my_series

    distance_df_by_hour = group_by_hour(df_with_distance_column)
    sum_distance_df = distance_df_by_hour.sum('distance')

    print(sum_distance_df)

    return sum_distance_df

def get_data_points(df):
    grouped_df = group_by_hour(df)
    grouped_df_dp = grouped_df.size()
    return grouped_df_dp

def main():
    #pull data from DB
    my_query = "SELECT * FROM devices"
    device_df = query_to_df(my_query)

    psql_engine.dispose()

    #apply cleaning
    pd.to_datetime(device_df['time'], unit='s')
    device_df['time'] = (pd.to_datetime(device_df['time'], unit='s'))

    # apply transformations

    #a)
    max_temp_df = get_max_temp(device_df)

    #b)
    distance_df = get_device_distance(device_df)

    #c)
    datapoints_df = get_data_points(device_df)

    dataframes = [max_temp_df, distance_df, datapoints_df]

    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['hour'],
                                                    how='outer'), dataframes)

    #load
    df_to_db(df_merged)


if __name__ == "__main__":
    main()
import pandas as pd
import numpy as np
import psycopg2
import yaml
import datetime as dt
from sqlalchemy import create_engine
import json

df_swiss_snow = pd.read_csv('swiss_snow.csv')
df_swiss_sunshine = pd.read_csv('swiss_sunshine.csv')
df_swiss_precipitation = pd.read_csv('swiss_precipitation.csv')
df_swiss_temp = pd.read_csv('swiss_temp.csv')
df_temp = pd.read_csv('temp.csv')
df_co2 = pd.read_csv('co2.csv')

countries = pd.read_json('data_collection/countries.json').T

credentials = yaml.load(open('config.yml'), Loader=yaml.FullLoader)

def get_df_name(df):
    '''
    Taken from https://stackoverflow.com/a/50620134/12183550
    '''
    name =[x for x in globals() if globals()[x] is df][0]
    return name

def get_db_table_length(table, credentials):
    '''
    Get table length form postgres sql db.

    Takes:
    - table: table name
    - credentials: loaded yaml file

    Returns:
    - len_table: length of table
    '''
    # set up connection parameters
    db_name = credentials['sql']['db']
    db_user = credentials['sql']['user']
    db_user_pw = credentials['sql']['pw']
    db_adress = credentials['sql']['host']
    db_port = credentials['sql']['port']

    # set up connection
    conn = psycopg2.connect(database = db_name, user = db_user, password = db_user_pw, host = db_adress, port = db_port)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM {table_name}".format(table_name = table))
    len_table = cursor.fetchone()
    conn.commit()
    cursor.close()
    return len_table[0]

def insert_df_into_db_table(df, table, engine):
    '''
    Takes given Dataframe and sends it to the database (using the given engine)

    Takes:
    - df: pandas Dataframe in correct format
    - table: string of the table name
    - engine: SQL-Alchemy engine
    '''
    df_name = get_df_name(df)

    #df.to_sql(table, engine, if_exists = 'append', index = False)
    print("Inserted df '{df_name}' into table '{table_name}'.".format(df_name = df_name, table_name = table))

def transform_swiss_snow_for_db(df):
    '''
    Transforms the downloaded snow data into the third normal form, ready to be stored in the database.

    Takes:
    - Dataframe of swiss snow data

    Returns:
    - 3 Dataframes (in this order!):
        - res_locations
        - res_sensors
        - res_sensor_readings
    '''

    df['Year'] = df['Year'].apply(str)
    df['Year'] = pd.to_datetime(df['Year']).dt.date
    res = df.copy()

    # transform for 'locations' table
    loc_len = get_db_table_length('locations', credentials)
    sensor_len = get_db_table_length('sensors', credentials)

    loc_name_rename = df['Region'].unique().tolist()
    loc_id_rename = list(range(loc_len + 1, len(loc_name_rename) + loc_len + 1))
    sensor_id = list(range(loc_len + 1, len(loc_name_rename) + loc_len + 1))

    res_location = res.drop(['Year', 'Neuschnee'], axis = 1).groupby(['Region'], as_index = False).last()
    #res_location = pd.merge(res_location, countries, left_on='Country', right_on='db_name')
    res_location = res_location.join(countries, on = 'Country')
    res_location.rename(columns={'alpha3':'alpha_3'}, inplace=True)
    #res_location['location_id'] = res_location.index + 1
    res_location = res_location.drop(['url', 'db_name'], axis = 1)
    res_location.columns = map(str.lower, res_location.columns)

    # transform for 'sensors' table
    res_sensors = res # MAYBE FIX POINTER LATER? .copy()
    res_sensors['location_id'] = res_sensors['Region'].replace(loc_name_rename, loc_id_rename)
    res_sensors['sensor_type'] = 'new_snow'
    res_sensors = res_sensors.drop(['Year','Country', 'Region', 'Neuschnee'], axis = 1).groupby(['location_id'], as_index = False).last()
    res_sensors['sensor_id'] = sensor_id
    res_sensors.columns = map(str.lower, res_sensors.columns)

    #transform for 'sensor_readings' table
    res_sensor_readings = res.copy()
    res_sensor_readings['int_reading'] = res['Neuschnee'].copy()
    res_sensor_readings = pd.merge(res_sensor_readings, res_sensors, left_on='location_id', right_on='sensor_id')
    res_sensor_readings = res_sensor_readings.drop([
        'Country', 'Region', 'Neuschnee', 'sensor_type_x', 'sensor_type_y', 'location_id_x', 'location_id_y'
    ], axis = 1)
    res_sensor_readings.rename(columns={'Year':'timestamp'}, inplace=True)
    res_sensor_readings.columns = map(str.lower, res_sensor_readings.columns)

    return res_location, res_sensors, res_sensor_readings

def transform_swiss_sunshine_for_db(df):
    '''
    Transforms the downloaded sunshine data into the third normal form, ready to be stored in the database.

    Takes:
    - Dataframe of swiss sunshine data

    Returns:
    - 2 Dataframes (in this order!):
        - res_sensors
        - res_sensor_readings
    '''

    df['Year'] = df['Year'].apply(str)
    df['Year'] = pd.to_datetime(df['Year']).dt.date
    res = df.copy()

    # transform for 'locations' table
    loc_len = get_db_table_length('locations', credentials)
    sensor_len = get_db_table_length('sensors', credentials)

    loc_name_rename = df['Region'].unique().tolist()
    loc_id_rename = list(range(1, loc_len + 1))
    sensor_id = list(range(loc_len + 1, len(loc_name_rename) + loc_len + 1))

    # transform for 'sensors' table
    res_sensors = res
    res_sensors['location_id'] = res_sensors['Region'].replace(loc_name_rename, loc_id_rename)
    res_sensors['sensor_type'] = 'sunshine'
    res_sensors = res_sensors.drop(['Year','Country', 'Region', 'Sonnenscheindauer'], axis = 1).groupby(['location_id'], as_index = False).last()
    res_sensors['sensor_id'] = sensor_id
    res_sensors.columns = map(str.lower, res_sensors.columns)

    #transform for 'sensor_readings' table
    res_sensor_readings = res
    res_sensor_readings['float_reading'] = res['Sonnenscheindauer'].copy()
    res_sensor_readings['location_id'] = res_sensor_readings['Region'].replace(loc_name_rename, loc_id_rename)
    res_sensor_readings = pd.merge(res_sensor_readings, res_sensors, left_on='location_id', right_on='location_id')
    res_sensor_readings = res_sensor_readings.drop([
        'Country', 'Region', 'Sonnenscheindauer', 'location_id', 'sensor_type_x', 'sensor_type_y'
    ], axis = 1)
    res_sensor_readings.rename(columns={'Year':'timestamp'}, inplace=True)
    res_sensor_readings.columns = map(str.lower, res_sensor_readings.columns)
    return res_sensors, res_sensor_readings

def transform_swiss_precipitation_for_db(df):
    '''
    Transforms the downloaded precipitation data into the third normal form, ready to be stored in the database.

    Takes:
    - Dataframe of swiss precipitation data

    Returns:
    - 2 Dataframes (in this order!):
        - res_sensors
        - res_sensor_readings
    '''

    df['Year'] = df['Year'].apply(str)
    df['Year'] = pd.to_datetime(df['Year']).dt.date
    res = df.copy()

    # transform for 'locations' table
    loc_len = get_db_table_length('locations', credentials)
    sensor_len = get_db_table_length('sensors', credentials)

    loc_name_rename = df['Region'].unique().tolist()
    loc_id_rename = list(range(1, loc_len + 1))
    sensor_id = list(range(sensor_len + 1, len(loc_name_rename) + sensor_len + 1))

    # transform for 'sensors' table
    res_sensors = res.copy()
    res_sensors['location_id'] = res_sensors['Region'].replace(loc_name_rename, loc_id_rename)
    res_sensors['sensor_type'] = 'precipitation'
    res_sensors = res_sensors.drop(['Year','Country', 'Region', 'Jahresniederschlag'], axis = 1).groupby(['location_id'], as_index = False).last()
    res_sensors['sensor_id'] = sensor_id
    res_sensors.columns = map(str.lower, res_sensors.columns)

    #transform for 'sensor_readings' table
    res_sensor_readings = res
    res_sensor_readings['float_reading'] = res['Jahresniederschlag'].copy()
    res_sensor_readings['location_id'] = res_sensor_readings['Region'].replace(loc_name_rename, loc_id_rename)
    res_sensor_readings = pd.merge(res_sensor_readings, res_sensors, left_on='location_id', right_on='location_id')
    res_sensor_readings = res_sensor_readings.drop([
        'Country', 'Region', 'Jahresniederschlag', 'location_id', 'sensor_type'
    ], axis = 1)
    res_sensor_readings.rename(columns={'Year':'timestamp'}, inplace=True)
    res_sensor_readings.columns = map(str.lower, res_sensor_readings.columns)

    return res_sensors, res_sensor_readings

def transform_swiss_temp_for_db(df):
    '''
    Transforms the downloaded precipitation data into the third normal form, ready to be stored in the database.

    Takes:
    - Dataframe of swiss precipitation data

    Returns:
    - 2 Dataframes (in this order!):
        - res_sensors
        - res_sensor_readings
    '''

    df['Year'] = df['Year'].apply(str)
    df['Year'] = pd.to_datetime(df['Year']).dt.date
    res = df.copy()

    # transform for 'locations' table
    loc_len = get_db_table_length('locations', credentials)
    sensor_len = get_db_table_length('sensors', credentials)

    loc_name_rename = df['Region'].unique().tolist()
    loc_id_rename = list(range(1, loc_len + 1))
    sensor_id = list(range(sensor_len + 1, len(loc_name_rename) + sensor_len + 1))

    # transform for 'sensors' table
    res_sensors = res.copy()
    res_sensors['location_id'] = res_sensors['Region'].replace(loc_name_rename, loc_id_rename)
    res_sensors['sensor_type'] = 'temperature'
    res_sensors = res_sensors.drop(['Year','Country', 'Region', 'Jahrestemperatur'], axis = 1).groupby(['location_id'], as_index = False).last()
    res_sensors['sensor_id'] = sensor_id
    res_sensors.columns = map(str.lower, res_sensors.columns)

    #transform for 'sensor_readings' table
    res_sensor_readings = res
    res_sensor_readings['float_reading'] = res['Jahrestemperatur'].copy()
    res_sensor_readings['location_id'] = res_sensor_readings['Region'].replace(loc_name_rename, loc_id_rename)
    res_sensor_readings = pd.merge(res_sensor_readings, res_sensors, left_on='location_id', right_on='location_id')
    res_sensor_readings = res_sensor_readings.drop([
        'Country', 'Region', 'Jahrestemperatur', 'location_id', 'sensor_type'
    ], axis = 1)
    res_sensor_readings.rename(columns={'Year':'timestamp'}, inplace=True)
    res_sensor_readings.columns = map(str.lower, res_sensor_readings.columns)

    return res_sensors, res_sensor_readings

def transform_temperature_for_db(df):
    '''
    Transforms the downloaded temperature data into the third normal form, ready to be stored in the database.

    Takes:
    - Dataframe of global temperature data

    Returns:
    - 3 Dataframes (in this order!):
        - res_locations
        - res_sensors
        - res_sensor_readings
    '''

    res = df.copy()

    # transform for 'locations' table
    loc_len = get_db_table_length('locations', credentials)
    sensor_len = get_db_table_length('sensors', credentials)
    reading_len = get_db_table_length('sensor_readings', credentials)

    temp_name_rename = df['country'].unique().tolist()
    temp_id_rename = list(range(loc_len + 1, len(temp_name_rename) + loc_len + 1))
    sensor_id = list(range(sensor_len + 1, len(temp_id_rename) + sensor_len + 1))

    res_location = res.groupby(['country'], as_index = False).last()
    res_location = res_location.join(countries, on = 'country')
    res_location.rename(columns={'date':'timestamp', 'alpha3':'alpha_3'}, inplace=True)
    #res_location['location_id'] = res_location.index + 1
    res_location = res_location.drop(['url', 'db_name', 'timestamp', 'monthly_anomaly'], axis = 1)
    res_location.columns = map(str.lower, res_location.columns)

    # transform for 'sensors' table
    res_sensors = res # MAYBE FIX POINTER LATER? .copy()
    res_sensors['location_id'] = res_sensors['country'].replace(temp_name_rename, temp_id_rename)
    res_sensors['sensor_type'] = 'temperature'
    res_sensors = res_sensors.drop(['date','country', 'monthly_anomaly'], axis = 1).groupby(['location_id'], as_index = False).last()
    res_sensors['sensor_id'] = sensor_id
    res_sensors.columns = map(str.lower, res_sensors.columns)

    #transform for 'sensor_readings' table
    res_sensor_readings = res.copy()
    res_sensor_readings['float_reading'] = res['monthly_anomaly'].copy().round(3)
    res_sensor_readings = pd.merge(res_sensor_readings, res_sensors, left_on='location_id', right_on='sensor_id')
    res_sensor_readings = res_sensor_readings.drop([
        'country', 'sensor_type_x', 'sensor_type_y', 'location_id_x', 'location_id_y', 'monthly_anomaly'
    ], axis = 1)
    res_sensor_readings.rename(columns={'date':'timestamp'}, inplace=True)
    res_sensor_readings.columns = map(str.lower, res_sensor_readings.columns)
    res_sensor_readings['int_reading'] = np.nan
    res_sensor_readings['bool_reading'] = np.nan
    res_sensor_readings['sensor_reading_id'] = res_sensor_readings.index + reading_len + 1
    res_sensor_readings = res_sensor_readings[['sensor_reading_id', 'timestamp', 'int_reading', 'float_reading', 'bool_reading','sensor_id']]
    return res_location, res_sensors, res_sensor_readings

def transform_co2_for_db(df):
    '''
    Transforms the downloaded co2 data into the third normal form, ready to be stored in the database.

    Takes:
    - Dataframe of global co2 data

    Returns:
    - 2 Dataframes (in this order!):
        - res_sensors
        - res_sensor_readings
    '''

    res = df.copy()

    co2_name_rename = df['country'].unique().tolist()
    loc_len = get_db_table_length('locations', credentials)
    co2_id_rename = list(range(loc_len + 1, len(co2_name_rename) + loc_len + 1))
    co2_id_rename = [x - len(co2_name_rename) for x in co2_id_rename]

    # transform for 'locations' table
    sensor_len = get_db_table_length('sensors', credentials)
    sensor_id = list(range(sensor_len + 1, len(co2_id_rename) + sensor_len + 1))
    reading_len = get_db_table_length('sensor_readings', credentials)

    loc_sql = pd.read_sql('locations', engine)[['location_id', 'country']]
    loc_sql = loc_sql.groupby(['country'], as_index = False).max()

    # locations exist, all countries are in the 'temperature step' created

    # transform for 'sensors' table
    res_sensors = res
    res_sensors['location_id'] = res_sensors['country'].replace(loc_sql['country'].tolist(), loc_sql['location_id'].tolist())
    res_sensors['sensor_type'] = 'co2'
    res_sensors = res_sensors.drop(['year','country', 'co2'], axis = 1).groupby(['location_id'], as_index = False).last()
    res_sensors['sensor_id'] = sensor_id
    res_sensors.columns = map(str.lower, res_sensors.columns)

    #transform for 'sensor_readings' table
    res_sensor_readings = res.copy()
    res_sensor_readings['float_reading'] = res['co2'].copy().round(5)
    res_sensor_readings = pd.merge(res_sensor_readings, res_sensors, left_on='location_id', right_on='location_id')
    res_sensor_readings = res_sensor_readings.drop([
        'country', 'sensor_type_x', 'sensor_type_y', 'location_id', 'co2'
    ], axis = 1)
    res_sensor_readings.rename(columns={'year':'timestamp'}, inplace=True)
    res_sensor_readings.columns = map(str.lower, res_sensor_readings.columns)
    res_sensor_readings['int_reading'] = np.nan
    res_sensor_readings['bool_reading'] = np.nan
    res_sensor_readings['sensor_reading_id'] = res_sensor_readings.index + reading_len + 1
    res_sensor_readings = res_sensor_readings[['sensor_reading_id', 'timestamp', 'int_reading', 'float_reading', 'bool_reading','sensor_id']]

    return res_sensors, res_sensor_readings

if name == '__main__':

    db_name = 'climate_change_db'
    db_user = 'db_user'
    db_user_pw = 'db_password'
    db_adress = '86.119.42.47'
    db_port = '5432'

    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{db}'.format(user = db_user, password = db_user_pw,
                                                                                        host = db_adress, port = db_port, db = db_name), echo=False)

    # Create the dataframes and then immediately send them to the database
    # The generation of the next location_ids depends on it.

    ## Swiss data
    ### Snow
    db_swiss_snow_loc, db_swiss_snow_sensors, db_swiss_snow_sensor_readings = transform_swiss_snow_for_db(df_swiss_snow)

    db_swiss_snow_loc.to_sql('locations', engine, if_exists = 'append', index = False)
    db_swiss_snow_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_swiss_snow_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)

    ### Sunshine

    db_swiss_sunshine_sensors, db_swiss_sunshine_sensor_readings = transform_swiss_sunshine_for_db(df_swiss_sunshine)

    db_swiss_sunshine_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_swiss_sunshine_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)

    ### Precipitation

    db_swiss_precipitation_sensors, db_swiss_precipitation_sensor_readings = transform_swiss_precipitation_for_db(df_swiss_precipitation)

    db_swiss_precipitation_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_swiss_precipitation_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)

    ### Temperature

    db_swiss_temp_sensors, db_swiss_temp_sensor_readings = transform_swiss_temp_for_db(df_swiss_temp)

    db_swiss_temp_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_swiss_temp_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)


    ## Global data
    ### Temperature

    db_temp_location, db_temp_sensors, db_temp_sensor_readings = transform_temperature_for_db(df_temp)

    db_temp_location.to_sql('locations', engine, if_exists = 'append', index = False)
    db_temp_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)

    # Takes way too long, aborted after over 30 min (probably takes around 1.5 hours to run)
    db_temp_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)
    # read in as csv
    #db_temp_sensor_readings.to_csv('db_temp_sensor_readings.csv',index = False)

    ### Carbon dioxide

    db_co2_sensors, db_co2_sensor_readings = transform_co2_for_db(df_co2)

    db_co2_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_co2_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)

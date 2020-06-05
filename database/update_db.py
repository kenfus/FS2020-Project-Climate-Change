import db_initial_setup as dbs
import pandas as pd

def update_swiss_snow_data(path_to_csv):
    '''
    Updates swiss snow data.

    Takes:
    - path to csv file of new data
    '''

    df = pd.read_csv(path_to_csv)
    db_swiss_snow_loc, db_swiss_snow_sensors, db_swiss_snow_sensor_readings = dbs.transform_swiss_snow_for_db(df)

    # send to database
    db_swiss_snow_loc.to_sql('locations', engine, if_exists = 'append', index = False)
    db_swiss_snow_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_swiss_snow_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)

def update_swiss_sunshine_data(path_to_csv):
    '''
    Updates swiss snow data.

    Takes:
    - path to csv file of new data
    '''

    df = pd.read_csv(path_to_csv)
    db_swiss_sunshine_sensors, db_swiss_sunshine_sensor_readings = dbs.transform_swiss_sunshine_for_db(df)

    db_swiss_sunshine_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_swiss_sunshine_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)


def update_swiss_precipitation_data(path_to_csv):
    '''
    Updates swiss snow data.

    Takes:
    - path to csv file of new data
    '''

    df = pd.read_csv(path_to_csv)
    db_swiss_precipitation_sensors, db_swiss_precipitation_sensor_readings = dbs.transform_swiss_precipitation_for_db(df)

    db_swiss_precipitation_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_swiss_precipitation_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)


def update_swiss_temperature_data(path_to_csv):
    '''
    Updates swiss snow data.

    Takes:
    - path to csv file of new data
    '''

    df = pd.read_csv(path_to_csv)
    db_swiss_temp_sensors, db_swiss_temp_sensor_readings = dbs.transform_swiss_temp_for_db(df)

    db_swiss_temp_sensors.to_sql('sensors', engine, if_exists = 'append', index = False)
    db_swiss_temp_sensor_readings.to_sql('sensor_readings', engine, if_exists = 'append', index = False)

import pandas as pd
from sqlalchemy import create_engine
import yaml

# Query swiss data
def get_swiss_data_db(engine):
    '''
    Collects the swiss precipitation data from the database.
    data names: precipitation, new_snow, sunshine, temperature.

    Takes:
    - engine: SQL-Alchemy engine

    Returns:
    - df: dictionary of pandas dataframes with data names as keys.
    '''

    # define sql queries
    q_precip = '''SELECT r.timestamp as date, l.country, l.region, r.float_reading AS precipitation
                     FROM locations l
                     JOIN (SELECT sensor_id, location_id, sensor_type FROM sensors) AS s
                           ON l.location_id = s.location_id
                     JOIN (SELECT timestamp, float_reading, sensor_id FROM sensor_readings) AS r
                           ON s.sensor_id = r.sensor_id
                     WHERE l.region IS NOT NULL and s.sensor_type = 'precipitation'
                     ORDER BY l.region'''

    q_snow = '''SELECT r.timestamp as date, l.country, l.region, r.int_reading AS new_snow
                FROM locations l
                JOIN (SELECT sensor_id, location_id, sensor_type FROM sensors) AS s
                      ON l.location_id = s.location_id
                JOIN (SELECT timestamp, int_reading, sensor_id FROM sensor_readings) AS r
                      ON s.sensor_id = r.sensor_id
                WHERE l.region IS NOT NULL and s.sensor_type = 'new_snow'
                ORDER BY l.region'''

    q_sun = '''SELECT r.timestamp as date, l.country, l.region, r.float_reading AS sunshine
                  FROM locations l
                  JOIN (SELECT sensor_id, location_id, sensor_type FROM sensors) AS s
                        ON l.location_id = s.location_id
                  JOIN (SELECT timestamp, float_reading, sensor_id FROM sensor_readings) AS r
                        ON s.sensor_id = r.sensor_id
                  WHERE l.region IS NOT NULL and s.sensor_type = 'sunshine'
                  ORDER BY l.region'''

    q_temp = '''SELECT r.timestamp as date, l.country, l.region, r.float_reading AS temperature
                   FROM locations l
                   JOIN (SELECT sensor_id, location_id, sensor_type FROM sensors) AS s
                         ON l.location_id = s.location_id
                   JOIN (SELECT timestamp, float_reading, sensor_id FROM sensor_readings) AS r
                         ON s.sensor_id = r.sensor_id
                   WHERE l.region IS NOT NULL and s.sensor_type = 'temperature'
                   ORDER BY l.region'''

    # get data from database
    df_precip = pd.read_sql(q_precip, engine)
    df_snow = pd.read_sql(q_snow, engine)
    df_sun = pd.read_sql(q_sun, engine)
    df_temp = pd.read_sql(q_temp, engine)


    df_list = [df_precip, df_snow, df_sun, df_temp]
    df_names = ['precipitation', 'new_snow', 'sunshine', 'temperature']

    return dict(zip(df_names, df_list))

# Query global data
def get_global_data_db(engine):
    '''
    Collects the swiss precipitation data from the database.
    data names: precipitation, new_snow, sunshine, temperature.

    Takes:
    - engine: SQL-Alchemy engine

    Returns:
    - df: dictionary of pandas dataframes with data names as keys.
    '''

    q_temp = '''SELECT r.timestamp as date, l.country, r.float_reading AS temperature
                FROM locations l
                JOIN (SELECT sensor_id, location_id, sensor_type FROM sensors) AS s
                      ON l.location_id = s.location_id
                JOIN (SELECT timestamp, float_reading, sensor_id FROM sensor_readings) AS r
                      ON s.sensor_id = r.sensor_id
                WHERE l.region IS NULL and s.sensor_type = 'temperature'
                ORDER BY l.country'''

    q_co2 = '''SELECT r.timestamp as date, l.country, r.float_reading AS co2
               FROM locations l
               JOIN (SELECT sensor_id, location_id, sensor_type FROM sensors) AS s
                     ON l.location_id = s.location_id
               JOIN (SELECT timestamp, float_reading, sensor_id FROM sensor_readings) AS r
                     ON s.sensor_id = r.sensor_id
               WHERE l.region IS NULL and s.sensor_type = 'co2'
               ORDER BY l.country'''

    df_temp = pd.read_sql(q_temp, engine)
    df_co2 = pd.read_sql(q_co2, engine)

    df_list = [df_temp, df_co2]
    df_names = ['temperature', 'co2']

    return dict(zip(df_names, df_list))

if __name__ == '__main__':
    credentials = yaml.load(open('../config.yml'), Loader=yaml.FullLoader)

    # set up connection parameters
    db_name = credentials['sql']['db']
    db_user = credentials['sql']['user']
    db_user_pw = credentials['sql']['pw']
    db_adress = credentials['sql']['host']
    db_port = credentials['sql']['port']

    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{db}'.format(user = db_user, password = db_user_pw,
                                                                                    host = db_adress, port = db_port, db = db_name), echo=False)

    swiss_data = get_swiss_data_db(engine)
    global_data = get_global_data_db(engine)

    # save data to disk
    swiss_data['precipitation'].to_csv('swiss_precipitation.csv', index = False)
    swiss_data['new_snow'].to_csv('swiss_new_snow.csv', index = False)
    swiss_data['sunshine'].to_csv('swiss_sunshine.csv', index = False)
    swiss_data['temperature'].to_csv('swiss_temperature.csv', index = False)

    global_data['temperature'].to_csv('global_temperature.csv', index = False)
    global_data['co2'].to_csv('global_co2.csv', index = False)

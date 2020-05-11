import psycopg2
import yaml

def drop_tables(connection):
    '''
    Drops the 'location', 'sensors' and 'sensor_readings' tables.

    Takes:
    connection: psycopg2.connect object
    '''
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS sensor_readings')
    cursor.execute('DROP TABLE IF EXISTS sensors')
    cursor.execute('DROP TABLE IF EXISTS location')
    connection.commit()
    cursor.close()
    return 'Tables dropped'


def create_table_locations(connection):
    '''
    Creates the 'location' table:

    | col_name  | location_id | latitude         | longitude        | region      | country     | alpha_3    |
    |-----------|-------------|------------------|------------------|-------------|-------------|------------|
    | data_type | smallserial | double precision | double precision | varchar(60) | varchar(60) | varchar(3) |

    Takes:
    - connection: psycopg2.connect object

    Returns:
    - psycopg2 cursor.statusmessage
    '''
    query_create_locations = (
        'CREATE TABLE "locations" ('
            '"location_id" smallserial,'
            '"latitude" double precision,'
            '"longitude" double precision,'
            '"region" varchar(60),'
            '"country" varchar(60),'
            '"alpha_3" varchar(3),'
            'PRIMARY KEY ("location_id")'
        ');'
    )

    cursor = connection.cursor()
    cursor.execute(query_create_locations)
    cursor_statusmessage = cursor.statusmessage
    connection.commit()
    cursor.close()

    return cursor_statusmessage


def create_table_sensors(connection):
    '''
    Creates the 'sensors' table:

    | col_name  | sensor_id | sensor_type  | location_id |
    |-----------|-----------|--------------|-------------|
    | data_type | serial    | varchar(100) | integer     |

    Takes:
    - connection: psycopg2.connect object

    Returns:
    - psycopg2 cursor.statusmessage
    '''
    query_create_sensors = (
        'CREATE TABLE "sensors" ('
            '"sensor_id" serial,'
            '"sensor_type" varchar(100),'
            'PRIMARY KEY ("sensor_id"),'
            '"location_id" integer REFERENCES locations(location_id)'
        ');'
    )

    cursor = connection.cursor()
    cursor.execute(query_create_sensors)
    cursor_statusmessage = cursor.statusmessage
    connection.commit()
    cursor.close()

    return cursor_statusmessage


def create_table_sensor_readings(connection):
    '''
    Creates the 'sensor_readings' table:

    | col_name  | sensor_reading_id | sensor_id | timestamp | int_reading | float_reading    | bool_reading |
    |-----------|-------------------|-----------|-----------|-------------|------------------|--------------|
    | data_type | bigserial         | integer   | date      | integer     | double precision | boolean      |

    Takes:
    - connection: psycopg2.connect object

    Returns:
    - psycopg2 cursor.statusmessage
    '''
    query_create_sensor_readings = (
        'CREATE TABLE "sensor_readings" ('
            '"sensor_reading_id" bigserial,'
            '"timestamp" date,'
            '"int_reading" integer,'
            '"float_reading" double precision,'
            '"bool_reading" boolean,'
            'PRIMARY KEY ("sensor_reading_id"),'
            '"sensor_id" integer REFERENCES sensors(sensor_id),'
            'CONSTRAINT only_one_reading CHECK'
            '('
                '( CASE WHEN int_reading IS NULL THEN 0 ELSE 1 END'
                 '+ CASE WHEN float_reading IS NULL THEN 0 ELSE 1 END'
                 '+ CASE WHEN bool_reading IS NULL THEN 0 ELSE 1 END'
                ') <= 1'
            ')'
        ');'
    )

    cursor = connection.cursor()
    cursor.execute(query_create_sensor_readings)
    cursor_statusmessage = cursor.statusmessage
    connection.commit()
    cursor.close()

    return cursor_statusmessage

def init_db():
    credentials = yaml.load(open('../config.yml'), Loader=yaml.FullLoader)

    # set up connection parameters
    db_name = credentials['sql']['db']
    db_user = credentials['sql']['user']
    db_user_pw = credentials['sql']['pw']
    db_adress = credentials['sql']['host']
    db_port = credentials['sql']['port']

    # set up connection
    conn = psycopg2.connect(database = db_name, user = db_user, password = db_user_pw, host = db_adress, port = db_port)

    create_table_locations(conn)
    print("Table 'locations' is set up")
    create_table_sensors(conn)
    print("Table 'sensors' is set up")
    create_table_sensor_readings(conn)
    print("Table 'sensor_readings' is set up")

    #Closing the connection
    conn.close()
    print('Table setup complete.')

if __name__ == '__main__':
    init_db()

# -*- coding: utf-8 -*-

"""class DBA(object):

    #static variable dba (DBA-object) --> (_ before varname is to declare it as private)
    _dba = None
    _queries = {'key' : 'query'} #queries could be stored alternatively in a key:value file

    def __init__(self):
        #setup connection (con) here
        self._con = con

    @staticmethod
    def get_DBA():
        #implement singleton pattern
        return dba

    def query(self, key):
        #perform query here
        return result"""

import psycopg2
from psycopg2 import pool


class DBA(object):
    minimum_connections = 1
    maximum_connections = 3
    postgreSQL_pool = None

    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None

    def initiate_connectionpool(self):
        """
        Creates a connection pool if not already exists and adds a connection object to the class object
        This connection pool only works for single-threaded processes. Allows the minimum amount of connections
        and maximal amount of connections given.

        Used package: https://pypi.org/project/psycopg2/

        :param None
        :returns Creates a connection pool and connects
        """
        if DBA.postgreSQL_pool == None:
            try:
                DBA.postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(DBA.minimum_connections,
                                                                         DBA.maximum_connections,
                                                                         user=self.user,
                                                                         password=self.password,
                                                                         host=self.host,
                                                                         port=self.port,
                                                                         database=self.database)
                if (DBA.postgreSQL_pool):
                    print("Connection pool created successfully")

            except(Exception, psycopg2.DatabaseError) as error:
                print("Error while connecting to PostgreSQL", error)
        else:
            print('Connection Pool already exists.')

    def query_data(self, statement):
        """
        Sends a query to the database and returns the resulting records.

        Used package: https://pypi.org/project/psycopg2/

        :param SQL-query
        :returns records
        """
        try:
            if DBA.postgreSQL_pool:
                # Use getconn() to Get Connection from connection pool
                self.conn = DBA.postgreSQL_pool.getconn()
            else:
                raise Exception('Create the connection pool first with function initiate_connectionpool.')

            print("Connection to connection pool successfull. Make query ...")
            self.cursor = self.conn.cursor()

            # Perform statement on SQL-Server
            self.cursor.execute(statement)
            records = self.cursor.fetchall()

            self.cursor.close()

        except (Exception, psycopg2.ProgrammingError) as error:
            raise Exception('Something went wrong.')

        finally:
            # Returns the open connection to the connection pool
            DBA.postgreSQL_pool.putconn(self.conn)
            self.conn = None
            print('Connection returned to pool.')

        return records

    def close_connection_pool(self):
        """
        Turns off all Connections to the server from the pool object.
        """
        # use closeall method to close all the active connections
        if (DBA.postgreSQL_pool):
            DBA.postgreSQL_pool.closeall
            DBA.postgreSQL_pool = None
            print("All connections closed of Connection Pool")
        else:
            print('All connections already closed.')

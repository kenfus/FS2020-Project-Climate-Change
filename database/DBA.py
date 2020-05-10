# -*- coding: utf-8 -*-

import sqlalchemy


class DBA(object):
    """
    For the use of this class create a class object:
    connection_object = DBA(database, user, password, host, port)
    """

    pool = False
    pool_size = 1

    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = None

    def _connect_to_db(self):
        """
        Creates an SQLalchemy engine object with a connection pool with given pool size.
        The engine operates as connector to the database which sends queries and receives data from the database.
        """
        if DBA.pool == False:
            self.engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(self.user,
                                                                                        self.password,
                                                                                        self.host, self.port,
                                                                                        self.database,
                                                                                        pool_size=DBA.pool_size,
                                                                                        max_overflow=0))
            DBA.pool = True
        else:
            pass

    def query_data(self, query):
        """
        Gets a connection from the engine and performs given query.
        It returns a pandas dataframe

        :param SQL-Query
        :returns pandas dataframe
        """
        if DBA.pool == False:
            self._connect_to_db()

        with self.engine.connect() as connection:
            result = pd.read_sql_query(query, con=connection)

        return result

    def close_connection(self):
        """
        Closes all unused connections of the connection engine of SQL-Alchemy
        """
        self.engine.dispose()
        self.engine = None
        DBA.pool = False

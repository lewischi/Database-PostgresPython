# Jason Pang, Lewis Chi
# 997658594, 996905905
# ECS 165A Homework #4
# Christopher Nitta
# 12/9/2014


import logging
import psycopg2


class ConnUtility:
    @staticmethod
    def connect():
        try:
            logging.info("Attempting to connect to the database...")
            connstring = "dbname='postgres'"
            logging.debug("Using connection string: %s" % connstring)
            conn = psycopg2.connect(connstring)
            logging.info("Successfully connected to the database.")
            return conn
        except Exception, e:
            logging.critical("I am unable to connect to the database using connection string: %s" % connstring)
            logging.critical("The actual error: %s" % e)

    @staticmethod
    def wipe_and_rebuild_schema(conn):
        logging.info('Dropping and recreating schema...')
        ConnUtility.execute_non_query(conn, """
        drop schema if exists hw4 cascade;
        create schema hw4;
        """)

    @staticmethod
    def execute_non_query(conn, query):
        try:
            open_cursor = conn.cursor()
            open_cursor.execute(query)
            conn.commit()
            open_cursor.close()
        except Exception as e:
            logging.critical("Error executing query.")
            logging.critical("The actual error: %s" % e)

    @staticmethod
    def execute_query_scalar(conn, query):
        try:
            open_cursor = conn.cursor()
            open_cursor.execute(query)
            result = open_cursor.fetchone()
            open_cursor.close()
            return result
        except Exception as e:
            logging.critical("Error executing query.")
            logging.critical("The actual error: %s" % e)
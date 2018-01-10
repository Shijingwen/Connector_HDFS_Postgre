# -*- coding: utf-8 -*-
# !usr/bin/python3
"""
    Connector of Postgresql
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Following Tutorial：
    http://www.psycopg.org/psycopg/docs/usage.html

    Preparation on Postgresql Server:

    (1)Create database and connect user
    sudo -s -u postgres
    psql
    CREATE DATABASE test;
    CREATE USER visitor WITH PASSWORD 'visitor';
    GRANT ALL PRIVILEGES ON DATABASE test to visitor;

    (2)Set ip and restart Postgresql

    vim /home/hbase/pgsql/data/postgresql.conf
    (Find and change this line to allow db server listen to request from any hosts)
        listen_addresses = '*'
    vim /home/hbase/pgsql/data/pg_hba.conf
    (Add this line to allow any hosts to visit server)
        host all all 0.0.0.0/0 md5

    Don't forget to restart PostgreSql.
"""

import psycopg2
import os
import pwd


class PgsqlConnector:

    def __init__(self):
        self.connString = self.conn_info(db='test', user='hbase', passwd='hbase',
                                         host='172.16.101.149', port='5432')

    def conn_info(self, db=None, user=None, passwd=None, host=None, port=None):

        if user is None:
            user = pwd.getpwuid(os.getuid())[0]
        if db is None:
            db = user

        str_info = 'dbname=%s user=%s' % (db, user)
        if passwd is not None:
            str_info += ' password=' + passwd
        if host is not None:
            str_info += ' host=' + host
        if port is not None:
            str_info += ' port=' + port
        return str_info

    def test_suit(self):
        # Connect to an existing database
        conn = psycopg2.connect(self.connString)
        print("Connection Test: Pass")

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Execute a command: this creates a new table
        cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
        print("Create Test: Pass")

        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no more SQL injections!)
        cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (1, "123"))
        print("Insert Test: (1, 123)")
        print("Insert Test: Pass")

        # Query the database and obtain data as Python objects
        cur.execute("SELECT * FROM test;")
        record = cur.fetchone()
        print("Find Test: Pass")
        print("Find Result:", record)
        # Make the changes to the database persistent
        conn.commit()

        # Close communication with the database
        cur.close()
        conn.close()


conn = PgsqlConnector()
conn.test_suit()


# -*- coding: utf-8 -*-
"""Main module defining filesystem and file classes"""

import os
import psycopg2
from Rules import *
from HdfsConnector_v1 import *


class Index:
    """
    Connection to HDFS namenode and Postgresql
    """
    def __init__(self):
        self.work_path = os.getcwd()

    def build_index(self, dict_pg_info, list_insert):
        """
        (1)Check if the index exit,create a new one if not.
        (2)Insert into index.
        :return: True/False Successful/Failed
        """
        flag_exit = True
        if flag_exit is False:
            self.create_new_index(dict_pg_info)
        self.insert_index(dict_pg_info, list_insert)

    def create_new_index(self, dict_pg_info):
        """
        Connect postgresql and create a table for index.
        :param tname: table name
        :return: true - creating is successful ,false - creating is unccessful.
        """
        # ! Setting if fun can use default setting
        ruler = Rules()
        str_conn = ruler.pg_info_rules(dict_pg_info)
        conn = psycopg2.connect(str_conn)

        with conn:
            with conn.cursor() as cur:
                str_create_table = "CREATE TABLE " + dict_pg_info['table'] + " (path varchar PRIMARY KEY);"
                # ! Check if table already exit
                cur.execute(str_create_table)
                cur.close()

        conn.close()

    def insert_index(self, dict_pg_info, list_insert=None):

        ruler = Rules()
        str_conn = ruler.pg_info_rules(dict_pg_info)
        try:
            conn = psycopg2.connect(str_conn)
            with conn:
                with conn.cursor() as cur:
                    for i in list_insert:
                        str_order = "INSERT INTO " + dict_pg_info['table'] + "(path) VALUES (%s)", i
                        print(str_order)
                        cur.execute(str_order)
                conn.commit()
        except:
            print("Fail to connect Index System!")
            return False

        return True

    def search_index(self, dict_pg_info, list_raw_query=None):
        """

        :return:
        """
        list_hdfs_paths = []

        ruler = Rules()
        list_final_query = ruler.query_rules(list_raw_query)
        str_conn = ruler.pg_info_rules(dict_pg_info)
        conn = psycopg2.connect(str_conn)
        with conn:
            with conn.cursor() as cur:
                for i in list_final_query:
                    str_order = "SELECT * FROM " + dict_pg_info['table']
                    print(str_order)
                    cur.execute(str_order)
                    list_hdfs_paths.append(cur.fetchone())
        return list_hdfs_paths

    def delete_index(self, dict_pg_info):
        """
        Delete index table.
        :param table: table name
        :return: true - creating is successful ,false - creating is unccessful.
        """
        ruler = Rules()
        str_conn = ruler.pg_info_rules(dict_pg_info)
        try:
            conn = psycopg2.connect(str_conn)
        except:
            print("Unable to connect to the database.")
        if conn is not None:
            cur = conn.cursor()
            str_order = "Delete TABLE " + dict_pg_info['table'] + ";"
            # ! Check if table already exit
            cur.execute(str_order)
            cur.close()
        else:
            print("Fail to open database, please complete correct information.")
        conn.close()

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
        cur.execute("INSERT INTO test2 (num, data) VALUES (%s, %s)", (1, "123"))
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

# index = Index()
# index.delete_index()

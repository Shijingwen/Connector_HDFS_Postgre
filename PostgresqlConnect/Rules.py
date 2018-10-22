# -*- coding: utf-8 -*-
"""Main module manage filesystem and index"""
import os
import pwd
import psycopg2


class Rules:

    def path_rules(self, list_raw_file):
        list_hdfs_file = list_raw_file
        for i in range(0, list_raw_file.__len__()):
            # ! Check if legal
            list_hdfs_file[i] = '/DataType1/' + list_raw_file[i]
        return list_hdfs_file

    def query_rules(self, list_raw_query):
        list_hdfs_file = list_raw_query
        for i in range(0, list_raw_query.__len__()):
            # ! Check if legal
            list_hdfs_file[i] = '/DataType1/' + list_raw_query[i]
        return list_hdfs_file

    def pg_info_rules(self, dict_pg_info):

        user = dict_pg_info.get('user')
        db = dict_pg_info.get('db')
        if user is None:
            user = pwd.getpwuid(os.getuid())[0]
        if db is None:
            db = user

        str_info = 'dbname=%s user=%s' % (db, user)
        if dict_pg_info.get('passwd') is not None:
            str_info += ' password=' + dict_pg_info.get('passwd')
        if dict_pg_info.get('host') is not None:
            str_info += ' host=' + dict_pg_info.get('host')
        if dict_pg_info.get('port') is not None:
            str_info += ' port=' + dict_pg_info.get('port')
        return str_info

# dict_pg_info = {'db': 'test', 'user': 'hbase', 'passwd': 'hbase',
#                 'host': '172.16.101.149', 'port': '5432', 'table': 'test2'}
# ruler = Rules()
# str_info = ruler.pg_info_rules(dict_pg_info)
# print(str_info)
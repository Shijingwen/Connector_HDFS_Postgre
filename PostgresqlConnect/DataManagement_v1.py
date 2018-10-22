# -*- coding: utf-8 -*-
"""Main module manage filesystem and index"""

import os
from hdfs3 import HDFileSystem
from HdfsConnector_v1 import *
from Index import *
from Rules import *


def get_files(self, list_fname):
        """
        :return: True/False Successful/Failed
        """

        list_files = []
        for i in range(0, list_fname.__len__()):
            path = os.getcwd() + list_fname
            list_files.append(path)
            # list_files.append(open(path))
        return list_files


class DataManager:

    def __init__(self, dict_pg_info=None, dict_hdfs_info=None):
        if dict_pg_info is None:
            self.dict_pg_info = {'db': 'test', 'user': 'hbase',
                                 'passwd': 'hbase', 'host': '172.16.101.149',
                                 'port': '5432', 'table': 'test2'}
        else:
            self.dict_pg_info = dict_pg_info

        if dict_hdfs_info is None:
            self.dict_hdfs_info = {'host': '172.16.0.149', 'post': 9000}
        else:
            self.dict_hdfs_info = dict_hdfs_info

    # def write_data(self, list_fname, list_files):
    #     """
    #     Write files into hdfs and insert hdfs path items into postgre
    #     """
    #
    #     list_fname = ['test1.txt', 'test2.txt']
    #     list_files = self.get_files(list_fname)
    #
    #     ruler = Rules()
    #     list_hdfs_paths = ruler.path_rules(list_fname)
    #
    #     flag = self.write_hdfs_index(list_hdfs_paths, list_files)
    #     if flag is True:
    #         print("Write to HDFS and Index successful!")
    #
    # def write_hdfs_index(self, list_hdfs_paths, list_files):
    #     """
    #     (1)Write files to HDFS first.
    #     (2)If (1) is successful,insert HDFS paths into index
    #     :return:
    #     """
    #     conn_hdfs = HdfsConnector()
    #     flag_hdfs = conn_hdfs.put_hdfs(self.dict_hdfs_info, list_hdfs_paths, list_files)
    #     if flag_hdfs is True:
    #         # Try to insert new items into postgresql
    #         index = Index()
    #         flag_pgsq = index.build_index(self.dict_pg_info, list_hdfs_paths)
    #         if flag_pgsq is True:
    #             print("Insert to index successful!")
    #             return True
    #     return False

    def delete_data(self, list_fname):
        """
        Delete files from hdfs and insert hdfs path items into postgre
        """

        list_fname = ['test1.txt', 'test2.txt']

        ruler = Rules()
        list_hdfs_paths = ruler.path_rules(list_fname)

        flag = self.delete_hdfs_index(list_hdfs_paths)
        if flag is True:
            print("Delete" + str(list_hdfs_paths) + "from HDFS and Index successful!")

    def delete_hdfs_index(self, list_hdfs_paths):
        """
        (1)Delete files from HDFS first.
        (2)If (1) is successful, delete HDFS paths into index
        :return:
        """
        #conn_hdfs = HdfsConnector()
        #flag_hdfs = conn_hdfs.delete_hdfs(self.dict_hdfs_info, list_hdfs_paths)
        flag_hdfs = True
        if flag_hdfs is True:
            # Try to delete items from postgresql
            index = Index()
            flag_pgsq = index.delete_items(self.dict_pg_info, list_hdfs_paths)
            if flag_pgsq is True:
                print("Delete from index successful!")
                return True
        return False

    def read_data(self):
        """
        Read files from hdfs according to index in Postgresql
        """

        list_query = ['test1.txt', 'test2.txt']
        list_files = self.read_hdfs_index(list_query)
        print("Find "+list_files.__len__()+" files")

    def read_hdfs_index(self, list_raw_query):
        """
        (1)Search in Index and find all needed HDFS paths.
        (2)If (1) is successful,insert HDFS paths into index
        :return:
        """
        index = Index()
        list_hdfs_paths = index.search_index(list_raw_query)

        if list_hdfs_paths is not None:
            conn_hdfs = HdfsConnector()
            list_files = conn_hdfs.open_hdfs(list_hdfs_paths)
        return list_files


dm = DataManager()
dm.write_file()
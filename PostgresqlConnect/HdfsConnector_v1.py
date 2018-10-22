# -*- coding: utf-8 -*-
# !usr/bin/python3
"""
    Connector of Hdfs Cluster
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Following Tutorial：
    http://hdfs3.readthedocs.io/en/latest/
"""

from hdfs3 import HDFileSystem


class HdfsConnector:
    """
    Connection to HDFS namenode and Postgresql
    """
    def put_hdfs(self, dict_hdfs_info, list_hdfs_paths, list_files):
        """
        Write files into HDFS according to corresponding paths
        :return: True/False Successful/Failed
        """
        # Try to connect HDFS File System
        try:
            hdfs = HDFileSystem(host=dict_hdfs_info['host'],
                                port=dict_hdfs_info['post'])
        except:
            print("Fail to connect HDFS File System!")
            return False

        # Try to write files to HDFS File System
        try:
            print(hdfs.ls('/'))
            for i in range(0, list_hdfs_paths.__len__()):
                hdfs.put(list_hdfs_paths, list_files)
            print(hdfs.ls('/'))
        except:
            print("Fail to write to HDFS File System!")
            return False
        return True

    def open_hdfs(self, dict_hdfs_info, list_hdfs_paths):
        """
        Write files into HDFS according to corresponding paths
        :return: True/False Successful/Failed
        """
        # Try to connect HDFS File System
        try:
            hdfs = HDFileSystem(host=dict_hdfs_info['host'],
                                port=dict_hdfs_info['post'])
        except:
            print("Fail to connect HDFS File System!")
            return False

        # Try to read files from HDFS File System
        list_files = []
        try:
            for i in range(0, list_hdfs_paths.__len__()):
                list_files.append(hdfs.open(list_hdfs_paths))
        except:
            print("Fail to write to HDFS File System!")
            return None
        return list_files


# conn = HdfsConnector()
# conn.test_suit()

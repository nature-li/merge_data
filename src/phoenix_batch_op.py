#!/usr/bin/env python2.7
# coding: utf-8

import traceback
import os
from mt_log.logger import Logger
import commands
from config.config import Config


class PhoenixBatchOp(object):
    def __init__(self, phoenix_cfg):
        """
        :type phoenix_cfg: Config.PhoenixCfg
        """
        self.phoenix_cfg = phoenix_cfg
        """:type: Config.PhoenixCfg"""

    def upload_to_table(self, local_path, file_name, column_lst, full_table=False):
        """
        :type local_path: str
        :type file_name: str
        :type column_lst: list[str]
        :type full_table: bool
        """
        try:
            # first upload local file to hdfs file, like:
            # /www/hadoop/bin/hadoop dfs -put luxury_9336.csv /tmp/user_tag
            local_full_path = os.path.join(local_path, file_name)
            lst_cmd = [self.phoenix_cfg.hadoop, 'dfs',
                       '-put', '-f', local_full_path,
                       self.phoenix_cfg.hdfs_path]
            shell_cmd = " ".join(lst_cmd)
            Logger.info(shell_cmd)
            status, output = commands.getstatusoutput(shell_cmd)
            if status != 0:
                Logger.error('execute command failed: %s' % ' '.join(lst_cmd))
                Logger.error('output is: ' + output)
                return False
            # load hdfs file to phoenix table, like:
            # /www/hadoop/bin/hadoop jar /www/phoenix/phoenix-4.13.1-HBase-1.2-client.jar
            #   org.apache.phoenix.mapreduce.CsvBulkLoadTool
            #   -i /tmp/user_tag/luxury_9336.csv
            #   -t USER_TAG.INDUSTRY_FULL
            #   -c DEVICE_ID,LUXURY
            #   -z www.x.com 
            hdfs_full_path = os.path.join(self.phoenix_cfg.hdfs_path, file_name)
            table_name = self.phoenix_cfg.full_table if full_table else self.phoenix_cfg.add_table
            lst_cmd = [self.phoenix_cfg.hadoop,
                       'jar', self.phoenix_cfg.phoenix_jar,
                       'org.apache.phoenix.mapreduce.CsvBulkLoadTool',
                       '-i', hdfs_full_path,
                       '-t', table_name,
                       '-c', ','.join(column_lst),
                       '-z', self.phoenix_cfg.phoenix_zk]
            shell_cmd = " ".join(lst_cmd)
            Logger.info(shell_cmd)
            status, output = commands.getstatusoutput(shell_cmd)
            if status != 0:
                Logger.error("execute command failed: %s" % ' '.join(lst_cmd))
                Logger.error('output is: ' + output)
                return False
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

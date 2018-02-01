#!/usr/bin/env python2.7
# coding: utf-8

import os
import json
import traceback
from mt_log.logger import LogEnv
from xml_to_dict import XmlDictConfig
import xml.etree.cElementTree as ElementTree


class Config(object):
    class MigrateCfg(object):
        def __init__(self):
            self.enable = False
            self.migrate_full = False
            self.erase_after_migrate = False
            self.active_days = None

    class SyncCfg(object):
        def __init__(self):
            self.from_dir = None
            self.target_dir = None
            self.keep_days = 3
            self.check_count = False

    class SentinelCfg(object):
        def __init__(self):
            self.hosts = list()
            self.names = list()
            self.password = None
            self.db = 0

        def get_hosts(self, address_list):
            for item in address_list:
                host, port = item.split(":")
                self.hosts.append((host, int(port)))

    class LogCfg(object):
        def __init__(self):
            self.env = LogEnv.develop
            self.dir = "logs"
            self.count = 100
            self.name = "sync_tag"

        def get_env(self, name):
            if name == "develop":
                self.env = LogEnv.develop
                return True
            if name == "abtest":
                self.env = LogEnv.abtest
                return True
            if name == "product":
                self.env = LogEnv.product
                return True
            raise ValueError("invalid environment{%s}, please using <develop>, <abtest> or <product>" % name)

    class PhoenixCfg(object):
        def __init__(self):
            self.url = None
            self.add_table = None
            self.full_table = None
            self.enable = False
            self.batch_count = None
            self.direct_to_full = False
            self.hadoop = ''
            self.hdfs_path = ''
            self.phoenix_jar = ''
            self.phoenix_zk = ''

    class ColNameToTagId(object):
        def __init__(self):
            self.col_name_dict = dict()

        def add_tag(self, col_name, tag_id):
            self.col_name_dict[col_name] = tag_id

        def get_tag(self, col_name):
            return self.col_name_dict.get(col_name, None)

        def contains(self, col_name):
            return col_name in self.col_name_dict

    class TaskIdToColName(object):
        def __init__(self):
            self.task_id_dict = dict()

        def add_col(self, task_id, col_name):
            self.task_id_dict[task_id] = col_name

        def get_col(self, task_id):
            return self.task_id_dict.get(task_id, None)

        def contains(self, task_id):
            return task_id in self.task_id_dict

    def __init__(self):
        self._json_dict = dict()
        self.migrate = Config.MigrateCfg()
        self.sync = Config.SyncCfg()
        self.sentinel = Config.SentinelCfg()
        self.log = Config.LogCfg()
        self.phoenix = Config.PhoenixCfg()
        self.col_name_2_tag_id = Config.ColNameToTagId()
        self.task_id_2_col_name = Config.TaskIdToColName()

    def __str__(self):
        return json.dumps(self._json_dict)

    def init(self):
        try:
            # parse json config file
            cur_dir = os.path.dirname(__file__)
            src_dir = os.path.dirname(cur_dir)
            sync_tag_dir = os.path.dirname(src_dir)
            config_dir = os.path.join(sync_tag_dir, "config")
            xml_file = os.path.join(config_dir, "config.xml")

            tree = ElementTree.parse(xml_file)
            root = tree.getroot()
            self._json_dict = XmlDictConfig(root)

            # migrate
            self.migrate.enable = bool(int(self._json_dict["migrate"]["enable"]))
            self.migrate.migrate_full = bool(int(self._json_dict["migrate"]["migrate_full"]))
            self.migrate.erase_after_migrate = bool(int(self._json_dict["migrate"]["erase_after_migrate"]))
            self.migrate.active_days = int(self._json_dict["migrate"]["active_days"])
            # sync
            self.sync.from_dir = self._json_dict["sync"]["from_dir"]
            self.sync.target_dir = self._json_dict["sync"]["target_dir"]
            self.sync.keep_days = int(self._json_dict["sync"]["keep_days"])
            self.sync.check_count = bool(int(self._json_dict["sync"]["check_count"]))
            # sentinel
            self.sentinel.get_hosts(self._json_dict["sentinel"]["hosts"]["host"])
            self.sentinel.names = self._json_dict["sentinel"]["names"]["name"]
            self.sentinel.password = self._json_dict["sentinel"]["password"]
            self.sentinel.db = int(self._json_dict["sentinel"]["db"])
            # log
            self.log.get_env(self._json_dict["log"]["env"])
            self.log.dir = self._json_dict["log"]["dir"]
            self.log.count = int(self._json_dict["log"]["count"])
            self.log.name = self._json_dict["log"]["name"]
            # phoenix
            self.phoenix.url = self._json_dict["phoenix"]["url"]
            self.phoenix.add_table = self._json_dict["phoenix"]["add_table"]
            self.phoenix.full_table = self._json_dict["phoenix"]["full_table"]
            self.phoenix.enable = bool(int(self._json_dict["phoenix"]["enable"]))
            self.phoenix.batch_count = int(self._json_dict["phoenix"]["batch_count"])
            self.phoenix.direct_to_full = bool(int(self._json_dict['phoenix']['direct_to_full']))
            self.phoenix.hadoop = self._json_dict['phoenix']['hadoop']
            self.phoenix.hdfs_path = self._json_dict['phoenix']['hdfs_path']
            self.phoenix.phoenix_jar = self._json_dict['phoenix']['phoenix_jar']
            self.phoenix.phoenix_zk = self._json_dict['phoenix']['phoenix_zk']
            # tags
            for col_name, tag_id in self._json_dict['column_2_tag_id'].items():
                self.col_name_2_tag_id.add_tag(col_name, tag_id)
            # rows
            for raw_task_id, col_name in self._json_dict['task_id_2_col_name'].items():
                task_id = raw_task_id[1:]
                self.task_id_2_col_name.add_col(task_id, col_name)
            return True
        except:
            print traceback.format_exc()
            return False

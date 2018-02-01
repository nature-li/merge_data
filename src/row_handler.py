#!/usr/bin/env  python2.7
# -*- coding:utf8 -*-

from mt_log.logger import Logger
import traceback
from config.config import Config


class RowHandler(object):
    def __init__(self, col_name_2_tag_id, task_id_2_col_name):
        """
        :type col_name_2_tag_id: Config.ColNameToTagId
        :type task_id_2_col_name: Config.TaskIdToColName
        """
        self._col_name_2_tag_id = col_name_2_tag_id
        """:type: Config.ColNameToTagId"""
        self._task_id_2_col_name = task_id_2_col_name
        """:type: Config.TaskIdToColName"""

    def contain_task_id(self, task_id):
        return self._task_id_2_col_name.contains(task_id)

    def _get_tag_id(self, task_id):
        col_name = self._task_id_2_col_name.get_col(task_id)
        if not task_id:
            Logger.error("get col_name for task_id<{}> failed".format(task_id))
            return None
        task_id = self._col_name_2_tag_id.get_tag(col_name)
        return task_id

    def get_task_id_list(self):
        return self._task_id_2_col_name.task_id_dict.keys()

    def parse_row(self, task_id, line):
        """
        :type task_id: str
        :type line: str
        """
        try:
            col_name = self._task_id_2_col_name.get_col(task_id)
            if not col_name:
                Logger.error("get column name for task_id<{}> failed".format(task_id))
                return None

            column_number = self._get_column_number(line)
            if column_number != 1 and column_number != 2:
                Logger.error('get an invalid line<{}> for task_id<{}>'.format(line, task_id))
                return None

            # 一共有2列数据则说明每行数据包含了 device_id, tag_id
            if column_number == 2:
                a_twice = self._parse_device_id_tag_id(line)
                if not a_twice:
                    return None
                device_id, tag_id = a_twice
            else:
                # 一共有1列数据则说明每行数据包含了 device_id
                device_id = self._parse_device_id(line)
                if not device_id:
                    return None

                tag_id = self._get_tag_id(task_id)
                if not tag_id:
                    Logger.error('get tag id for task_id<{}> failed, line:<{}>'.format(task_id, line))
                    return None

            return device_id, col_name, tag_id
        except:
            Logger.error(traceback.format_exc())
            return False

    @classmethod
    def _get_column_number(cls, line):
        try:
            fields = line.split('\t')
            return len(fields)
        except:
            Logger.error(traceback.format_exc())
            return 0

    @classmethod
    def _parse_device_id_tag_id(cls, line):
        """
        :type line: str
        """
        try:
            fields = line.split("\t")
            if len(fields) != 2:
                Logger.error('line<{}> split result length != 2'.format(line))
                return False

            device_id = fields[0].strip()
            if not cls._check_device_id(device_id):
                Logger.warn('line<{}> include an invalid device_id'.format(line))
                return False

            tag_id = fields[1].strip()
            if not tag_id.isdigit():
                # Logger.warn('line<{}> include an invalid device_id'.format(line))
                return False

            return device_id, tag_id
        except:
            Logger.error(traceback.format_exc())
            return False

    @classmethod
    def _parse_device_id(cls, line):
        """
        :type line: str
        """
        try:
            if not cls._check_device_id(line):
                Logger.warn('line<{}> include an invalid device_id'.format(line))
                return False
            return line
        except:
            Logger.error(traceback.format_exc())
            return False

    @classmethod
    def _check_device_id(cls, device_id):
        """
        :type device_id: str
        """
        try:
            if not device_id:
                return False
            if len(device_id) < 8 or len(device_id) > 60:
                return False
            for item in device_id:
                if item.isalnum():
                    continue
                if item == '-':
                    continue
                return False
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

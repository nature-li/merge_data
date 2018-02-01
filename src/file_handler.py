#!/usr/bin/env  python2.7
# -*- coding:utf8 -*-

import os
import traceback
import re
import shutil
import datetime
import gzip
from mt_log.logger import Logger
from row_handler import RowHandler
from phoenix_batch_op import PhoenixBatchOp


class FileHandler(object):
    def __init__(self, from_dir, to_dir, phoenix_enable=False, direct_to_full=False,
                 keep_days=3, check_count=False):
        # 数据文件和时间戳文件所在目录
        self._from_dir = from_dir
        """:type: str"""
        # 处理完数据后保存最近3天历史数据目录
        self._target_dir = to_dir
        """:type: str"""
        # 是否启用保存数据到 phoenix 功能
        self._phoenix_enable = phoenix_enable
        """:type: bool"""
        # 是否直接导入到全量表
        self._phoenix_direct_to_full = direct_to_full
        """:type: bool"""
        # 数据文件最多保存天数
        self._keep_days = keep_days
        """:type: int"""
        # 是否检测文件数量
        self._check_count = check_count
        """:type: bool"""
        # 日期格式为 20180105
        self._dt = None
        """:type: str"""
        # 数据文件后缀
        self._filename_tail = 'txt.gz'
        """:type: str"""
        # 时间戳文件后缀
        self._validate_tail = 'txt.gz_validate'
        """:type: str"""
        # 位于历史数据目录中的success文件夹
        self._target_success_dir = None
        """:type: str"""
        # 位于历史数据目录中的failed文件夹
        self._target_failed_dir = None
        """:type: str"""
        # 位于历史数据目录中的data文件夹
        self._target_data_dir = None
        """:type: str"""
        # 数据文件名称及fd
        self._target_data_files = dict()
        """:type: dict[str][file]"""
        # 行处理器
        self._row_handler = None
        """:type: RowHandler"""
        # 数据文件
        self._data_file_name_dict = dict()
        """:type: dict[str][str]"""
        # 校验文件
        self._validate_file_name_dict = dict()
        """:type: dict[str][str]"""

    def _init(self, dt, row_handler):
        """ 功能：简单初始化并确保目标目录中的success和failed文件夹存在
        :type dt: str
        :type row_handler: RowHandler
        :rtype: bool
        """
        try:
            self._dt = dt
            self._row_handler = row_handler
            self._target_success_dir = os.path.join(self._target_dir, self._dt, 'success')
            self._target_failed_dir = os.path.join(self._target_dir, self._dt, 'failed')
            self._target_data_dir = os.path.join(self._target_dir, self._dt, 'data')
            Logger.info('target_success_dir:=<{}>'.format(self._target_success_dir))
            Logger.info('target_failed_dir:=<{}>'.format(self._target_failed_dir))

            if not self._make_destination_dirs():
                return False
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    @classmethod
    def _create_dir_if_not_exist(cls, dir_path):
        """ 检查目录 dir_path, 若目录不存在则创建它。若目录存在或成功创建则返回True,否则返回False
        :type dir_path: str
        :return: boolean
        """
        try:
            if os.path.exists(dir_path):
                return True

            os.makedirs(dir_path)
            if os.path.exists(dir_path):
                return True

            Logger.error("directory path{} can not be created".format(dir_path))
            return False
        except:
            Logger.error(traceback.format_exc())
            return False

    def _make_destination_dirs(self):
        """ 确保目标目录、成功目录和失败目录三者都存在
        """
        try:
            if not self._create_dir_if_not_exist(self._target_dir):
                return False
            if not self._create_dir_if_not_exist(self._target_success_dir):
                return False
            if not self._create_dir_if_not_exist(self._target_failed_dir):
                return False
            if not self._create_dir_if_not_exist(self._target_data_dir):
                return False
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def _get_validate_name(self, task_id):
        """ 给出 task_id 拼出时间戳文件名
        :type task_id: str
        """
        return self._validate_file_name_dict.get(task_id, None)

    def _get_file_name(self, task_id):
        """ 给出 task_id 拼出文件名
        :type task_id: str
        """
        return self._data_file_name_dict.get(task_id, None)

    @classmethod
    def _check_valid(cls, file_path, validate_path):
        """ 给出文件路径和时间戳文件路径，检测文件是否有效
        :type file_path: str
        :type validate_path: str
        :return: boolean
        """
        try:
            stat = os.stat(file_path)
            file_modify_time = int(stat.st_mtime)

            with open(validate_path) as f:
                record_modify_time = f.read()

            record_modify_time = int(record_modify_time) / 1000
            if file_modify_time == record_modify_time:
                Logger.info('file<{}>: modify_time<{}> == record_time<{}>'
                            .format(file_path, file_modify_time, record_modify_time))
                return True

            Logger.warn('file<{}>: modify_time<{}> != record_time<{}>'
                        .format(file_path, file_modify_time, record_modify_time))
            return False
        except:
            Logger.error(traceback.format_exc())
            return False

    def _get_data_file(self):
        """ 列出 dir_path 目录下符合规定且有效的文件列表。
        :return: dict[str, str] | None
        """
        try:
            if not os.path.exists(self._from_dir):
                Logger.error('dir_path<{}> does not exist'.format(self._from_dir))
                return None

            task_name = dict()
            task_validate = dict()

            # for example: 9344_20180118170142.txt.gz
            filename_reg = r'^(\d+)_{}(\d+){{6}}.{}$'.format(self._dt, self._filename_tail)
            validate_reg = r'^(\d+)_{}(\d+){{6}}.{}$'.format(self._dt, self._validate_tail)
            day_filename_reg = r'^(\d+)_{}.{}$'.format(self._dt, self._filename_tail)
            day_validate_reg = r'^(\d+)_{}.{}$'.format(self._dt, self._validate_tail)
            list_entries = os.listdir(self._from_dir)
            for entry in list_entries:
                if os.path.isdir(entry):
                    Logger.warn('file<{}> is a directory'.format(entry))
                    continue

                matched = False
                file_m = re.match(filename_reg, entry)
                day_file_m = re.match(day_filename_reg, entry)
                if file_m or day_file_m:
                    m = file_m if file_m else day_file_m
                    matched = True
                    task_id = m.group(1)
                    task_name[task_id] = entry
                    self._data_file_name_dict[task_id] = entry
                    Logger.info('file<{}> match regexp<{}>'.format(entry, filename_reg))

                validate_m = re.match(validate_reg, entry)
                day_validate_m = re.match(day_validate_reg, entry)
                if validate_m or day_validate_m:
                    m = validate_m if validate_m else day_validate_m
                    matched = True
                    task_id = m.group(1)
                    task_validate[task_id] = entry
                    self._validate_file_name_dict[task_id] = entry
                    Logger.info('file<{}> match regexp<{}>'.format(entry, validate_reg))

                if not matched:
                    Logger.warn('file<{}> does not match regexp in ({}, {}, {}, {})'
                                .format(entry, filename_reg, day_filename_reg, validate_reg, day_validate_reg))

            valid_task = dict()
            for task_id, file_name in task_name.items():
                validate_name = self._get_validate_name(task_id)
                if task_id not in task_validate:
                    Logger.warn('validate_name<{}> does not in the dir_path<{}>'.format(validate_name, self._from_dir))
                    continue

                if not self._row_handler.contain_task_id(task_id):
                    Logger.warn('file_name<{}> is not an expected file name'.format(file_name))
                    continue

                file_path = os.path.join(self._from_dir, file_name)
                validate_path = os.path.join(self._from_dir, validate_name)
                if not self._check_valid(file_path, validate_path):
                    continue

                valid_task[task_id] = file_path
            return valid_task
        except:
            Logger.error(traceback.format_exc())
            return None

    def _move_file(self, task_id, target_dir):
        """功能： 将 task_id 对应的 *.gz.txt 和 *.gz.txt_validate 文件移动到 target_dir 目录中
        :type task_id: str
        :type target_dir: str
        :rtype: bool
        """
        try:
            file_name = self._get_file_name(task_id)
            source_full_path = os.path.join(self._from_dir, file_name)
            target_full_path = os.path.join(target_dir, file_name)
            Logger.info("mv <{}> to {}".format(source_full_path, target_full_path))
            shutil.move(source_full_path, target_full_path)

            validate_name = self._get_validate_name(task_id)
            source_full_path = os.path.join(self._from_dir, validate_name)
            target_full_path = os.path.join(target_dir, validate_name)
            Logger.info("mv <{}> to {}".format(source_full_path, target_full_path))
            shutil.move(source_full_path, target_full_path)
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def _delete_old_data(self):
        """
        功能：只保留最近N(keep_days)天的备份文件，其它的全部删除掉
        :rtype: bool
        """
        dt = datetime.datetime.now()
        the_oldest_day = dt - datetime.timedelta(days=self._keep_days)
        lst_dir = os.listdir(self._target_dir)
        for a_dir in lst_dir:
            when = self._str_to_date(a_dir)
            if not when:
                Logger.info("dir_name<{}> will be deleted".format(a_dir))
                full_path = os.path.join(self._target_dir, a_dir)
                self._delete_dir(full_path)
                continue

            if when < the_oldest_day:
                Logger.info("dir_name<{}> will be deleted".format(a_dir))
                full_path = os.path.join(self._target_dir, a_dir)
                self._delete_dir(full_path)
            else:
                Logger.info("dir_name<{}> is left".format(a_dir))

    @classmethod
    def _delete_dir(cls, a_dir):
        """功能：删除 a_dir 指定目录
        :rtype: bool
        """
        try:
            if os.path.isdir(a_dir):
                shutil.rmtree(a_dir)
            else:
                os.remove(a_dir)
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    @classmethod
    def _str_to_date(cls, a_string):
        """功能：20080808这样的字符串转date
        :type a_string: str
        :rtype: datetime.datetime
        """
        try:
            when = datetime.datetime.strptime(a_string, '%Y%m%d')
            to_string = when.strftime('%Y%m%d')
            if a_string == to_string:
                return when
            Logger.warn('<{}> is not a date string'.format(a_string))
            return None
        except Exception as e:
            Logger.warn('<{}> to date error: {}'.format(a_string, e.message))
            return None

    def _move_success(self, task_id):
        """功能：将 task_id 对应的 *.gz.txt和*.gz.txt_validate文件移动到成功目录下
        :rtype: bool
        """
        return self._move_file(task_id, self._target_success_dir)

    def _move_failed(self, task_id):
        """功能：将 task_id 对应的 *.gz.txt和*.gz.txt_validate文件移动到失败目录下
        :rtype: bool
        """
        return self._move_file(task_id, self._target_failed_dir)

    def _save_file(self, task_id, col_name, parameters):
        """
        :type task_id: str
        :type col_name: str
        :type parameters: list[tuple[str, str]]
        """
        try:
            file_name = '{}_{}.csv'.format(col_name, task_id)
            if col_name not in self._target_data_files:
                file_full_path = os.path.join(self._target_data_dir, file_name)
                f = open(file_full_path, 'w')
                self._target_data_files[col_name] = f
            else:
                f = self._target_data_files.get(col_name)

            lines = list()
            for (device_id, col_value) in parameters:
                lines.append(device_id + ',' + col_value + '\n')
            if len(parameters) > 0:
                f.writelines(lines)
                Logger.info('write <{}> lines to file<{}>'.format(len(parameters), file_name))
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def _close_file(self, col_name):
        try:
            if col_name in self._target_data_files:
                f = self._target_data_files.get(col_name)
                f.close()
                del self._target_data_files[col_name]
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def _upload_file(self, task_id, full_path, batch_handler, batch_count):
        """功能：读取数据文件并将每行记录上传到phoenix中
        :type task_id: str
        :type full_path: str
        :type batch_handler: PhoenixBatchOp
        :type batch_count: int
        :rtype: bool
        """
        try:
            Logger.info("uploading {}".format(full_path))
            with gzip.open(full_path) as f:
                col_name = None
                parameters = list()

                for line in f:
                    a_tuple = self._row_handler.parse_row(task_id, line)
                    if not a_tuple:
                        # Logger.warn("file{%s} found an invalid line{%s}" % (full_path, line))
                        continue
                    device_id, col_name, col_value = a_tuple

                    parameters.append((device_id, col_value))
                    # 写入文件并上传到phoenix
                    if len(parameters) >= batch_count:
                        if not self._save_file(task_id, col_name, parameters):
                            return False
                        parameters = list()

                # 写入文件并上传到 phoenix
                if len(parameters) > 0:
                    if not self._save_file(task_id, col_name, parameters):
                        return False

                # 关闭打开的文件
                self._close_file(col_name)

                # 上传文件到phoenix
                if self._phoenix_enable:
                    file_name = '{}_{}.csv'.format(col_name, task_id)
                    column_lst = ['DEVICE_ID', col_name.upper()]
                    if self._phoenix_direct_to_full:
                        result = batch_handler.upload_to_table(self._target_data_dir, file_name,
                                                               column_lst, full_table=True)
                    else:
                        result = batch_handler.upload_to_table(self._target_data_dir, file_name,
                                                               column_lst, full_table=False)
                    if not result:
                        Logger.error("bulk upload file<{}> failed".format(file_name))
                        return False
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def handle(self, dt, row_handler, batch_handler, batch_count):
        """ 功能：上传指定目录下的所有合法数据文件，若上传成功则将文件移到成功目录下，若下传失败则将文件移动到失败目录下
        :type dt: string
        :type row_handler: RowHandler
        :type batch_handler: PhoenixBatchOp
        :type batch_count: int
        :return:
        """
        try:
            # 初始化工作
            if not self._init(dt, row_handler):
                return False
            # 列出所有合法文件
            task_files = self._get_data_file()
            # 检测文件是否够数
            if self._check_count:
                task_id_list = self._row_handler.get_task_id_list()
                for task_id in task_id_list:
                    if task_id not in task_files:
                        Logger.error("task_id<{}> have no invalid data file".format(task_id))
                        return False

            # 一个一个地上传文件
            for task_id, full_file_path in task_files.items():
                if not self._upload_file(task_id, full_file_path, batch_handler, batch_count):
                    Logger.error("upload_file failed: " + full_file_path)
                    return False
                self._move_success(task_id)
            # 清除过期备份文件
            self._delete_old_data()
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

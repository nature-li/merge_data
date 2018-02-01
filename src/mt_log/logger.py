#!/usr/bin/env python2.7
# coding: utf-8

import os
import logging
import datetime
import traceback
import inspect
import functools
from mt_file_handler import MtTimedFileHandler


# log environment
class LogEnv(object):
    product = "product"
    abtest = "abtest"
    develop = "develop"


# log severity
class LogSev(object):
    report = (70, "report")
    fatal = (60, "fatal")
    error = (50, "error")
    warn = (40, "warn")
    info = (30, "info")
    debug = (20, "debug")
    trace = (10, "trace")


class Logger(object):
    __process = None
    __report = None
    __third = None
    __sep = '\x1E'
    __env = LogEnv.develop
    __third_handler = None
    __project_root_len = 0

    class Formatter(logging.Formatter):
        converter = datetime.datetime.fromtimestamp

        def formatTime(self, record, datefmt=None):
            ct = self.converter(record.created)
            if datefmt:
                s = ct.strftime(datefmt)
            else:
                t = ct.strftime("%Y-%m-%d %H:%M:%S")
                s = "%s,%03d" % (t, record.msecs)
            return s

    @classmethod
    def init(cls, env, target, file_name, file_size=100 * 1024 * 1024, max_file_count=-1, project_root=None):
        """ Initialize logger

        :type env: string
        :type target: string
        :type file_name: string
        :type file_size: int
        :type max_file_count: int
        :type project_root: str
        :return: bool
        """
        try:
            # Create log path if not existing
            if not os.path.exists(target):
                os.mkdir(target)

            # Save environment
            cls.__env = env

            # Save root dir
            if project_root:
                cls.__project_root_len = len(project_root)

            # Define log level
            logging.addLevelName(LogSev.trace[0], LogSev.trace[1])
            logging.addLevelName(LogSev.debug[0], LogSev.debug[1])
            logging.addLevelName(LogSev.info[0], LogSev.info[1])
            logging.addLevelName(LogSev.warn[0], LogSev.warn[1])
            logging.addLevelName(LogSev.error[0], LogSev.error[1])
            logging.addLevelName(LogSev.fatal[0], LogSev.fatal[1])
            logging.addLevelName(LogSev.report[0], LogSev.report[1])

            # Set log format
            argv = [
                '[%(asctime)s]',
                '[%(levelname)s]',
                '[%(thread)d]',
                '%(message)s'
            ]
            formatter = Logger.Formatter(fmt=cls.__sep.join(argv), datefmt="%Y-%m-%d %H:%M:%S.%f")

            # Process logger to process.log
            process_file = os.path.join(target, file_name + '.process.log')
            cls.__process_handler = MtTimedFileHandler(process_file, file_size, max_file_count)
            cls.__process_handler.setLevel(LogSev.trace[0])
            cls.__process_handler.setFormatter(formatter)
            process_logger = logging.getLogger("process")
            process_logger.setLevel(LogSev.trace[0])
            process_logger.addHandler(cls.__process_handler)
            cls.__process = process_logger

            # Report logger to report.log
            report_file = os.path.join(target, file_name + '.report.log')
            cls.__report_handler = MtTimedFileHandler(report_file, file_size, max_file_count)
            cls.__report_handler.setLevel(LogSev.trace[0])
            cls.__report_handler.setFormatter(formatter)
            report_logger = logging.getLogger("report")
            report_logger.setLevel(LogSev.trace[0])
            report_logger.addHandler(cls.__report_handler)
            cls.__report = report_logger

            # Set third format
            third_argv = [
                '[%(asctime)s]',
                '[%(levelname)s]',
                '[%(thread)d]',
                '[position]',
                '[env]',
                '[pvid]',
                '[keyword]'
                '[%(message)s]'
            ]
            third_formatter = Logger.Formatter(fmt=cls.__sep.join(third_argv), datefmt="%Y-%m-%d %H:%M:%S.%f")

            # Third logger to process.log
            third_file = os.path.join(target, file_name + '.process.log')
            cls.__third_handler = MtTimedFileHandler(third_file, file_size, max_file_count)
            cls.__third_handler.setLevel(LogSev.trace[0])
            cls.__third_handler.setFormatter(third_formatter)
            third_logger = logging.getLogger("third")
            third_logger.setLevel(LogSev.trace[0])
            third_logger.addHandler(cls.__third_handler)
            cls.__third = third_logger

            return True
        except:
            print traceback.format_exc()
            return False

    @classmethod
    def get_third_handler(cls):
        return cls.__third_handler

    @classmethod
    def set_level(cls, level):
        """
        :type level: tuple[int, string]
        :return:
        """
        cls.__process.setLevel(level[0])

    @classmethod
    def trace(cls, msg, pvid="", keyword="normal", stack_idx=1):
        try:
            caller_frame_record = inspect.stack()[stack_idx]
            frame = caller_frame_record[0]
            info = inspect.getframeinfo(frame)
            file_name = info.filename
            line_number = info.lineno
            func = info.function
            message = cls.json_message(file_name, line_number, func, pvid, keyword, msg)
            cls.__process.log(LogSev.trace[0], message)
        except:
            print traceback.format_exc()

    @classmethod
    def debug(cls, msg, pvid="", keyword="normal", stack_idx=1):
        try:
            caller_frame_record = inspect.stack()[stack_idx]
            frame = caller_frame_record[0]
            info = inspect.getframeinfo(frame)
            file_name = info.filename
            line_number = info.lineno
            func = info.function
            message = cls.json_message(file_name, line_number, func, pvid, keyword, msg)
            cls.__process.log(LogSev.debug[0], message)
        except:
            print traceback.format_exc()

    @classmethod
    def info(cls, msg, pvid="", keyword="normal", stack_idx=1):
        try:
            caller_frame_record = inspect.stack()[stack_idx]
            frame = caller_frame_record[0]
            info = inspect.getframeinfo(frame)
            file_name = info.filename
            line_number = info.lineno
            func = info.function
            message = cls.json_message(file_name, line_number, func, pvid, keyword, msg)
            cls.__process.log(LogSev.info[0], message)
        except:
            print traceback.format_exc()

    @classmethod
    def warn(cls, msg, pvid="", keyword="normal", stack_idx=1):
        try:
            caller_frame_record = inspect.stack()[stack_idx]
            frame = caller_frame_record[0]
            info = inspect.getframeinfo(frame)
            file_name = info.filename
            line_number = info.lineno
            func = info.function
            message = cls.json_message(file_name, line_number, func, pvid, keyword, msg)
            cls.__process.log(LogSev.warn[0], message)
        except:
            print traceback.format_exc()

    @classmethod
    def error(cls, msg, pvid="", keyword="normal", stack_idx=1):
        try:
            caller_frame_record = inspect.stack()[stack_idx]
            frame = caller_frame_record[0]
            info = inspect.getframeinfo(frame)
            file_name = info.filename
            line_number = info.lineno
            func = info.function
            message = cls.json_message(file_name, line_number, func, pvid, keyword, msg)
            cls.__process.log(LogSev.error[0], message)
        except:
            print traceback.format_exc()

    @classmethod
    def fatal(cls, msg, pvid="", keyword="normal", stack_idx=1):
        try:
            caller_frame_record = inspect.stack()[stack_idx]
            frame = caller_frame_record[0]
            info = inspect.getframeinfo(frame)
            file_name = info.filename
            line_number = info.lineno
            func = info.function
            message = cls.json_message(file_name, line_number, func, pvid, keyword, msg)
            cls.__process.log(LogSev.fatal[0], message)
        except:
            print traceback.format_exc()

    @classmethod
    def report(cls, msg, pvid="", keyword="normal", stack_idx=1):
        try:
            caller_frame_record = inspect.stack()[stack_idx]
            frame = caller_frame_record[0]
            info = inspect.getframeinfo(frame)
            file_name = info.filename
            line_number = info.lineno
            func = info.function
            message = cls.json_message(file_name, line_number, func, pvid, keyword, msg)
            cls.__report.log(LogSev.report[0], message)
        except:
            print traceback.format_exc()

    @classmethod
    def json_message(cls, file_name, line_number, func, pvid, keyword, msg):
        try:
            if not isinstance(file_name, (str, unicode)):
                file_name = str(file_name)
            if not isinstance(line_number, (str, unicode)):
                line_number = str(line_number)
            if not isinstance(func, (str, unicode)):
                func = str(func)
            if not isinstance(pvid, (str, unicode)):
                pvid = str(pvid)
            if not isinstance(keyword, (str, unicode)):
                keyword = str(keyword)
            if not isinstance(msg, (str, unicode)):
                msg = str(msg)
            file_name = file_name[cls.__project_root_len:]
            message = "[" + file_name + ":" + line_number + ":" + func + "]" + cls.__sep
            message += "[" + cls.__env + "]" + cls.__sep
            message += "[" + pvid + "]" + cls.__sep
            message += "[" + keyword + "]" + cls.__sep
            message += "[" + msg + "]" + cls.__sep
            return message
        except:
            print traceback.format_exc()


def LogError(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            Logger.error(traceback.format_exc(), stack_idx=2)
            return None
    return wrapper

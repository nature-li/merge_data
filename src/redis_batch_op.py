#!/usr/bin/env python2.7
# coding: utf-8

import traceback
import multiprocessing
from mt_shard.shard_sentinel import ShardSentinel
from mt_log.logger import Logger
import consts
from user_pb2 import UserInfo
from multiprocessing import Queue, Lock
from Queue import Empty, Full
import sys


class Worker(multiprocessing.Process):
    def __init__(self, idx, queue, quit_queue, error_queue, current_day, keep_days, user_tag_keys, migrate_full):
        """
        :type queue: Queue
        :type quit_queue: Queue
        """
        super(Worker, self).__init__()
        self._idx = idx
        """:type: idx"""
        self._queue = queue
        """:type: Queue"""
        self._quit_queue = quit_queue
        """:type: Queue"""
        self._error_queue = error_queue
        """:type: Queue"""
        self._sentinel = None
        """:type: ShardSentinel"""
        self._current_day = current_day
        """:type: int"""
        self._keep_days = keep_days
        """:type: int"""
        self._user_tag_keys = user_tag_keys
        """:type: list[str]"""
        self._migrate_full = migrate_full
        """:type: bool"""

    def init(self, hosts, names, password, db):
        self._sentinel = ShardSentinel(hosts, names, password, db)
        self._sentinel.init()

    def _keep_or_not(self, a_dict):
        # 除需要月活设备外，还需要其它全部数据
        if self._keep_days is None:
            return True

        # 只要月活数据，但此设备没有活动过
        if a_dict[consts.active_day] is None:
            return False

        # 最近一个月没有活跃过,不保留
        active_day = int(a_dict[consts.active_day])
        if self._current_day - active_day > self._keep_days:
            return False
        return True

    def _need_update_redis(self, a_dict):
        # 全量更新需要更新redis
        if self._migrate_full:
            return True
        # 增量更新，只更新有变化的数据
        if a_dict[consts.update_day] is None:
            return False
        # 增量更新，只更有变化的数据
        update_day = int(a_dict[consts.update_day])
        if self._current_day != update_day:
            return False
        return True

    def _empty(self, a_dict):
        """:type: dict[str][str]"""
        if not a_dict:
            return True
        for k in self._user_tag_keys:
            if a_dict[k] is not None:
                return False
        return True

    def _get_user_info(self, a_dict):
        if self._empty(a_dict):
            return None

        user_info = UserInfo()

        if a_dict[consts.age] is not None:
            user_info.age = int(a_dict[consts.age])
        if a_dict[consts.gender] is not None:
            user_info.gender = int(a_dict[consts.gender])
        if a_dict[consts.delicious] is not None:
            user_info.occupation.append(int(a_dict[consts.delicious]))
        if a_dict[consts.dress] is not None:
            user_info.occupation.append(int(a_dict[consts.dress]))
        if a_dict[consts.travel] is not None:
            user_info.occupation.append(int(a_dict[consts.travel]))
        if a_dict[consts.finance] is not None:
            user_info.occupation.append(int(a_dict[consts.finance]))
        if a_dict[consts.decorate] is not None:
            user_info.occupation.append(int(a_dict[consts.decorate]))
        if a_dict[consts.estate] is not None:
            user_info.occupation.append(int(a_dict[consts.estate]))
        if a_dict[consts.luxury] is not None:
            user_info.occupation.append(int(a_dict[consts.luxury]))
        if a_dict[consts.shopping] is not None:
            user_info.occupation.append(int(a_dict[consts.shopping]))
        if a_dict[consts.wedding] is not None:
            user_info.occupation.append(int(a_dict[consts.wedding]))
        if a_dict[consts.infant] is not None:
            user_info.occupation.append(int(a_dict[consts.infant]))
        if a_dict[consts.education] is not None:
            user_info.occupation.append(int(a_dict[consts.education]))
        if a_dict[consts.car] is not None:
            user_info.occupation.append(int(a_dict[consts.car]))
        if a_dict[consts.game] is not None:
            user_info.occupation.append(int(a_dict[consts.game]))
        return user_info

    def run(self):
        try:
            Logger.info("sub process #%s is running..." % self._idx)
            while True:
                a_dict = None
                empty = False
                try:
                    a_dict = self._queue.get(timeout=1)
                except Empty:
                    empty = True
                except:
                    self._set_error(traceback.format_exc())

                a_quit = False
                try:
                    a_quit = self._quit_queue.get(block=False)
                except Empty:
                    a_quit = False
                except:
                    self._set_error(traceback.format_exc())

                if a_quit:
                    Logger.info("sub process #%s is quitting..." % self._idx)
                    break

                if empty:
                    continue

                if not a_dict:
                    continue

                try:
                    key = a_dict[consts.device_id]
                    if not self._keep_or_not(a_dict):
                        # 如果只要活跃用户，则删除非活跃用户
                        self._sentinel.delete(str(key))
                        continue

                    if self._need_update_redis(a_dict):
                        # 如果是增量更新，则只更新有变化数据
                        user_info = self._get_user_info(a_dict)
                        if not user_info:
                            # 删除空数据
                            self._sentinel.delete(str(key))
                            continue

                        # 写入用户标签数据
                        value = user_info.SerializeToString()
                        self._sentinel.set(str(key), value)
                except:
                    self._set_error(traceback.format_exc())
                    break
        except:
            self._set_error(traceback.format_exc())

    def _set_error(self, error_msg):
        try:
            self._error_queue.put(error_msg)
        except Full:
            pass
        except:
            print >> sys.stderr, traceback.format_exc()


class WorkerController(object):
    def __init__(self, current_day, keep_days, user_tag_keys, migrate_full, process_count=None):
        self._current_day = current_day
        """:type: int"""
        self._keep_days = keep_days
        """:type: int"""
        self._user_tag_keys = user_tag_keys
        """:type: list[str]"""
        self._migrate_full = migrate_full
        """:type: boolean"""
        if process_count is not None:
            self._process_count = process_count
            """:type: int"""
        else:
            self._process_count = multiprocessing.cpu_count()
            """:type: int"""
        self._list = list()
        """:type: list[Worker]"""
        self._queue = multiprocessing.Queue(10000)
        self._quit_queue = multiprocessing.Queue(self._process_count)
        self._error_queue = multiprocessing.Queue(self._process_count)

    def start(self, hosts, names, password, db):
        try:
            for i in xrange(self._process_count):
                worker = Worker(i, self._queue, self._quit_queue, self._error_queue, self._current_day,
                                self._keep_days, self._user_tag_keys, self._migrate_full)
                self._list.append(worker)
                worker.init(hosts, names, password, db)
                worker.start()
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def stop(self):
        for i in xrange(self._process_count):
            self._quit_queue.put(True)

        for i in xrange(self._process_count):
            Logger.info("wait subprocess #%s to stop..." % i)
            worker = self._list[i]
            worker.join()

    def put(self, a_dict):
        """
        :rtype: boolean|str
        """
        try:
            try:
                error_msg = self._error_queue.get(block=False)
            except Empty:
                error_msg = None
            if error_msg:
                return error_msg
            self._queue.put(a_dict)
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

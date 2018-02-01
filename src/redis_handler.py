#!/usr/bin/env python2.7
# coding: utf-8

from mt_shard.shard_sentinel import ShardSentinel
from user_pb2 import UserInfo
import consts
from mt_log.logger import Logger
import traceback


class RedisHandler(object):
    def __init__(self, hosts, names, password, db, current_day, active_days=None):
        self.sentinel = ShardSentinel(hosts, names, password, db)
        """:type: ShardSentinel"""
        self.current_day = current_day
        """:type: int"""
        self.keep_days = active_days
        """:type: int"""

    def init(self):
        try:
            self.sentinel.init()
            return True
        except:
            Logger.error(traceback)
            return False

    @classmethod
    def get_user_info(cls, a_dict):
        user_info = UserInfo()
        if a_dict[consts.age]:
            user_info.age = int(a_dict[consts.age])
        if a_dict[consts.gender]:
            user_info.gender = int(a_dict[consts.gender])
        if a_dict[consts.delicious]:
            user_info.occupation.append(int(a_dict[consts.delicious]))
        if a_dict[consts.dress]:
            user_info.occupation.append(int(a_dict[consts.dress]))
        if a_dict[consts.travel]:
            user_info.occupation.append(int(a_dict[consts.travel]))
        if a_dict[consts.finance]:
            user_info.occupation.append(int(a_dict[consts.finance]))
        if a_dict[consts.decorate]:
            user_info.occupation.append(int(a_dict[consts.decorate]))
        if a_dict[consts.estate]:
            user_info.occupation.append(int(a_dict[consts.estate]))
        if a_dict[consts.luxury]:
            user_info.occupation.append(int(a_dict[consts.luxury]))
        if a_dict[consts.shopping]:
            user_info.occupation.append(int(a_dict[consts.shopping]))
        if a_dict[consts.wedding]:
            user_info.occupation.append(int(a_dict[consts.wedding]))
        if a_dict[consts.infant]:
            user_info.occupation.append(int(a_dict[consts.infant]))
        if a_dict[consts.education]:
            user_info.occupation.append(int(a_dict[consts.education]))
        if a_dict[consts.car]:
            user_info.occupation.append(int(a_dict[consts.car]))
        if a_dict[consts.game]:
            user_info.occupation.append(int(a_dict[consts.game]))
        return user_info

    def _keep_or_not(self, a_dict):
        # 除需要月活设备外，还需要其它全部数据
        if self.keep_days is None:
            return True

        # 需要月活数据，但此设备没有活动过
        if a_dict[consts.day_active] is None:
            return False

        # 最后一次活动日期
        day_active = int(a_dict[consts.day_active])

        # 最几一段时间没有活跃,不保留
        if self.current_day - day_active > self.keep_days:
            return False
        return True

    def save(self, a_dict):
        try:
            keep_or_not = self._keep_or_not(a_dict)

            key = a_dict[consts.device_id]
            if keep_or_not:
                # 添加或更新数据
                user_info = self.get_user_info(a_dict)
                value = user_info.SerializeToString()
                self.sentinel.set(str(key), value)
            else:
                # 删除数据
                self.sentinel.delete(str(key))
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

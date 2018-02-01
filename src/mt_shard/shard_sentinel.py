#!/usr/bin/env python
# -*- coding: utf-8 -*-

import shard_commands
from redis.sentinel import Sentinel
from shard_base import ShardBase


class ShardSentinel(ShardBase):
    def __init__(self, address_list, server_names, password, db=0, socket_timeout=30):
        """
        :param address_list: sentinel address for example [('192.168.0.1', 16379), ('192.168.0.2', 16379)]
        :type address_list: list[tuple[str, int]]
        :type password: str
        :type db: int
        :type socket_timeout: int
        """
        super(ShardSentinel, self).__init__()
        self._address_list = address_list
        self._server_names = server_names
        self._password = password
        self._db = db
        self._socket_timeout = socket_timeout
        self._sentinel = None

    def init(self):
        super(ShardSentinel, self)._init_base(self._server_names)
        self._sentinel = Sentinel(sentinels=self._address_list, socket_timeout=self._socket_timeout, db=self._db)

    def _get_shard_client(self, key):
        name = self._get_shard_name(key)
        if key in shard_commands.READ_COMMANDS:
            client = self._sentinel.slave_for(name, password=self._password)
        else:
            client = self._sentinel.master_for(name, password=self._password)
        return client





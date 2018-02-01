#!/usr/bin/env python
# -*- coding: utf-8 -*-

from redis.client import StrictRedis
from shard_base import ShardBase


class ShardRedis(ShardBase):
    def __init__(self, address_list, password, db=0):
        """
        :param address_list: redis address for example [('192.168.0.1', 6379), ('192.168.0.2', 6379)]
        :type address_list: list[tuple[str, int]]
        :type password: str
        :type db: int
        """
        super(ShardRedis, self).__init__()
        self._address_list = address_list
        self._password = password
        self._db = db
        self._client_dict = dict()

    def init(self):
        super(ShardRedis, self)._init_base(self._address_list)
        for address in self._address_list:
            host, port = address
            self._client_dict[address] = StrictRedis(host=host, port=port, db=self._db, password=self._password)

    def _get_shard_client(self, key):
        address = self._get_shard_name(key)
        client = self._client_dict.get(address, None)
        return client





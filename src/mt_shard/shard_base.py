#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mt_hash
import functools
import shard_commands


class ShardBase(object):
    def __init__(self):
        self._seed = 0x1234ABCD
        self._base_string = (str, bytes)
        self._node_dict = dict()

    def _get_shard_name(self, key):
        hash_value = self._hash(key)
        node_values = self._node_dict.keys()
        node_values.sort()
        result_key = node_values[0]
        for node_value in node_values:
            if node_value >= hash_value:
                result_key = node_value
                break
        return self._node_dict.get(result_key)

    def _get_shard_client(self, key):
        """
        :type key: str|bytes
        :return: StrictRedis
        """
        return None

    def __getattr__(self, method):
        if method in shard_commands.SHARD_METHODS:
            return functools.partial(self._wrap, method)
        err_msg = "method '%s' cannot be shard" % method
        raise NotImplementedError(err_msg)

    def _wrap(self, method, *args, **kwargs):
        key = args[0]
        if not isinstance(key, self._base_string):
            err_msg = "method '%s' requires a key param as the first argument" % method
            raise ValueError(err_msg)
        client = self._get_shard_client(key)
        f = getattr(client, method)
        return f(*args, **kwargs)

    def _hash(self, key):
        return mt_hash.hash(key, self._seed)

    def _init_base(self, item_list):
        for i in xrange(len(item_list)):
            for n in xrange(160):
                hash_key = "SHARD-" + str(i) + "-NODE-" + str(n)
                hash_value = self._hash(hash_key)
                self._node_dict[hash_value] = item_list[i]

    def brpop(self, key, timeout=0):
        if not isinstance(key, self._base_string):
            raise NotImplementedError("The key must be single string. Multiple keys cannot be sharded")
        client = self._get_shard_client(key)
        return client.brpop(key, timeout)

    def blpop(self, key, timeout=0):
        if not isinstance(key, self._base_string):
            raise NotImplementedError("The key must be single string. Multiple keys cannot be sharded")
        client = self._get_shard_client(key)
        return client.blpop(key, timeout)

    def haskey(self, key):
        if not isinstance(key, self._base_string):
            raise ValueError("invalid key")
        client = self._get_shard_client(key)
        return key in client

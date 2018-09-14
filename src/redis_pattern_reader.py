#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
read data from redis with key pattern
"""
import redis


class RedisReader(object):
    def __init__(self, host=None, port=None, password=None):
        host = "127.0.0.1" if host is None else host
        port = 6379 if port is None else port
        self.r = redis.Redis(host=host, port=port, password=password)
        self.get = GetValue(r=self.r)

    def keys(self, pattern, count=10000):
        """
        get all of the keys that match a pattern
        :param pattern: the pattern of key
        :param count: how many keys to scan each time
        :return: all of the keys that match the pattern
        """
        original_cur = 0
        cur, first_scan_keys = self.r.scan(original_cur, match=pattern,
                                           count=count)

        res = first_scan_keys
        while cur != 0:
            cur, new_keys = self.r.scan(cur, match=pattern, count=count)
            if new_keys:
                res.extend(new_keys)

        return res

    def values(self, pattern, count=None, key_type=None):
        keys = self.keys(pattern)
        res = self.get.r_string(keys)
        return res

    # todo: judge the key type and filter keys to different type.


class GetValue(object):
    """
    class used to get value from redis

    note: mget and pipeline ensure the result returned is in order.
    """
    def __init__(self, r):
        self.r = r

    def r_string(self, keys):
        """
        get value of keys from redis and return in type of k, v list
        :param keys:
        :return: [(key, value), ...]
        """
        # as what is said on github, the order of mget is ensured
        # https://github.com/antirez/redis/issues/4647
        values = self.r.mget(keys)
        # return in type of k, v list
        res = zip(keys, values)
        return res

    def r_list(self, keys):
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.lrange(key, 0, -1)
            values = pipe.execute()
        res = zip(keys, values)
        return res

    def r_hash(self, keys):
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key)
            values = pipe.execute()
        res = zip(keys, values)
        return res

    def r_set(self, keys):
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                # todo: sscan
                pipe.smembers(key)
            values = pipe.execute()
        res = zip(keys, values)
        return res

    def r_zset(self, keys):
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.zrange(key, 0, -1)
            values = pipe.execute()
        res = zip(keys, values)
        return res








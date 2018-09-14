#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
read data from redis with key pattern
"""
import redis
from itertools import chain


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
        res = [key.decode() for key in res]
        return res

    def values(self, pattern):
        """
        get values of all keys that match a pattern
        :param pattern:
        :return:
        """
        keys = self.keys(pattern)
        str_keys = self.filter_keys_by_type(keys, "string")
        hash_keys = self.filter_keys_by_type(keys, "hash")
        list_keys = self.filter_keys_by_type(keys, "list")
        set_keys = self.filter_keys_by_type(keys, "set")
        zset_keys = self.filter_keys_by_type(keys, "zset")

        str_res = self.get.r_string(str_keys)
        hash_res = self.get.r_hash(hash_keys)
        list_res = self.get.r_list(list_keys)
        set_res = self.get.r_set(set_keys)
        zset_res = self.get.r_zset(zset_keys)

        return chain(str_res, hash_res, list_res, set_res, zset_res)

    def filter_keys_by_type(self, keys, t):
        """
        judge the key type and filter keys to different type.
        :param keys: keys to filter
        :param t: the type of keys
        :return: filtered keys
        """
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.type(key)
            types = pipe.execute()
            types = [item.decode() for item in types]
        # result in type of (key, key_type)
        key_pairs = zip(keys, types)
        res = [key_pair[0] for key_pair in key_pairs if key_pair[1] == t]
        return res


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
        if not keys:
            # mget could not accept empty list
            return []
        # as what is said on github, the order of mget is ensured
        # https://github.com/antirez/redis/issues/4647
        values = self.r.mget(keys)
        values = self.decode_values(values)
        # return in type of k, v list
        res = zip(keys, values)
        return res

    def r_list(self, keys):
        """
        get redis list values
        :param keys:
        :return:
        """
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.lrange(key, 0, -1)
            values = pipe.execute()
            # values = self.decode_values(values)
        res = zip(keys, values)
        return res

    def r_hash(self, keys):
        """
        get redis hash values
        :param keys:
        :return:
        """
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key)
            values = pipe.execute()
            # values = self.decode_values(values)
        res = zip(keys, values)
        return res

    def r_set(self, keys):
        """
        get redis set values
        :param keys:
        :return:
        """
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                # todo: sscan
                pipe.smembers(key)
            values = pipe.execute()
            # values = self.decode_values(values)
        res = zip(keys, values)
        return res

    def r_zset(self, keys):
        """
        get redis zset values
        :param keys:
        :return:
        """
        with self.r.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.zrange(key, 0, -1)
            values = pipe.execute()
            # values = self.decode_values(values)
        res = zip(keys, values)
        return res

    @staticmethod
    def decode_values(values):
        res = [item.decode() for item in values]
        return res


if __name__ == "__main__":
    redis_reader = RedisReader()
    res_get = redis_reader.values("*")
    for item in res_get:
        print(item)

    # todo: python3 compatible ... byte unicode.




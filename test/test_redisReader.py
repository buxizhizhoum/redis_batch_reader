#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from unittest import TestCase

from src.redis_pattern_reader import RedisReader


class TestRedisReader(TestCase):
    def test_keys(self):
        redis_reader = RedisReader()
        res_get = redis_reader.values("*")
        print(res_get)
        self.fail()

import unittest
from pslx.tool.lru_cache_tool import LRUCacheTool


class LRUCacheToolTest(unittest.TestCase):

    def test_add(self):
        lru_cache_tool = LRUCacheTool(max_capacity=2)
        lru_cache_tool.set(key='key_1', value='value_1')
        self.assertEqual(lru_cache_tool.get_cur_capacity(), 1)
        lru_cache_tool.set(key='key_2', value='value_2')
        self.assertEqual(lru_cache_tool.get_cur_capacity(), 2)
        lru_cache_tool.set(key='key_3', value='value_3')
        self.assertEqual(lru_cache_tool.get_cur_capacity(), 2)

    def test_get(self):
        lru_cache_tool = LRUCacheTool(max_capacity=2)
        lru_cache_tool.set(key='key_1', value='value_1')
        self.assertEqual(lru_cache_tool.get(key='key_1'), 'value_1')
        lru_cache_tool.set(key='key_2', value='value_2')
        lru_cache_tool.set(key='key_3', value='value_3')
        self.assertEqual(lru_cache_tool.get(key='key_3'), 'value_3')
        self.assertEqual(lru_cache_tool.get(key='key_1'), None)
        lru_cache_tool.get(key='key_2')
        lru_cache_tool.set(key='key_4', value='value_4')
        self.assertEqual(lru_cache_tool.get(key='key_3'), None)

import unittest
from pslx.core.linked_list_base import LinkedListBase
from pslx.core.node_base import OrderedNodeBase


class LinkedListBaseTest(unittest.TestCase):

    def test_get_size(self):
        linked_list = LinkedListBase()
        self.assertEqual(linked_list.get_size(), 0)
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        linked_list.add_to_head(test_node_1)
        self.assertEqual(linked_list.get_size(), 1)
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        linked_list.add_to_head(test_node_2)
        self.assertEqual(linked_list.get_size(), 2)

    def test_add_to_head(self):
        linked_list = LinkedListBase()
        for i in range(10):
            test_node = OrderedNodeBase(node_name='test_node_' + str(i))
            linked_list.add_to_head(test_node)
            self.assertEqual(linked_list.get_size(), i + 1)

    def test_delete(self):
        linked_list = LinkedListBase()
        for i in range(10):
            test_node = OrderedNodeBase(node_name='test_node_' + str(i))
            linked_list.add_to_head(test_node)

        linked_list.delete_by_node_name(node_name='test_node_0')
        self.assertEqual(linked_list.get_size(), 9)
        linked_list.delete_by_node_name(node_name='test_node_4')
        self.assertEqual(linked_list.get_size(), 8)
        linked_list.delete_by_node_name(node_name='test_node_xyz')
        self.assertEqual(linked_list.get_size(), 8)

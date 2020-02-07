import unittest
from pslx.core.node_base import OrderedNodeBase, UnorderedNodeBase
from pslx.schema.enums_pb2 import SortOrder


class NodeBaseTest(unittest.TestCase):

    def test_unordered_node_children_ordered(self):
        test_node = UnorderedNodeBase(node_name='test_node')
        self.assertFalse(test_node.is_children_ordered())

    def test_ordered_node_children_ordered(self):
        test_node = OrderedNodeBase(node_name='test_node')
        self.assertTrue(test_node.is_children_ordered())

    def test_get_node_name(self):
        test_node = UnorderedNodeBase(node_name='test_node')
        self.assertEqual(test_node.get_node_name(), 'test_node')

    def test_get_children(self):
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node_1 = UnorderedNodeBase(node_name='child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='child_node_2')
        test_parent_node.add_child(child_node=test_child_node_1)
        test_parent_node.add_child(child_node=test_child_node_2)

        expected_children = {
            'child_node_1': test_child_node_1,
            'child_node_2': test_child_node_2,
        }
        self.assertDictEqual(test_parent_node.get_children(), expected_children)

    def test_get_children_names(self):
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node_1 = UnorderedNodeBase(node_name='child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='child_node_2')
        test_parent_node.add_child(child_node=test_child_node_1)
        test_parent_node.add_child(child_node=test_child_node_2)
        self.assertSetEqual(set(test_parent_node.get_children_names()), {'child_node_1', 'child_node_2'})

    def test_get_children_nodes(self):
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node_1 = UnorderedNodeBase(node_name='child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='child_node_2')
        test_parent_node.add_child(child_node=test_child_node_1)
        test_parent_node.add_child(child_node=test_child_node_2)
        self.assertSetEqual(set(test_parent_node.get_children_nodes()), {test_child_node_1, test_child_node_2})

    def test_get_num_children(self):
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node_1 = UnorderedNodeBase(node_name='child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='child_node_2')
        test_parent_node.add_child(child_node=test_child_node_1)
        test_parent_node.add_child(child_node=test_child_node_2)
        self.assertEqual(test_parent_node.get_num_children(), 2)

    def test_get_parents(self):
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node_1 = UnorderedNodeBase(node_name='parent_node_1')
        test_parent_node_2 = UnorderedNodeBase(node_name='parent_node_2')
        test_child_node.add_parent(parent_node=test_parent_node_1)
        test_child_node.add_parent(parent_node=test_parent_node_2)

        expected_parents = {
            'parent_node_1': test_parent_node_1,
            'parent_node_2': test_parent_node_2,
        }
        self.assertDictEqual(test_child_node.get_parents(), expected_parents)

    def test_get_parents_names(self):
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node_1 = UnorderedNodeBase(node_name='parent_node_1')
        test_parent_node_2 = UnorderedNodeBase(node_name='parent_node_2')
        test_child_node.add_parent(parent_node=test_parent_node_1)
        test_child_node.add_parent(parent_node=test_parent_node_2)
        self.assertSetEqual(set(test_child_node.get_parents_names()), {'parent_node_1', 'parent_node_2'})

    def test_get_parents_nodes(self):
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node_1 = UnorderedNodeBase(node_name='parent_node_1')
        test_parent_node_2 = UnorderedNodeBase(node_name='parent_node_2')
        test_child_node.add_parent(parent_node=test_parent_node_1)
        test_child_node.add_parent(parent_node=test_parent_node_2)
        self.assertSetEqual(set(test_child_node.get_parents_nodes()), {test_parent_node_1, test_parent_node_2})

    def test_get_num_parents(self):
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node_1 = UnorderedNodeBase(node_name='parent_node_1')
        test_parent_node_2 = UnorderedNodeBase(node_name='parent_node_2')
        test_child_node.add_parent(parent_node=test_parent_node_1)
        test_child_node.add_parent(parent_node=test_parent_node_2)
        self.assertEqual(test_child_node.get_num_parents(), 2)

    def test_delete_child(self):
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node_1 = UnorderedNodeBase(node_name='child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='child_node_2')
        test_parent_node.add_child(child_node=test_child_node_1)
        test_parent_node.add_child(child_node=test_child_node_2)

        test_parent_node.delete_child(child_node=test_child_node_1)
        self.assertEqual(test_parent_node.get_num_children(), 1)

        test_parent_node.delete_child(child_node=test_child_node_2)
        self.assertEqual(test_parent_node.get_num_children(), 0)

    def test_delete_parent(self):
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node_1 = UnorderedNodeBase(node_name='parent_node_1')
        test_parent_node_2 = UnorderedNodeBase(node_name='parent_node_2')
        test_child_node.add_parent(parent_node=test_parent_node_1)
        test_child_node.add_parent(parent_node=test_parent_node_2)

        test_child_node.delete_parent(parent_node=test_parent_node_1)
        self.assertEqual(test_child_node.get_num_parents(), 1)

        test_child_node.delete_parent(parent_node=test_parent_node_2)
        self.assertEqual(test_child_node.get_num_parents(), 0)

    def test_get_child(self):
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node.add_child(child_node=test_child_node)

        self.assertEqual(test_parent_node.get_child(child_name='child_node'), test_child_node)
        self.assertEqual(test_parent_node.get_child(child_name='child_node_0'), None)

    def test_has_child(self):
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node.add_child(child_node=test_child_node)

        self.assertTrue(test_parent_node.has_child(child_name='child_node'))
        self.assertFalse(test_parent_node.has_child(child_name='child_node_0'))

    def test_get_parent(self):
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node.add_parent(parent_node=test_parent_node)

        self.assertEqual(test_child_node.get_parent(parent_name='parent_node'), test_parent_node)
        self.assertEqual(test_child_node.get_parent(parent_name='parent_node_0'), None)

    def test_has_parent(self):
        test_child_node = UnorderedNodeBase(node_name='child_node')
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node.add_parent(parent_node=test_parent_node)

        self.assertTrue(test_child_node.has_parent(parent_name='parent_node'))
        self.assertFalse(test_child_node.has_parent(parent_name='parent_node_0'))

    def test_combined_tests_1(self):
        test_child_node_1 = UnorderedNodeBase(node_name='child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='child_node_2')
        test_parent_node_1 = UnorderedNodeBase(node_name='parent_node_1')
        test_parent_node_2 = UnorderedNodeBase(node_name='parent_node_2')

        test_parent_node_1.add_child(child_node=test_child_node_1)
        test_parent_node_2.add_child(child_node=test_child_node_1)
        test_parent_node_1.add_child(child_node=test_child_node_2)
        test_parent_node_2.add_child(child_node=test_child_node_2)

        self.assertSetEqual(set(test_parent_node_1.get_children_names()), {'child_node_1', 'child_node_2'})
        self.assertSetEqual(set(test_parent_node_2.get_children_names()), {'child_node_1', 'child_node_2'})
        self.assertSetEqual(set(test_child_node_1.get_parents_names()), {'parent_node_1', 'parent_node_2'})
        self.assertSetEqual(set(test_child_node_2.get_parents_names()), {'parent_node_1', 'parent_node_2'})

    def test_combined_tests_2(self):
        test_parent_node = UnorderedNodeBase(node_name='parent_node')
        test_child_node_1 = UnorderedNodeBase(node_name='child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='child_node_2')
        test_parent_node.add_child(child_node=test_child_node_1)
        test_parent_node.add_child(child_node=test_child_node_2)

        test_parent_node.delete_child(child_node=test_child_node_1)
        self.assertEqual(test_parent_node.get_num_children(), 1)

        test_parent_node.add_child(child_node=test_child_node_1)
        self.assertEqual(test_parent_node.get_num_children(), 2)

    def test_combined_tests_3(self):
        test_child_node_1 = UnorderedNodeBase(node_name='child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='child_node_2')
        test_parent_node_1 = UnorderedNodeBase(node_name='parent_node_1')
        test_parent_node_2 = UnorderedNodeBase(node_name='parent_node_2')

        test_parent_node_1.add_child(child_node=test_child_node_1)
        test_parent_node_2.add_child(child_node=test_child_node_1)
        test_parent_node_1.add_child(child_node=test_child_node_2)
        test_parent_node_2.add_child(child_node=test_child_node_2)

        test_parent_node_1.delete_child(child_node=test_child_node_1)

        self.assertSetEqual(set(test_parent_node_1.get_children_names()), {'child_node_2'})
        self.assertSetEqual(set(test_parent_node_2.get_children_names()), {'child_node_1', 'child_node_2'})
        self.assertSetEqual(set(test_child_node_1.get_parents_names()), {'parent_node_2'})
        self.assertSetEqual(set(test_child_node_2.get_parents_names()), {'parent_node_1', 'parent_node_2'})

    def test_combined_tests_4(self):
        test_node_1 = UnorderedNodeBase(node_name='node_1')
        test_node_2 = UnorderedNodeBase(node_name='node_2')
        test_node_3 = UnorderedNodeBase(node_name='node_3')

        test_node_1.add_child(child_node=test_node_2)
        test_node_2.add_child(child_node=test_node_3)
        self.assertEqual(test_node_3.get_parent(parent_name='node_2').get_parent(parent_name='node_1'), test_node_1)

    def test_ordered_node_order(self):
        test_parent_node = OrderedNodeBase(node_name='parent_node', order=SortOrder.ORDER)
        test_child_node_1 = OrderedNodeBase(node_name='child_node_1', order=SortOrder.ORDER)
        test_child_node_2 = OrderedNodeBase(node_name='child_node_2', order=SortOrder.ORDER)
        test_child_node_3 = OrderedNodeBase(node_name='child_node_3', order=SortOrder.ORDER)

        test_parent_node.add_child(child_node=test_child_node_1)
        test_parent_node.add_child(child_node=test_child_node_2)
        test_parent_node.add_child(child_node=test_child_node_3)
        self.assertListEqual(test_parent_node.get_children_names(), ['child_node_1', 'child_node_2', 'child_node_3'])

    def test_ordered_node_reverse_1(self):
        test_parent_node = OrderedNodeBase(node_name='parent_node', order=SortOrder.REVERSE)
        test_child_node_1 = OrderedNodeBase(node_name='child_node_1', order=SortOrder.REVERSE)
        test_child_node_2 = OrderedNodeBase(node_name='child_node_2', order=SortOrder.REVERSE)
        test_child_node_3 = OrderedNodeBase(node_name='child_node_3', order=SortOrder.REVERSE)

        test_parent_node.add_child(child_node=test_child_node_1)
        test_parent_node.add_child(child_node=test_child_node_2)
        test_parent_node.add_child(child_node=test_child_node_3)
        self.assertListEqual(test_parent_node.get_children_names(), ['child_node_3', 'child_node_2', 'child_node_1'])

    def test_ordered_node_reverse_2(self):
        test_child_node = OrderedNodeBase(node_name='child_node', order=SortOrder.REVERSE)
        test_parent_node_1 = OrderedNodeBase(node_name='parent_node_1', order=SortOrder.REVERSE)
        test_parent_node_2 = OrderedNodeBase(node_name='parent_node_2', order=SortOrder.REVERSE)
        test_parent_node_3 = OrderedNodeBase(node_name='parent_node_3', order=SortOrder.REVERSE)

        test_child_node.add_parent(parent_node=test_parent_node_1)
        test_child_node.add_parent(parent_node=test_parent_node_2)
        test_child_node.add_parent(parent_node=test_parent_node_3)
        self.assertListEqual(test_child_node.get_parents_names(), ['parent_node_3', 'parent_node_2', 'parent_node_1'])


if __name__ == '__main__':
    unittest.main()

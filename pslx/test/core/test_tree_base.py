import unittest
from pslx.core.node_base import UnorderedNodeBase, OrderedNodeBase
from pslx.core.tree_base import TreeBase


class TreeBaseTest(unittest.TestCase):

    def test_get_root_node(self):
        test_root_node = UnorderedNodeBase(node_name='test_root_node')
        test_tree = TreeBase(root=test_root_node)
        self.assertEqual(test_tree.get_root_node(), test_root_node)

    def test_set_root_node(self):
        test_root_node = UnorderedNodeBase(node_name='test_root_node')
        test_tree = TreeBase(root=test_root_node)
        test_root_node_2 = UnorderedNodeBase(node_name='test_root_node_2')
        test_tree.set_root_node(root=test_root_node_2)
        self.assertEqual(test_tree.get_root_node(), test_root_node_2)

    def test_add_node(self):
        test_parent_node = UnorderedNodeBase(node_name='test_parent_node')
        test_child_node = UnorderedNodeBase(node_name='test_child_node')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node
        )
        self.assertEqual(test_tree.get_tree_size(), 2)

    def test_find_node(self):
        test_parent_node = UnorderedNodeBase(node_name='test_parent_node')
        test_child_node = UnorderedNodeBase(node_name='test_child_node')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node
        )
        self.assertEqual(test_tree.find_node(node_name='test_parent_node'), test_parent_node)
        self.assertEqual(test_tree.find_node(node_name='test_child_node'), test_child_node)
        self.assertEqual(test_tree.find_node(node_name='random_node'), None)

    def test_get_tree_size(self):
        test_parent_node = UnorderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = UnorderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='test_child_node_2')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_2
        )
        self.assertEqual(test_tree.get_tree_size(), 3)

    def test_get_sub_tree_size(self):
        test_parent_node = UnorderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = UnorderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = UnorderedNodeBase(node_name='test_child_node_2')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_2
        )
        self.assertEqual(test_tree.get_subtree_size(node=test_parent_node), 3)
        self.assertEqual(test_tree.get_subtree_size(node=test_child_node_1), 2)

    def test_bfs_search(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_2
        )
        self.assertListEqual(
            test_tree.bfs_search(max_num_node=-1),
            ['test_parent_node', 'test_child_node_1', 'test_child_node_2']
        )
        self.assertListEqual(
            test_tree.bfs_search(max_num_node=2),
            ['test_parent_node', 'test_child_node_1']
        )

    def test_dfs_search(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_2
        )
        self.assertListEqual(
            test_tree.dfs_search(max_num_node=-1),
            ['test_parent_node', 'test_child_node_1', 'test_child_node_2']
        )
        test_child_node_3 = OrderedNodeBase(node_name='test_child_node_3')
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_3
        )
        self.assertListEqual(
            test_tree.dfs_search(max_num_node=-1),
            ['test_parent_node', 'test_child_node_3', 'test_child_node_1', 'test_child_node_2']
        )
        test_child_node_4 = OrderedNodeBase(node_name='test_child_node_4')
        test_tree.add_node(
            parent_node=test_child_node_3,
            child_node=test_child_node_4
        )
        self.assertListEqual(
            test_tree.dfs_search(max_num_node=-1),
            ['test_parent_node', 'test_child_node_3', 'test_child_node_4', 'test_child_node_1', 'test_child_node_2']
        )
        test_child_node_5 = OrderedNodeBase(node_name='test_child_node_5')
        test_tree.add_node(
            parent_node=test_child_node_3,
            child_node=test_child_node_5
        )
        self.assertListEqual(
            test_tree.dfs_search(max_num_node=-1),
            ['test_parent_node', 'test_child_node_3', 'test_child_node_5', 'test_child_node_4', 'test_child_node_1',
             'test_child_node_2']
        )

    def test_trim_tree_1(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_2
        )
        test_tree.trim_tree(max_capacity=2)
        self.assertListEqual(test_tree.bfs_search(), ['test_parent_node', 'test_child_node_2'])
        self.assertEqual(test_tree.get_tree_size(), 2)

    def test_trim_tree_2(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_2
        )
        test_tree.trim_tree(max_capacity=2)
        self.assertListEqual(test_tree.bfs_search(), ['test_parent_node', 'test_child_node_1'])
        self.assertEqual(test_tree.get_tree_size(), 2)

    def test_trim_tree_3(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_child_node_3 = OrderedNodeBase(node_name='test_child_node_3')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_3
        )
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_2
        )
        test_tree.trim_tree(max_capacity=3)
        self.assertListEqual(test_tree.bfs_search(), ['test_parent_node', 'test_child_node_1', 'test_child_node_2'])
        self.assertEqual(test_tree.get_tree_size(), 3)

    def test_trim_tree_4(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_child_node_3 = OrderedNodeBase(node_name='test_child_node_3')
        test_child_node_4 = OrderedNodeBase(node_name='test_child_node_4')
        test_child_node_5 = OrderedNodeBase(node_name='test_child_node_5')
        test_child_node_6 = OrderedNodeBase(node_name='test_child_node_6')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_2
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_3
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_4
        )
        test_tree.add_node(
            parent_node=test_child_node_2,
            child_node=test_child_node_5
        )
        test_tree.add_node(
            parent_node=test_child_node_2,
            child_node=test_child_node_6
        )
        test_tree.trim_tree(max_capacity=3)
        self.assertListEqual(test_tree.bfs_search(), ['test_parent_node', 'test_child_node_2', 'test_child_node_6'])

    def test_trim_tree_5(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_child_node_3 = OrderedNodeBase(node_name='test_child_node_3')
        test_child_node_4 = OrderedNodeBase(node_name='test_child_node_4')
        test_child_node_5 = OrderedNodeBase(node_name='test_child_node_5')
        test_child_node_6 = OrderedNodeBase(node_name='test_child_node_6')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_2
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_3
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_4
        )
        test_tree.add_node(
            parent_node=test_child_node_2,
            child_node=test_child_node_5
        )
        test_tree.add_node(
            parent_node=test_child_node_2,
            child_node=test_child_node_6
        )
        test_tree.trim_tree(max_capacity=4)
        self.assertListEqual(
            test_tree.bfs_search(),
            ['test_parent_node', 'test_child_node_2', 'test_child_node_5', 'test_child_node_6']
        )
        self.assertEqual(test_tree.get_tree_size(), 4)

    def test_trim_tree_6(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_child_node_3 = OrderedNodeBase(node_name='test_child_node_3')
        test_child_node_4 = OrderedNodeBase(node_name='test_child_node_4')
        test_child_node_5 = OrderedNodeBase(node_name='test_child_node_5')
        test_child_node_6 = OrderedNodeBase(node_name='test_child_node_6')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_2
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_3
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_4
        )
        test_tree.add_node(
            parent_node=test_child_node_2,
            child_node=test_child_node_5
        )
        test_tree.add_node(
            parent_node=test_child_node_2,
            child_node=test_child_node_6
        )
        test_tree.trim_tree(max_capacity=5)
        self.assertListEqual(
            test_tree.bfs_search(),
            ['test_parent_node', 'test_child_node_1', 'test_child_node_2', 'test_child_node_5', 'test_child_node_6']
        )
        self.assertEqual(test_tree.get_tree_size(), 5)

    def test_get_leaves(self):
        test_parent_node = OrderedNodeBase(node_name='test_parent_node')
        test_child_node_1 = OrderedNodeBase(node_name='test_child_node_1')
        test_child_node_2 = OrderedNodeBase(node_name='test_child_node_2')
        test_tree = TreeBase(root=test_parent_node)
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_1
        )
        test_tree.add_node(
            parent_node=test_child_node_1,
            child_node=test_child_node_2
        )
        self.assertListEqual(test_tree.get_leaves(), ['test_child_node_2'])
        test_child_node_3 = OrderedNodeBase(node_name='test_child_node_3')
        test_tree.add_node(
            parent_node=test_parent_node,
            child_node=test_child_node_3
        )
        self.assertListEqual(test_tree.get_leaves(), ['test_child_node_3', 'test_child_node_2'])


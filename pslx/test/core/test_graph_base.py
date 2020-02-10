import unittest
from pslx.core.node_base import OrderedNodeBase
from pslx.core.graph_base import GraphBase


class GraphBaseTest(unittest.TestCase):

    def test_add_direct_edge(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        self.assertEqual(test_graph.get_num_nodes(), 2)
        self.assertEqual(test_graph.get_num_edges(), 1)
        self.assertTrue(test_node_1.has_child(child_name='test_node_2'))
        self.assertFalse(test_node_2.has_child(child_name='test_node_1'))

    def test_add_indirect_edge(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_graph = GraphBase()
        test_graph.add_indirect_edge(node_1=test_node_1, node_2=test_node_2)
        self.assertEqual(test_graph.get_num_nodes(), 2)
        self.assertEqual(test_graph.get_num_edges(), 1)
        self.assertTrue(test_node_1.has_child(child_name='test_node_2'))
        self.assertTrue(test_node_2.has_child(child_name='test_node_1'))

    def test_is_dag_1(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_graph = GraphBase()
        test_graph.add_indirect_edge(node_1=test_node_1, node_2=test_node_2)
        self.assertFalse(test_graph.is_dag())

    def test_is_dag_2(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_2, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_3, to_node=test_node_1)
        self.assertFalse(test_graph.is_dag())

    def test_is_dag_3(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_node_4 = OrderedNodeBase(node_name='test_node_4')
        test_node_5 = OrderedNodeBase(node_name='test_node_5')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_3, to_node=test_node_4)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_5)
        test_graph.add_direct_edge(from_node=test_node_5, to_node=test_node_3)
        self.assertFalse(test_graph.is_dag())

    def test_is_dag_4(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_2, to_node=test_node_3)
        self.assertTrue(test_graph.is_dag())

    def test_is_dag_5(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_node_4 = OrderedNodeBase(node_name='test_node_4')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_3)
        self.assertTrue(test_graph.is_dag())

    def test_is_connected_1(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_2, to_node=test_node_3)
        self.assertTrue(test_graph.is_connected())

    def test_is_connected_2(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_node_4 = OrderedNodeBase(node_name='test_node_4')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_3)
        self.assertTrue(test_graph.is_connected())

    def test_is_connected_3(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_node_4 = OrderedNodeBase(node_name='test_node_4')
        test_node_5 = OrderedNodeBase(node_name='test_node_5')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_3, to_node=test_node_4)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_5)
        test_graph.add_direct_edge(from_node=test_node_5, to_node=test_node_3)
        self.assertFalse(test_graph.is_connected())

    def test_get_source_nodes(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_node_4 = OrderedNodeBase(node_name='test_node_4')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_3)
        self.assertListEqual(test_graph.get_source_nodes(), [test_node_1, test_node_4])

    def test_replace_node(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_node_4 = OrderedNodeBase(node_name='test_node_4')
        test_node_5 = OrderedNodeBase(node_name='test_node_5')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_3)

        test_graph.replace_node(old_node=test_node_4, new_node=test_node_5)
        self.assertListEqual(
            test_graph.topological_sort(),
            [('test_node_1', 0), ('test_node_5', 0), ('test_node_2', 1), ('test_node_3', 1)]
        )

    def test_topological_sort_1(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)

        test_graph.add_direct_edge(from_node=test_node_3, to_node=test_node_2)
        self.assertListEqual(test_graph.topological_sort(),
                             [('test_node_1', 0), ('test_node_3', 1), ('test_node_2', 2)])
        self.assertDictEqual(
            test_graph.get_node_levels(),
            {0: ['test_node_1'], 1: ['test_node_3'], 2: ['test_node_2']}
        )

    def test_topological_sort_2(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_node_4 = OrderedNodeBase(node_name='test_node_4')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_4)
        test_graph.add_direct_edge(from_node=test_node_3, to_node=test_node_4)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_2)
        self.assertListEqual(test_graph.topological_sort(),
                             [('test_node_1', 0), ('test_node_3', 1), ('test_node_4', 2), ('test_node_2', 3)])
        self.assertDictEqual(
            test_graph.get_node_levels(),
            {0: ['test_node_1'], 1: ['test_node_3'], 2: ['test_node_4'], 3: ['test_node_2']}
        )

    def test_topological_sort_3(self):
        test_node_1 = OrderedNodeBase(node_name='test_node_1')
        test_node_2 = OrderedNodeBase(node_name='test_node_2')
        test_node_3 = OrderedNodeBase(node_name='test_node_3')
        test_node_4 = OrderedNodeBase(node_name='test_node_4')
        test_node_5 = OrderedNodeBase(node_name='test_node_5')
        test_graph = GraphBase()
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_1, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_2)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_3)
        test_graph.add_direct_edge(from_node=test_node_4, to_node=test_node_5)
        self.assertDictEqual(
            test_graph.get_node_levels(),
            {0: ['test_node_1', 'test_node_4'], 1: ['test_node_2', 'test_node_3', 'test_node_5']}
        )


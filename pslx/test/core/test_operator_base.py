import unittest

from pslx.schema.enums_pb2 import DataModelType, Status
from pslx.schema.snapshots_pb2 import OperatorSnapshot, NodeSnapshot
from pslx.util.dummy_util import DummyUtil


class TestOperatorBase(unittest.TestCase):

    def test_set_data_model(self):
        test_operator = DummyUtil.dummy_operator(node_name='test_operator')
        test_operator.set_data_model(model=DataModelType.BATCH)
        self.assertEqual(test_operator.get_data_model(), DataModelType.BATCH)

    def test_unset_model(self):
        test_operator = DummyUtil.dummy_operator(node_name='test_operator')
        test_operator.unset_data_model()
        self.assertEqual(test_operator.get_data_model(), DataModelType.DEFAULT)

    def test_set_status(self):
        test_operator = DummyUtil.dummy_operator(node_name='test_operator')
        test_operator.set_status(status=Status.FAILED)
        self.assertEqual(test_operator.get_status(), Status.FAILED)

    def test_unset_status(self):
        test_operator = DummyUtil.dummy_operator(node_name='test_operator')
        test_operator.unset_status()
        self.assertEqual(test_operator.get_status(), Status.IDLE)

    def test_wait_for_upstream_status(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.add_child(child_node=test_operator_2)
        test_operator_1.set_status(status=Status.SUCCEEDED)
        self.assertTrue(test_operator_2.wait_for_upstream_status())

    def test_is_data_model_consistent_1(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.set_data_model(model=DataModelType.BATCH)
        test_operator_2.set_data_model(model=DataModelType.BATCH)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertTrue(test_operator_2.is_data_model_consistent())

    def test_is_data_model_consistent_2(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.set_data_model(model=DataModelType.BATCH)
        test_operator_2.set_data_model(model=DataModelType.STREAMING)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertFalse(test_operator_2.is_data_model_consistent())

    def test_is_status_consistent_1(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.set_status(status=Status.IDLE)
        test_operator_2.set_status(status=Status.RUNNING)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertFalse(test_operator_2.is_status_consistent())

    def test_is_status_consistent_2(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.set_status(status=Status.RUNNING)
        test_operator_2.set_status(status=Status.SUCCEEDED)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertFalse(test_operator_2.is_status_consistent())

    def test_is_status_consistent_3(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.set_status(status=Status.FAILED)
        test_operator_2.set_status(status=Status.SUCCEEDED)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertFalse(test_operator_2.is_status_consistent())

    def test_is_status_consistent_4(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.set_status(status=Status.SUCCEEDED)
        test_operator_2.set_status(status=Status.FAILED)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertTrue(test_operator_2.is_status_consistent())

    def test_take_snapshot_1(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.set_status(status=Status.SUCCEEDED)
        test_operator_2.set_status(status=Status.FAILED)
        test_operator_1.add_child(child_node=test_operator_2)
        expected_node_snapshot = NodeSnapshot()
        expected_node_snapshot.node_name = 'test_operator_1'
        expected_node_snapshot.children_names.extend(['test_operator_2'])
        expected_operator_snapshot = OperatorSnapshot()
        expected_operator_snapshot.operator_name = 'test_operator_1'
        expected_operator_snapshot.data_model = DataModelType.DEFAULT
        expected_operator_snapshot.status = Status.SUCCEEDED
        expected_operator_snapshot.node_snapshot.CopyFrom(expected_node_snapshot)
        self.assertEqual(test_operator_1.get_operator_snapshot(), expected_operator_snapshot)

    def test_take_snapshot_2(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_3 = DummyUtil.dummy_operator(node_name='test_operator_3')
        test_operator_1.set_status(status=Status.SUCCEEDED)
        test_operator_2.set_status(status=Status.FAILED)
        test_operator_3.set_status(status=Status.SUCCEEDED)
        test_operator_1.add_child(child_node=test_operator_2)
        test_operator_1.add_child(child_node=test_operator_3)
        expected_node_snapshot = NodeSnapshot()
        expected_node_snapshot.node_name = 'test_operator_1'
        expected_node_snapshot.children_names.extend(['test_operator_2', 'test_operator_3'])
        expected_operator_snapshot = OperatorSnapshot()
        expected_operator_snapshot.operator_name = 'test_operator_1'
        expected_operator_snapshot.data_model = DataModelType.DEFAULT
        expected_operator_snapshot.status = Status.SUCCEEDED
        expected_operator_snapshot.node_snapshot.CopyFrom(expected_node_snapshot)
        self.assertEqual(test_operator_1.get_operator_snapshot(), expected_operator_snapshot)

    def test_execution_1(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_1.add_child(child_node=test_operator_2)
        test_operator_1.set_status(status=Status.SUCCEEDED)
        test_operator_1.execute()
        self.assertEqual(test_operator_1.get_operator_snapshot().status, Status.SUCCEEDED)

    def test_execution_2(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_3 = DummyUtil.dummy_operator(node_name='test_operator_3')
        test_operator_1.add_child(child_node=test_operator_2)
        test_operator_2.add_child(child_node=test_operator_3)
        test_operator_1.set_status(status=Status.SUCCEEDED)
        test_operator_2.execute()
        test_operator_3.execute()
        self.assertEqual(test_operator_2.get_operator_snapshot().status, Status.SUCCEEDED)
        self.assertEqual(test_operator_3.get_operator_snapshot().status, Status.SUCCEEDED)

    def test_execution_3(self):
        test_operator_1 = DummyUtil.dummy_operator(node_name='test_operator_1')
        test_operator_2 = DummyUtil.dummy_operator(node_name='test_operator_2')
        test_operator_3 = DummyUtil.dummy_operator(node_name='test_operator_3')
        test_operator_1.add_child(child_node=test_operator_2)
        test_operator_2.add_child(child_node=test_operator_3)
        test_operator_1.set_status(status=Status.FAILED)
        test_operator_2.execute()
        test_operator_3.execute()

        self.assertEqual(test_operator_2.get_operator_snapshot().status, Status.FAILED)
        self.assertEqual(test_operator_3.get_operator_snapshot().status, Status.FAILED)

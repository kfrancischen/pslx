import unittest
from pslx.schema.enums_pb2 import DataModelType
from pslx.schema.enums_pb2 import OperatorStatus
from pslx.util.dummy_util import DummyOperator


class TestOperatorBase(unittest.TestCase):

    def test_set_data_model(self):
        test_operator = DummyOperator(node_name='test_operator')
        test_operator.set_data_model(model=DataModelType.BATCH)
        self.assertEqual(test_operator.get_data_model(), DataModelType.BATCH)

    def test_unset_model(self):
        test_operator = DummyOperator(node_name='test_operator')
        test_operator.unset_data_model()
        self.assertEqual(test_operator.get_data_model(), DataModelType.DEFAULT)

    def test_set_status(self):
        test_operator = DummyOperator(node_name='test_operator')
        test_operator.set_status(status=OperatorStatus.FAILED)
        self.assertEqual(test_operator.get_status(), OperatorStatus.FAILED)

    def test_unset_status(self):
        test_operator = DummyOperator(node_name='test_operator')
        test_operator.unset_status()
        self.assertEqual(test_operator.get_status(), OperatorStatus.IDLE)

    def test_wait_for_upstream_status(self):
        test_operator_1 = DummyOperator(node_name='test_operator_1')
        test_operator_2 = DummyOperator(node_name='test_operator_2')
        test_operator_1.add_child(child_node=test_operator_2)
        test_operator_1.set_status(status=OperatorStatus.SUCCEEDED)
        self.assertTrue(test_operator_2.wait_for_upstream_status())

    def test_is_data_model_consistent_1(self):
        test_operator_1 = DummyOperator(node_name='test_operator_1')
        test_operator_2 = DummyOperator(node_name='test_operator_2')
        test_operator_1.set_data_model(model=DataModelType.BATCH)
        test_operator_2.set_data_model(model=DataModelType.BATCH)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertTrue(test_operator_2.is_data_model_consistent())

    def test_is_data_model_consistent_2(self):
        test_operator_1 = DummyOperator(node_name='test_operator_1')
        test_operator_2 = DummyOperator(node_name='test_operator_2')
        test_operator_1.set_data_model(model=DataModelType.BATCH)
        test_operator_2.set_data_model(model=DataModelType.STREAMING)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertFalse(test_operator_2.is_data_model_consistent())

    def test_is_status_consistent_1(self):
        test_operator_1 = DummyOperator(node_name='test_operator_1')
        test_operator_2 = DummyOperator(node_name='test_operator_2')
        test_operator_1.set_status(status=OperatorStatus.IDLE)
        test_operator_2.set_status(status=OperatorStatus.RUNNING)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertFalse(test_operator_2.is_status_consistent())

    def test_is_status_consistent_2(self):
        test_operator_1 = DummyOperator(node_name='test_operator_1')
        test_operator_2 = DummyOperator(node_name='test_operator_2')
        test_operator_1.set_status(status=OperatorStatus.RUNNING)
        test_operator_2.set_status(status=OperatorStatus.SUCCEEDED)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertFalse(test_operator_2.is_status_consistent())

    def test_is_status_consistent_3(self):
        test_operator_1 = DummyOperator(node_name='test_operator_1')
        test_operator_2 = DummyOperator(node_name='test_operator_2')
        test_operator_1.set_status(status=OperatorStatus.FAILED)
        test_operator_2.set_status(status=OperatorStatus.SUCCEEDED)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertFalse(test_operator_2.is_status_consistent())

    def test_is_status_consistent_4(self):
        test_operator_1 = DummyOperator(node_name='test_operator_1')
        test_operator_2 = DummyOperator(node_name='test_operator_2')
        test_operator_1.set_status(status=OperatorStatus.SUCCEEDED)
        test_operator_2.set_status(status=OperatorStatus.FAILED)
        test_operator_1.add_child(child_node=test_operator_2)
        self.assertTrue(test_operator_2.is_status_consistent())

from collections import OrderedDict, defaultdict
from pslx.core.base import Base
from pslx.schema.enums_pb2 import SortOrder
from pslx.schema.snapshots_pb2 import NodeSnapshot


class NodeBase(Base):
    CHILDREN_DATA_STRUCT = None
    PARENTS_DATA_STRUCT = None
    IS_ORDERED = False

    def __init__(self, node_name):
        super().__init__()
        self._node_name = node_name
        self._children = self.CHILDREN_DATA_STRUCT()
        self._parents = self.PARENTS_DATA_STRUCT()

    @classmethod
    def is_children_ordered(cls):
        return cls.IS_ORDERED

    def get_node_name(self):
        return self._node_name

    def get_children(self):
        return self._children

    def get_children_names(self):
        return list(self._children.keys())

    def get_children_nodes(self):
        return list(self._children.values())

    def get_num_children(self):
        return len(self._children)

    def get_parents(self):
        return self._parents

    def get_parents_names(self):
        return list(self._parents.keys())

    def get_parents_nodes(self):
        return list(self._parents.values())

    def get_num_parents(self):
        return len(self._parents)

    def add_child(self, child_node):
        raise NotImplementedError

    def delete_child(self, child_node):
        self._children.pop(child_node.get_node_name(), None)
        if child_node.has_parent(self._node_name):
            child_node.delete_parent(parent_node=self)

    def add_parent(self, parent_node):
        raise NotImplementedError

    def delete_parent(self, parent_node):
        self._parents.pop(parent_node.get_node_name(), None)
        if parent_node.has_child(self._node_name):
            parent_node.delete_child(child_node=self)

    def get_child(self, child_name):
        return self._children.get(child_name, None)

    def has_child(self, child_name):
        return child_name in self._children

    def get_parent(self, parent_name):
        return self._parents.get(parent_name, None)

    def has_parent(self, parent_name):
        return parent_name in self._parents

    def get_node_snapshot(self):
        node_snapshot = NodeSnapshot()
        node_snapshot.node_name = self._node_name
        for child_name in self.get_children_names():
            node_snapshot.children_names.append(child_name)
        for parent_name in self.get_parents_names():
            node_snapshot.parents_names.append(parent_name)
        return node_snapshot


class UnorderedNodeBase(NodeBase):
    CHILDREN_DATA_STRUCT = defaultdict
    PARENTS_DATA_STRUCT = defaultdict

    def __init__(self, node_name):
        super().__init__(node_name=node_name)

    def add_child(self, child_node):
        assert child_node != self
        if self.has_child(child_name=child_node.get_node_name()):
            self.sys_log("Node name duplicated. Will overwrite the previous node.")
        self._children[child_node.get_node_name()] = child_node
        if not child_node.has_parent(parent_name=self._node_name):
            child_node.add_parent(parent_node=self)

    def add_parent(self, parent_node):
        assert parent_node != self
        if self.has_parent(parent_name=parent_node.get_node_name()):
            self.sys_log("Node name duplicated. Will overwrite the previous node.")
        self._parents[parent_node.get_node_name()] = parent_node
        if not parent_node.has_child(child_name=self._node_name):
            parent_node.add_child(child_node=self)


class OrderedNodeBase(NodeBase):
    CHILDREN_DATA_STRUCT = OrderedDict
    PARENTS_DATA_STRUCT = OrderedDict
    IS_ORDERED = True

    def __init__(self, node_name, order=SortOrder.ORDER):
        super().__init__(node_name=node_name)
        self._order = order

    def add_child(self, child_node):
        assert child_node != self
        if self.has_child(child_name=child_node.get_node_name()):
            self.sys_log("Node name duplicated. Will overwrite the previous node.")
        self._children[child_node.get_node_name()] = child_node
        if self._order == SortOrder.REVERSE:
            self._children.move_to_end(key=child_node.get_node_name(), last=False)
        if not child_node.has_parent(parent_name=self._node_name):
            child_node.add_parent(parent_node=self)

    def add_parent(self, parent_node):
        assert parent_node != self
        if self.has_parent(parent_name=parent_node.get_node_name()):
            self.sys_log("Node name duplicated. Will overwrite the previous node.")
        self._parents[parent_node.get_node_name()] = parent_node
        if self._order == SortOrder.REVERSE:
            self._parents.move_to_end(key=parent_node.get_node_name(), last=False)
        if not parent_node.has_child(child_name=self._node_name):
            parent_node.add_child(child_node=self)

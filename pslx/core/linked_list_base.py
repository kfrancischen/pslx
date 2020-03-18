from pslx.core.base import Base
from pslx.core.node_base import OrderedNodeBase


class LinkedListBase(Base):
    def __init__(self):
        super().__init__()
        self._dummy_node = OrderedNodeBase('LINKED_LIST_BASE_DUMMY_NODE')
        self._tail = self._dummy_node
        self._node_dict = {}

    def get_size(self):
        return len(self._node_dict)

    def get_tail_node(self):
        return self._tail

    def add_to_head(self, node):
        self.sys_log("Adding " + node.get_node_name() + " to head.")
        if node.get_node_name() in self._node_dict:
            self.delete_by_node(node=node)

        if self._dummy_node.get_num_children() == 0:
            self._tail = node
        else:
            cur_head = self._dummy_node.get_children_nodes()[0]
            self._dummy_node.delete_child(child_node=cur_head)
            node.add_child(child_node=cur_head)

        self._dummy_node.add_child(child_node=node)
        self._node_dict[node.get_node_name()] = node
        self.check_valid()

    def delete_by_node(self, node):
        self.sys_log("Deleting " + node.get_node_name() + ".")
        if not node or node.get_num_parents() == 0:
            return

        node_parent = node.get_parents_nodes()[0]
        node_parent.delete_child(child_node=node)
        if node == self._tail:
            self._tail = node_parent
        else:
            node_child = node.get_children_nodes()[0]
            node_child.delete_parent(parent_node=node)
            node_parent.add_child(child_node=node_child)

        self.check_valid()

    def delete_by_node_name(self, node_name):
        node = self._node_dict.pop(node_name, None)
        if node:
            self.delete_by_node(node=node)

    def print_self(self):
        print_str = []
        start_node = self._dummy_node
        while start_node.get_num_children() > 0:
            start_node = start_node.get_children_nodes()[0]
            print_str.append(start_node.get_node_name())

        print('->'.join(print_str))

    def check_valid(self):
        start_node = self._dummy_node
        assert start_node.get_num_children() == 0 or start_node.get_num_children() == 1
        while start_node.get_num_children() > 0:
            start_node = start_node.get_children_nodes()[0]
            assert start_node.get_num_children() == 0 or start_node.get_num_children() == 1

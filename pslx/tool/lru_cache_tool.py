from pslx.core.base import Base
from pslx.core.linked_list_base import LinkedListBase
from pslx.core.node_base import OrderedNodeBase


class LRUCacheTool(Base):
    def __init__(self, max_capacity):
        super().__init__()
        self._max_capacity = max_capacity
        self._key_value_store = {}
        self._linked_list = LinkedListBase()

    def get_max_capacity(self):
        return self._max_capacity

    def get_cur_capacity(self):
        return len(self._key_value_store)

    def get(self, key):
        if key in self._key_value_store:
            node = self._key_value_store[key]
            self._linked_list.delete_by_node(node=node)
            self._linked_list.add_to_head(node=node)
            return node.get_content()
        return None

    def set(self, key, value):
        if key not in self._key_value_store and self._linked_list.get_size() >= self._max_capacity:
            tail_node = self._linked_list.get_tail_node()
            self._linked_list.delete_by_node(node=tail_node)
            self._key_value_store.pop(tail_node.get_node_name(), None)

        self._linked_list.delete_by_node(node=self._key_value_store.pop(key, None))
        if isinstance(key, str):
            new_node = OrderedNodeBase(node_name=key)
        else:
            assert isinstance(key, tuple)
            new_node = OrderedNodeBase(node_name='_'.join([str(val) for val in key]))
        new_node.set_content(content=value)
        self._key_value_store[key] = new_node
        self._linked_list.add_to_head(node=new_node)

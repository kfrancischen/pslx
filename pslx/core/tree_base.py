from collections import deque
from collections import OrderedDict
from pslx.core.base import Base
from pslx.schema.enums_pb2 import SortOrder


class TreeBase(Base):
    def __init__(self, root, max_dict_size=-1):
        super().__init__()
        self._root = root
        self._node_name_to_node_dict = OrderedDict(
            {root.get_node_name(): root}
        )
        self._max_dict_size = max_dict_size

    def get_root_node(self):
        return self._root

    def get_root_name(self):
        return self._root.get_node_name()

    def set_root_node(self, root):
        for child in self._root.get_children_nodes():
            root.add_child(child)
            self._root.delete(child)

        self._root = root
        return

    def add_node(self, parent_node, child_node, order=SortOrder.ORDER):
        self._SYS_LOGGER.info(
            "Adding parent node [" + parent_node.get_node_name() + "] to " + child_node.get_node_name() + '.')
        assert child_node.get_num_parents() == 0 and parent_node != child_node
        if parent_node != self._root:
            assert parent_node.get_num_parents() != 0

        parent_node.add_child(child_node, order=order)
        if parent_node.get_node_name() in self._node_name_to_node_dict:
            self._SYS_LOGGER.info(
                parent_node.get_node_name() + " already exists.")
        else:
            self._SYS_LOGGER.info(
                "Adding new parent node to tree [" + parent_node.get_node_name() + '].')
            self._node_name_to_node_dict[parent_node.get_node_name(
            )] = parent_node

        if child_node.get_node_name() in self._node_name_to_node_dict:
            self._SYS_LOGGER.info(
                'Node [' + child_node.get_node_name() + "] already exists.")
        else:
            self._SYS_LOGGER.info(
                "Adding new child node to tree [" + child_node.get_node_name() + '].')
            self._node_name_to_node_dict[child_node.get_node_name(
            )] = child_node

        self._clean_dict()

    def _clean_dict(self):
        while len(self._node_name_to_node_dict) > self._max_dict_size > 0:
            self._node_name_to_node_dict.popitem(last=False)

    def find_node(self, node_name):
        if node_name in self._node_name_to_node_dict:
            self._SYS_LOGGER.info(
                "Successfully found the node with name [" + node_name + '].')
            return self._node_name_to_node_dict[node_name]
        search_queue = deque()
        search_queue.append(self._root)
        while search_queue:
            search_node = search_queue.popleft()
            if search_node.get_node_name() == node_name:
                return search_node
            child_nodes = search_node.get_children_nodes()
            for child_node in child_nodes:
                search_queue.append(child_node)
        return None

    def get_num_nodes(self):
        return self.get_num_nodes_subtree(node=self._root)

    def get_num_nodes_subtree(self, node):
        if node.get_num_children() == 0:
            return 1
        else:
            result = 1
            for child_node in node.get_children_nodes():
                result += self.get_num_nodes_subtree(node=child_node)
            return result

    def bfs_search(self, max_num_node=-1):
        result, num_result_nodes = [], 0
        search_queue = deque()
        search_queue.append((self._root, 0))
        while search_queue:
            if num_result_nodes >= max_num_node > 0:
                self._SYS_LOGGER.info(
                    "Reaching maximum number of nodes in bfs search.")
                break
            search_node, dist_to_root = search_queue.popleft()
            result.append((search_node.get_node_name(), dist_to_root))
            num_result_nodes += 1

            for child_node in search_node.get_children_nodes():
                search_queue.append((child_node, dist_to_root + 1))
        return result

    def dfs_search(self, max_num_node=-1):
        result, num_result_nodes = [], 0
        search_stack = [(self._root, 0)]
        while search_stack:
            if num_result_nodes >= max_num_node > 0:
                self._SYS_LOGGER.info(
                    "Reaching maximum number of nodes in dfs search.")
                break
            search_node, dist_to_root = search_stack.pop()
            result.append((search_node.get_node_name(), dist_to_root))
            num_result_nodes += 1

            for child_node in search_node.get_children_nodes():
                search_stack.append((child_node, dist_to_root + 1))
        return result

    def _trim_tree_from_right(self, node, max_capacity=-1):
        self._SYS_LOGGER.info("Trimming with capacity " + str(max_capacity) + " from node [" + node.get_node_name()
                              + '].')
        if not node.is_children_ordered():
            self._SYS_LOGGER.info(node.get_node_name(
            ) + ' is not ordered. Be careful when you trim the tree.')

        cumulative_size = self.get_num_nodes_subtree(node=node)
        if max_capacity <= 0 or cumulative_size <= max_capacity:
            return
        if max_capacity < 1 + node.get_num_children():
            num_children_to_trim = 1 + node.get_num_children() - max_capacity
            for child_node in node.get_children_nodes()[::-1][:num_children_to_trim]:
                child_node.delete_parent(parent_node=node)
            return
        else:
            children_nodes = node.get_children_nodes()
            pivot_index = len(children_nodes) - 1
            while pivot_index >= 0:
                child_node = children_nodes[pivot_index]

                cumulative_size -= self.get_num_nodes_subtree(node=child_node)
                if cumulative_size >= max_capacity:
                    pivot_index -= 1
                else:
                    self._trim_tree_from_right(
                        node=child_node,
                        max_capacity=max_capacity - cumulative_size
                    )
                    break

            for index in range(len(children_nodes) - 1, pivot_index, -1):
                child_node = children_nodes[index]
                child_node.delete_parent(parent_node=node)
                self._node_name_to_node_dict.pop(child_node.get_node_name(), None)
            return

    def _trim_tree_from_left(self, node, max_capacity=-1):
        self._SYS_LOGGER.info("Trimming with capacity " + str(max_capacity) +
                              " from node [" + node.get_node_name() + '].')
        if not node.is_children_ordered():
            self._SYS_LOGGER.info('Node [' + node.get_node_name() +
                                  '] is not ordered. Be careful when you trim the tree.')

        cumulative_size = self.get_num_nodes_subtree(node=node)
        if max_capacity <= 0 or cumulative_size <= max_capacity:
            return
        if max_capacity < 1 + node.get_num_children():
            num_children_to_trim = 1 + node.get_num_children() - max_capacity
            for child_node in node.get_children_nodes()[:num_children_to_trim]:
                child_node.delete_parent(parent_node=node)
            return
        else:
            children_nodes = node.get_children_nodes()
            pivot_index = 0
            while pivot_index < len(children_nodes):
                child_node = children_nodes[pivot_index]

                cumulative_size -= self.get_num_nodes_subtree(node=child_node)
                if cumulative_size >= max_capacity:
                    pivot_index += 1
                else:
                    self._trim_tree_from_left(
                        node=child_node,
                        max_capacity=max_capacity - cumulative_size
                    )
                    break

            for index in range(pivot_index):
                child_node = children_nodes[index]
                child_node.delete_parent(parent_node=node)
                self._node_name_to_node_dict.pop(child_node.get_node_name(), None)
            return

    def trim_tree(self, max_capacity=-1, from_right=True):
        if from_right:
            self._trim_tree_from_right(
                node=self._root,
                max_capacity=max_capacity
            )
        else:
            self._trim_tree_from_left(
                node=self._root,
                max_capacity=max_capacity
            )
        return

    def get_leaves(self):
        leaf_node_names = []
        search_queue = deque()
        search_queue.append(self._root)
        while search_queue:
            search_node = search_queue.popleft()
            child_nodes = search_node.get_children_nodes()

            if len(child_nodes) == 0:
                leaf_node_names.append(search_node.get_node_name())

            for child_node in child_nodes:
                search_queue.append(child_node)
        return leaf_node_names

    def get_rightmost_leaf(self):
        node = self._root
        while node.get_num_children() > 0:
            node = node.get_children_nodes()[-1]
        return node.get_node_name()

    def get_leftmost_leaf(self):
        node = self._root
        while node.get_num_children() > 0:
            node = node.get_children_nodes()[0]
        return node.get_node_name()

    def get_height(self):
        node = self._root
        height = 1
        while node.get_num_children() > 0:
            node = node.get_children_nodes()[0]
            height += 1
        return height

    def print_self(self):
        search_queue = deque()
        search_queue.append(self._root)
        search_queue_count = 1
        print_str, level = '', 0
        while search_queue:
            search_node = search_queue.popleft()
            search_queue_count -= 1
            print_str += search_node.get_node_name()
            for child_node in search_node.get_children_nodes():
                search_queue.append(child_node)

            if search_queue_count > 0:
                print_str += '->'
            else:
                print('Level ' + str(level) + ': ' + print_str)
                print_str = ''
                level += 1
                search_queue_count = len(search_queue)

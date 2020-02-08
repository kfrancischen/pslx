from collections import deque
from collections import OrderedDict
from pslx.core.base import Base


class GraphBase(Base):
    def __init__(self):
        super().__init__()
        self._node_name_to_node_dict = OrderedDict()
        self._num_edge = 0

    def add_direct_edge(self, from_node, to_node):
        assert from_node != to_node
        if from_node.get_node_name() not in self._node_name_to_node_dict:
            self._node_name_to_node_dict[from_node.get_node_name()] = from_node
        if to_node.get_node_name() not in self._node_name_to_node_dict:
            self._node_name_to_node_dict[to_node.get_node_name()] = to_node
        from_node.add_child(to_node)
        self._num_edge += 1

    def add_indirect_edge(self, node_1, node_2):
        self.add_direct_edge(from_node=node_1, to_node=node_2)
        self.add_direct_edge(from_node=node_2, to_node=node_1)
        self._num_edge -= 1

    def find_node(self, node_name):
        return self._node_name_to_node_dict.get(node_name, None)

    def get_num_nodes(self):
        return len(self._node_name_to_node_dict)

    def get_num_edges(self):
        return self._num_edge

    def is_dag(self):

        def _check_dag(seed_node, visited_dict):
            visited_dict[seed_node.get_node_name()] = True
            for child in seed_node.get_children_nodes():
                if visited_dict[child.get_node_name()]:
                    return False
                else:
                    if not _check_dag(seed_node=child, visited_dict=visited_dict):
                        return False
            visited_dict[seed_node.get_node_name()] = False
            return True

        for node in self._node_name_to_node_dict.values():
            visited = {node_name: False for node_name in self._node_name_to_node_dict.keys()}
            if not _check_dag(seed_node=node, visited_dict=visited):
                return False

        return True

    def is_connected(self):
        visited = {node_name: False for node_name in self._node_name_to_node_dict.keys()}
        search_queue = deque()
        search_queue.append(list(self._node_name_to_node_dict.values())[0])
        while search_queue:
            search_node = search_queue.popleft()
            visited[search_node.get_node_name()] = True

            for child_node in search_node.get_children_nodes():
                if not visited[child_node.get_node_name()]:
                    search_queue.append(child_node)
            for parent_node in search_node.get_parents_nodes():
                if not visited[parent_node.get_node_name()]:
                    search_queue.append(parent_node)

        for val in visited.values():
            if not val:
                return False
        return True

    def get_source_nodes(self):
        result = []
        for node in self._node_name_to_node_dict.values():
            if node.get_num_parents() == 0:
                result.append(node)

        return result

    def replace_node(self, old_node, new_node):
        for parent in old_node.get_parents_nodes():
            new_node.add_parent(parent)
            old_node.delete_parent(parent)

        for child in old_node.get_children_nodes():
            new_node.add_child(child)
            old_node.delete_child(child)

        self._node_name_to_node_dict.pop(old_node.get_node_name(), None)
        self._node_name_to_node_dict[new_node.get_node_name()] = new_node

    def bfs_search(self):
        if not self.is_dag():
            self.log_print(string='Cannot do unique BFS search if the graph is cyclic.')
            return []
        if not self.is_connected():
            self.log_print(string='Cannot do unique BFS search if the graph is not fully connected.')
            return []
        source_nodes = self.get_source_nodes()
        visited = {node_name: False for node_name in self._node_name_to_node_dict.keys()}
        result = []
        search_queue = deque()
        for source_node in source_nodes:
            visited[source_node.get_node_name()] = True
            search_queue.append((source_node, 0))
        while search_queue:
            search_node, dist_to_root = search_queue.popleft()
            result.append((search_node.get_node_name(), dist_to_root))
            for child_node in search_node.get_children_nodes():
                if not visited[child_node.get_node_name()]:
                    visited[child_node.get_node_name()] = True
                    search_queue.append((child_node, dist_to_root + 1))
        return result

    def dfs_search(self):
        if not self.is_dag():
            self.log_print(string='Cannot do unique DFS search if the graph is cyclic.')
            return []
        if not self.is_connected():
            self.log_print(string='Cannot do unique DFS search if the graph is not fully connected.')
            return []
        source_nodes = self.get_source_nodes()
        visited = {node_name: False for node_name in self._node_name_to_node_dict.keys()}
        result = []
        search_stack = []
        for source_node in source_nodes:
            visited[source_node.get_node_name()] = True
            search_stack.append((source_node, 0))
        while search_stack:
            search_node, dist_to_root = search_stack.pop()

            visited[search_node.get_node_name()] = True
            result.append((search_node.get_node_name(), dist_to_root))

            for child_node in search_node.get_children_nodes():
                if not visited[child_node.get_node_name()]:
                    visited[child_node.get_node_name()] = True
                    search_stack.append((child_node, dist_to_root + 1))
        return result

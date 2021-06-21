from collections import deque
from collections import defaultdict
from collections import OrderedDict
from pslx.core.base import Base


class GraphBase(Base):
    def __init__(self):
        super().__init__()
        self._node_name_to_node_dict = OrderedDict()
        self._num_edge = 0

    def add_direct_edge(self, from_node, to_node):
        assert from_node != to_node
        self._SYS_LOGGER.info("Adding direct edge from " + from_node.get_node_name() + " to " +
                              to_node.get_node_name() + '.')
        if from_node.get_node_name() not in self._node_name_to_node_dict:
            self._node_name_to_node_dict[from_node.get_node_name()] = from_node
        if to_node.get_node_name() not in self._node_name_to_node_dict:
            self._node_name_to_node_dict[to_node.get_node_name()] = to_node
        from_node.add_child(to_node)
        self._num_edge += 1

    def add_indirect_edge(self, node_1, node_2):
        self._SYS_LOGGER.info("Adding indirect edge between " + node_1.get_node_name() + " and " +
                              node_2.get_node_name() + '.')
        self.add_direct_edge(from_node=node_1, to_node=node_2)
        self.add_direct_edge(from_node=node_2, to_node=node_1)
        self._num_edge -= 1

    def find_node(self, node_name):
        return self._node_name_to_node_dict.get(node_name, None)

    def get_num_nodes(self):
        return len(self._node_name_to_node_dict)

    def get_nodes(self):
        return list(self._node_name_to_node_dict.values())

    def get_node_names(self):
        return list(self._node_name_to_node_dict.keys())

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
        self._SYS_LOGGER.info("Replacing old node: " + old_node.get_node_name() + " with new node: "
                              + new_node.get_node_name() + '.')
        for parent in old_node.get_parents_nodes():
            new_node.add_parent(parent)
            old_node.delete_parent(parent)

        for child in old_node.get_children_nodes():
            new_node.add_child(child)
            old_node.delete_child(child)

        self._node_name_to_node_dict.pop(old_node.get_node_name(), None)
        self._node_name_to_node_dict[new_node.get_node_name()] = new_node

    def topological_sort(self):

        def _traverse(cur_node, cur_visited, cur_stack):
            visited[cur_node.get_node_name()] = True
            for next_node in cur_node.get_children_nodes():
                if not visited[next_node.get_node_name()]:
                    _traverse(next_node, cur_visited, cur_stack)
            cur_stack.append(cur_node.get_node_name())

        if not self.is_dag():
            self._SYS_LOGGER.info(string='Cannot do topological sort if the graph is cyclic.')
            return []
        if not self.is_connected():
            self._SYS_LOGGER.info(string='Cannot do topological sort if the graph is not fully connected.')
            return []

        traversed_nodes = []
        levels = {node_name: float('-inf') for node_name in self._node_name_to_node_dict.keys()}

        visited = {node_name: False for node_name in self._node_name_to_node_dict.keys()}
        for node in self._node_name_to_node_dict.values():
            if not visited[node.get_node_name()]:
                _traverse(node, visited, traversed_nodes)

        source_nodes = self.get_source_nodes()
        for node in source_nodes:
            levels[node.get_node_name()] = 0

        while traversed_nodes:
            node_name = traversed_nodes.pop()
            if levels[node_name] != float('inf'):
                for child_node in self._node_name_to_node_dict[node_name].get_children_nodes():

                    if levels[child_node.get_node_name()] < levels[node_name] + 1:
                        levels[child_node.get_node_name()] = levels[node_name] + 1

        return sorted([(key, val) for key, val in levels.items()], key=lambda pair: pair[1])

    def get_node_levels(self):
        sort_result = self.topological_sort()
        level_map = defaultdict(list)
        for result in sort_result:
            level_map[result[1]].append(result[0])

        for level in level_map:
            level_map[level].sort(
                key=lambda name: self._node_name_to_node_dict[name].get_num_children())
        return level_map

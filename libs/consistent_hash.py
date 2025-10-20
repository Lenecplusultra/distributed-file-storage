import hashlib
import bisect
from typing import List


def _h(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    """
    - Nodes are strings (e.g., "<node_id>|<host:port>")
    - Uses virtual nodes for smoother key distribution
    - get_n(key, n) walks clockwise, returning n distinct physical nodes
    """

    def __init__(self, replicas_per_node: int = 128):
        self.replicas_per_node = replicas_per_node
        self.keys: List[int] = []
        self.values: List[str] = []
        self.phys_set = set()


    def add_node(self, node_id: str):
        if node_id in self.phys_set:
            return
        self.phys_set.add(node_id)
        for i in range(self.replicas_per_node):
            k = _h(f"{node_id}#{i}")
            idx = bisect.bisect_left(self.keys, k)
            self.keys.insert(idx, k)
            self.values.insert(idx, node_id)


    def remove_node(self, node_id: str):
        if node_id not in self.phys_set:
            return
        self.phys_set.remove(node_id)
        # Rebuild simple & safe for small/medium ring sizes
        nodes = list(self.phys_set)
        self.keys.clear(); self.values.clear()
        for n in nodes:
            self.add_node(n)


    def get_n(self, key: str, n: int) -> List[str]:
        if not self.keys or n <= 0:
            return []
        start = bisect.bisect_left(self.keys, _h(key))
        out, seen = [], set()
        i = start
        total = len(self.keys)
        while len(out) < min(n, len(self.phys_set)) and total > 0:
            node = self.values[i % total]
            if node not in seen:
                seen.add(node)
                out.append(node)
            i += 1
        return out
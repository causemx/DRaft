
from typing import Set, Tuple, List


class CircularNodeList:
    """Circular linked list for symmetric node distribution around leader"""
    
    def __init__(self, nodes: List[Tuple[str, int]], exclude_failed: Set[str] = None):
        """Initialize with sorted list of (node_id, port) tuples, excluding failed nodes"""
        if exclude_failed is None:
            exclude_failed = set()
            
        # Filter out failed nodes and sort for consistent ordering
        active_nodes = [(node_id, port) for node_id, port in nodes if node_id not in exclude_failed]
        self.nodes = sorted(active_nodes, key=lambda x: x[0])
        self.size = len(self.nodes)
        
        # Log active nodes
        if exclude_failed:
            print(f"CircularNodeList excluding failed nodes: {exclude_failed}")
        print(f"Active nodes in formation: {[n[0] for n in self.nodes]}")
    
    def get_node_index(self, node_id: str) -> int:
        """Get index of node in the circular list"""
        for i, (nid, _) in enumerate(self.nodes):
            if nid == node_id:
                return i
        return -1
    
    def get_symmetric_distribution(self, leader_node_id: str) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
        """
        Get symmetric distribution of nodes around leader
        Returns: (left_side_nodes, right_side_nodes)
        """
        leader_idx = self.get_node_index(leader_node_id)
        if leader_idx == -1:
            raise ValueError(f"Leader node {leader_node_id} not found")
        
        if self.size <= 1:
            return [], []
        
        # Calculate how many nodes go on each side
        remaining_nodes = self.size - 1  # Exclude leader
        left_count = remaining_nodes // 2
        right_count = remaining_nodes - left_count
        
        left_side = []
        right_side = []
        
        # Fill left side (going backwards in circular fashion)
        for i in range(1, left_count + 1):
            idx = (leader_idx - i) % self.size
            left_side.append(self.nodes[idx])
        
        # Fill right side (going forwards in circular fashion)
        for i in range(1, right_count + 1):
            idx = (leader_idx + i) % self.size
            right_side.append(self.nodes[idx])
        
        return left_side, right_side
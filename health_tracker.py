#!/usr/bin/env python3
"""
Peer Health Tracking Module
Extracted from raft_node.py for better organization
"""

import time
from typing import Dict, List, Set
from node_metadata import NodeMetadata


class PeerHealthTracker:
    """Tracks the health status of peer nodes using NodeMetadata"""
    
    def __init__(self, node_metadata: NodeMetadata, heartbeat_timeout: float = 10.0):
        self.node_metadata = node_metadata
        self.heartbeat_timeout = heartbeat_timeout
        self.peer_health: Dict[str, float] = {}  # "host:port" -> last_seen_timestamp
        self.failed_peers: Set[str] = set()  # Set of "host:port" strings
        
    def _peer_key(self, peer_metadata: NodeMetadata) -> str:
        """Generate a unique key for peer"""
        return f"{peer_metadata.get_host()}:{peer_metadata.get_port()}"
        
    def update_peer_heartbeat(self, peer_metadata: NodeMetadata):
        """Update the last seen time for a peer"""
        peer_key = self._peer_key(peer_metadata)
        self.peer_health[peer_key] = time.time()
        # Remove from failed peers if it was there
        self.failed_peers.discard(peer_key)
        
    def mark_peer_failed(self, peer_metadata: NodeMetadata):
        """Manually mark a peer as failed"""
        peer_key = self._peer_key(peer_metadata)
        self_key = self._peer_key(self.node_metadata)
        if peer_key != self_key:  # Don't mark self as failed
            self.failed_peers.add(peer_key)
        
    def get_active_peers(self, all_peers: List[NodeMetadata]) -> List[NodeMetadata]:
        """Get list of currently active (non-failed) peers"""
        current_time = time.time()
        active_peers = []
        
        for peer in all_peers:
            peer_key = self._peer_key(peer)
            self_key = self._peer_key(self.node_metadata)
            
            # Always include self
            if peer_key == self_key:
                active_peers.append(peer)
                continue
                
            # Check if peer is manually marked as failed
            if peer_key in self.failed_peers:
                continue
                
            # Check if we've seen a heartbeat recently
            last_seen = self.peer_health.get(peer_key, 0)
            if current_time - last_seen > self.heartbeat_timeout:
                # Mark as failed due to timeout
                self.failed_peers.add(peer_key)
                continue
                
            active_peers.append(peer)
        
        return active_peers
    
    def get_failed_peers(self) -> Set[str]:
        """Get set of failed peer keys"""
        return self.failed_peers.copy()
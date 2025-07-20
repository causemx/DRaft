#!/usr/bin/env python3
"""
Integrated Raft-Drone Node - Combines Raft consensus with drone control
Each Raft node controls a specific drone, enabling distributed consensus for swarm operations
Enhanced with leader-centered formation system and NodeMetadata support
"""

import asyncio
import json
import random
import time
from dataclasses import asdict
from core.control import DroneController
from util.calculate import Calculator
from util.data_struct import CircularNodeList
from util.logger_helper import LoggerFactory
from confs.config_manager import get_config_manager
from node_metadata import NodeMetadata
from net.network_comm import NetworkComm
from health_tracker import PeerHealthTracker
from typing import List, Dict, Optional, Tuple
from message import (
    Message, 
    LogEntry, 
    NodeState, 
    MessageType, 
    VoteRequest, 
    VoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
    SwarmCommand,
    SwarmCommandType,
)

# Global configuration manager
config_manager = get_config_manager()
logger = LoggerFactory.get_logger(name="raft_node.py")


class RaftDroneNode:
    def __init__(self, node_metadata: NodeMetadata, drone_index: int, peers: List[NodeMetadata]):
        self.node_metadata = node_metadata
        self.node_id = f"drone{drone_index + 1}"  # Keep node_id for compatibility
        self.peers = peers  # List of NodeMetadata objects
        self.drone_index = drone_index
        
        # Create SimpleNetworkComm instead of manual server
        self.network_comm = NetworkComm(
            nodes=peers,
            port=node_metadata.get_port(),
            bind_host="0.0.0.0", # Bind to all devices
            send_timeout=5.0
        )
        
        # Set Raft's connection handler
        self.network_comm.set_connection_handler(self.handle_client_connection)
        
        # Get drone connection string from config
        drone_connection = config_manager.get_drone_connection(drone_index)
        self.drone_controller = DroneController(drone_connection)
        self.drone_connected = False
        
        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER
        
        # Leader state - now using host:port keys
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()
        self.heartbeat_interval = 0.15  # 150ms
        
        # Network
        self.server: Optional[asyncio.Server] = None
        
        # Tasks
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.election_task: Optional[asyncio.Task] = None
        self.drone_monitor_task: Optional[asyncio.Task] = None
        
        # Peer Health Tracker
        self.health_tracker = PeerHealthTracker(node_metadata)
    
        # formation tracking
        self.last_formation_config = None
        self.last_formation_time = 0
        
        # Swarm coordination
        self.swarm_state = {
            'formation_type': None,
            'formation_role': None,
            'target_gps': None,
            'leader_id': None,
            'leader_gps': None
        }
    
    def _peer_key(self, peer_metadata: NodeMetadata) -> str:
        """Generate a unique key for peer"""
        return f"{peer_metadata.get_host()}:{peer_metadata.get_port()}"
        
    def _random_election_timeout(self) -> float:
        """Random election timeout between 1.5-3 seconds"""
        return random.uniform(1.5, 3.0)
    
    def get_active_peers(self) -> List[NodeMetadata]:
        """Get currently active (non-failed) peers"""
        return self.health_tracker.get_active_peers(self.peers)

    def handle_peer_communication_failure(self, peer_metadata: NodeMetadata):
        """Handle communication failure with a peer"""
        peer_key = self._peer_key(peer_metadata)
        # logger.warning(f"Communication failed with peer {peer_key}")
        self.health_tracker.mark_peer_failed(peer_metadata)
        
        # If we're the leader and have an active formation, recalculate
        if (self.state == NodeState.LEADER and 
            self.last_formation_config and 
            time.time() - self.last_formation_time > 5.0):  # TODO Don't recalculate too frequently
            logger.info("Triggering formation recalculation due to peer failure")
            asyncio.create_task(self.recalculate_formation())

    def handle_successful_peer_communication(self, peer_metadata: NodeMetadata):
        """Handle successful communication with a peer"""
        self.health_tracker.update_peer_heartbeat(peer_metadata)
    
    async def recalculate_formation(self):
        """Recalculate formation with current active nodes"""
        if not self.last_formation_config:
            return
            
        failed_peers = self.health_tracker.get_failed_peers()
        active_peers = self.get_active_peers()
        
        logger.info(f"Recalculating formation: {len(active_peers)} active, {len(failed_peers)} failed")
        
        # Re-execute the last formation command with current active nodes
        result = await self.execute_set_formation(self.last_formation_config)
        
        if result.get('success'):
            logger.info("Formation recalculated successfully")
            self.last_formation_time = time.time()
        else:
            logger.error(f"Formation recalculation failed: {result.get('error')}")
    
    async def start(self):
        """Start the Raft node and drone connection"""
        logger.info(f"Starting Raft-Drone node {self.node_id} on {self.node_metadata.get_host()}:{self.node_metadata.get_port()}")
        
        # Connect to drone
        await self.connect_drone()
        
        await self.network_comm.run()
        
        # Start election timer
        self.election_task = asyncio.create_task(self.election_timer())
        
        # Start drone monitoring
        self.drone_monitor_task = asyncio.create_task(self.drone_monitor_loop())
        
        logger.info(f"Node {self.node_id} is listening on {self.node_metadata.get_host()}:{self.node_metadata.get_port()}")
    
    async def connect_drone(self):
        """Connect to the drone"""
        try:
            # Run drone connection in a thread to avoid blocking
            loop = asyncio.get_event_loop()
            connected = await loop.run_in_executor(
                None, self.drone_controller.connect
            )
            if connected:
                self.drone_connected = True
                logger.info(f"Connected to drone {self.drone_index}")
            else:
                logger.error(f"Failed to connect to drone {self.drone_index}")
        except Exception as e:
            logger.error(f"Error connecting to drone: {e}")
    
    async def drone_monitor_loop(self):
        """Monitor drone status and apply committed commands - WITH HEALTH MONITORING"""
        while True:
            try:
                # Apply any committed but not applied log entries
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    if self.last_applied <= len(self.log):
                        entry = self.log[self.last_applied - 1]
                        await self.apply_command(entry.command)
                
                # If we're leader, check for failed peers and recalculate formation if needed
                if self.state == NodeState.LEADER:
                    failed_peers = self.health_tracker.get_failed_peers()
                    if (failed_peers and 
                        self.last_formation_config and 
                        time.time() - self.last_formation_time > 10.0):  # Check every 10 seconds
                        
                        logger.info(f"Detected {len(failed_peers)} failed peers, checking if formation recalculation needed")
                        await self.recalculate_formation()
                
                await asyncio.sleep(0.1)  # Check every 100ms
            except Exception as e:
                logger.error(f"Error in drone monitor loop: {e}")
                await asyncio.sleep(1)
    
    async def apply_command(self, command: str):
        """
        Apply a committed command to the drone
        SIMPLIFIED VERSION - Only essential commands for basic swarm operation
        
        Args:
            command: Command string in format "COMMAND_TYPE:JSON_DATA"
        """
        try:
            if not self.drone_connected:
                logger.warning(f"Cannot apply command '{command}' - drone not connected")
                return
            
            # Parse command format: "COMMAND_TYPE:JSON_DATA"
            parts = command.split(':', 1)
            if len(parts) != 2:
                logger.warning(f"Invalid command format: {command}")
                return
            
            cmd_type, cmd_data = parts
            
            # Parse JSON data
            try:
                data = json.loads(cmd_data) if cmd_data.strip() else {}
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in command '{command}': {e}")
                return
            
            # Get asyncio loop for blocking operations
            loop = asyncio.get_event_loop()
            
            logger.info(f"Applying command: {cmd_type}")
            
            # ========== BASIC DRONE COMMANDS ==========
            
            if cmd_type in ["ARM", "ARM_ALL"]:
                result = await loop.run_in_executor(None, self.drone_controller.arm)
                logger.info(f"ARM command: {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type in ["DISARM", "DISARM_ALL"]:
                result = await loop.run_in_executor(None, self.drone_controller.disarm)
                logger.info(f"DISARM command: {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type in ["TAKEOFF", "TAKEOFF_ALL"]:
                altitude = data.get('altitude', 10.0)
                result = await loop.run_in_executor(None, self.drone_controller.takeoff, altitude)
                logger.info(f"TAKEOFF to {altitude}m: {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type in ["LAND", "LAND_ALL"]:
                result = await loop.run_in_executor(None, self.drone_controller.land)
                logger.info(f"LAND command: {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type == "SET_MODE":
                mode = data.get('mode', 'GUIDED')
                result = await loop.run_in_executor(None, self.drone_controller.set_flight_mode, mode)
                logger.info(f"SET_MODE to {mode}: {'SUCCESS' if result else 'FAILED'}")
            
            # ========== FORMATION COMMANDS ==========
            
            elif cmd_type == "SET_FORMATION_GPS":
                """Handle GPS-based formation assignment with dynamic peer management"""
                leader_id = data.get('leader_id')
                leader_gps = data.get('leader_gps', {})
                assignments = data.get('assignments', {})
                active_nodes = data.get('active_nodes', [])
                excluded_nodes = data.get('excluded_failed_nodes', [])
                formation_type = data.get('formation_type', 'unknown')
                
                # Log formation info
                logger.info(f"Formation update: {formation_type}, Leader: {leader_id}")
                logger.info(f"Active nodes: {active_nodes}")
                if excluded_nodes:
                    logger.warning(f"Excluded failed nodes: {excluded_nodes}")
                
                # Check if this node is excluded due to failure
                if self.node_id in excluded_nodes:
                    logger.warning(f"Node {self.node_id} excluded from formation")
                    self.swarm_state['formation_role'] = 'excluded'
                    self.swarm_state['target_gps'] = None
                    return
                
                # Get this node's assignment
                my_assignment = assignments.get(self.node_id)
                if my_assignment:
                    if my_assignment.get('is_leader'):
                        logger.info(f"Leader {self.node_id} stays at center: {leader_gps}")
                        self.swarm_state['formation_role'] = 'leader_center'
                    else:
                        target_lat = my_assignment['lat']
                        target_lon = my_assignment['lon']
                        target_alt = my_assignment['alt']
                        rel_pos = my_assignment['relative_pos']
                        
                        logger.info(f"Follower {self.node_id} target: {target_lat:.6f}, {target_lon:.6f}")
                        logger.info(f"Relative: {rel_pos[0]:+.1f}m East, {rel_pos[1]:+.1f}m North")
                        self.swarm_state['formation_role'] = 'follower'
                    
                    # Store formation data
                    self.swarm_state['formation_type'] = formation_type
                    self.swarm_state['target_gps'] = my_assignment
                    self.swarm_state['leader_id'] = leader_id
                    self.swarm_state['leader_gps'] = leader_gps
                    self.swarm_state['excluded_nodes'] = excluded_nodes
                else:
                    logger.warning(f"No formation assignment for {self.node_id}")
            
            elif cmd_type in ["EXECUTE_FORMATION", "EXECUTE_GPS_FORMATION"]:
                """Execute movement to formation position"""
                target_gps = self.swarm_state.get('target_gps')
                formation_role = self.swarm_state.get('formation_role')
                
                if not target_gps:
                    logger.warning(f"No formation target for {self.node_id}")
                    return
                
                if formation_role == 'excluded':
                    logger.warning(f"Node {self.node_id} excluded from formation movement")
                    return
                
                if target_gps.get('is_leader'):
                    logger.info(f"Leader {self.node_id} stays at center - no movement")
                    return
                
                # Follower movement
                target_lat = target_gps['lat']
                target_lon = target_gps['lon'] 
                target_alt = target_gps['alt']
                
                # Get current position
                current_status = self.get_drone_status()
                current_pos = current_status.get('position')
                
                if current_pos:
                    current_lat, current_lon = current_pos
                    
                    # Calculate distance to target
                    from util.calculate import Calculator
                    distance, bearing = Calculator.calculate_distance_bearing(
                        current_lat, current_lon, target_lat, target_lon
                    )
                    
                    logger.info(f"Moving to formation: {distance:.1f}m at {bearing:.1f}°")
                    
                    # Execute GPS movement
                    result = await loop.run_in_executor(
                        None, self.drone_controller.fly_to_target, target_lat, target_lon, target_alt
                    )
                    
                    logger.info(f"Formation movement: {'SUCCESS' if result else 'FAILED'}")
                else:
                    logger.error("Current GPS position not available")
            
            else:
                logger.warning(f"Unknown command type: {cmd_type}")
                logger.info("Available commands: ARM, DISARM, TAKEOFF, LAND, SET_MODE, FLY_TO, FLY_TO_GPS, SET_FORMATION_GPS, EXECUTE_FORMATION, EMERGENCY_STOP, EMERGENCY_LAND")
            
        except Exception as e:
            logger.error(f"Error applying command '{command}': {e}")

    
    def calculate_formation_positions_leader_centered(self, formation_type: str, interval: float, angle: float = 0.0) -> List[Tuple[float, float]]:
        """
        Calculate formation positions using circular linked list for symmetric distribution
        ENHANCED WITH DYNAMIC PEER MANAGEMENT - EXCLUDES FAILED NODES
        
        Args:
            formation_type: 'line' or 'wedge'
            interval: Distance between adjacent drones in meters
            angle: Rotation angle of the entire formation in degrees
        
        Returns:
            List of (x, y) relative positions for each ACTIVE drone
            Leader position is always (0, 0)
        """
        import math
        
        # Get only active peers (excluding failed ones)
        active_peers = self.get_active_peers()
        failed_peers = self.health_tracker.get_failed_peers()
        
        # Log current peer status
        if failed_peers:
            logger.warning(f"Formation calculation excluding failed peers: {failed_peers}")
        
        logger.info(f"Formation calculation with {len(active_peers)} active nodes: {[self._peer_key(p) for p in active_peers]}")
        
        if len(active_peers) <= 1:
            logger.warning("Not enough active peers for formation - only leader available")
            return [(0.0, 0.0)]  # Only leader position
        
        # Create circular node list with only active peers, excluding failed ones
        # Convert NodeMetadata to (node_id, port) format for CircularNodeList
        active_peers_tuples = [(f"drone{i+1}", peer.get_port()) for i, peer in enumerate(active_peers)]
        failed_peers_ids = set()  # Convert failed peer keys to node IDs if needed
        
        circular_list = CircularNodeList(active_peers_tuples, failed_peers_ids)
        
        # Get symmetric distribution around leader
        left_side, right_side = circular_list.get_symmetric_distribution(self.node_id)
        
        # Log the circular distribution
        logger.info(f"Circular distribution for leader {self.node_id}:")
        logger.info(f"  Left side: {[n[0] for n in left_side]}")
        logger.info(f"  Right side: {[n[0] for n in right_side]}")
        
        # Create formation order: left nodes (reversed) + leader + right nodes
        formation_order = []
        
        # Add left side nodes (in reverse order for proper left-to-right positioning)
        for node in reversed(left_side):
            formation_order.append(node)
        
        # Add leader
        leader_idx = circular_list.get_node_index(self.node_id)
        if leader_idx == -1:
            logger.error(f"Leader {self.node_id} not found in active peers!")
            return [(0.0, 0.0)]
        
        formation_order.append(circular_list.nodes[leader_idx])
        
        # Add right side nodes
        for node in right_side:
            formation_order.append(node)
        
        # Calculate positions based on formation type
        positions_dict = {}
        formation_type_lower = formation_type.lower()
        
        logger.info(f"Calculating {formation_type.upper()} formation with {interval}m interval, {angle}° rotation")
        
        if formation_type_lower == 'line':
            # LINE FORMATION: Symmetric line with leader at center
            leader_pos_in_order = len(left_side)  # Leader is after all left nodes
            
            logger.info(f"LINE formation - Leader at position {leader_pos_in_order} in order of {len(formation_order)} nodes")
            
            for i, (node_id, _) in enumerate(formation_order):
                if i == leader_pos_in_order:
                    # Leader stays at center (0, 0)
                    x, y = 0.0, 0.0
                    role = "CENTER"
                else:
                    # Calculate position relative to leader center
                    position_offset = i - leader_pos_in_order
                    
                    # Calculate position along the line
                    x_offset = position_offset * interval
                    y_offset = 0.0
                    
                    # Apply rotation transformation
                    angle_rad = math.radians(angle)
                    x = x_offset * math.cos(angle_rad) - y_offset * math.sin(angle_rad)
                    y = x_offset * math.sin(angle_rad) + y_offset * math.cos(angle_rad)
                    
                    role = f"LEFT-{leader_pos_in_order-i}" if i < leader_pos_in_order else f"RIGHT-{i-leader_pos_in_order}"
                
                positions_dict[node_id] = (x, y)
                logger.info(f"  {node_id} ({role}): position ({x:.1f}, {y:.1f})")
        
        elif formation_type_lower == 'wedge':
            # WEDGE FORMATION: V-shape with leader at the tip (center front)
            leader_pos_in_order = len(left_side)  # Leader is after all left nodes
            
            logger.info(f"WEDGE formation - Leader at tip position {leader_pos_in_order} in order of {len(formation_order)} nodes")
            
            for i, (node_id, _) in enumerate(formation_order):
                if i == leader_pos_in_order:
                    # Leader stays at center front (0, 0) - tip of the wedge
                    x, y = 0.0, 0.0
                    role = "TIP"
                elif i < leader_pos_in_order:
                    # Left wing of the wedge
                    level = leader_pos_in_order - i  # 1, 2, 3, ...
                    x_offset = -level * interval  # Negative x (left side)
                    y_offset = -level * interval  # Negative y (behind leader)
                    
                    # Apply rotation transformation
                    angle_rad = math.radians(angle)
                    x = x_offset * math.cos(angle_rad) - y_offset * math.sin(angle_rad)
                    y = x_offset * math.sin(angle_rad) + y_offset * math.cos(angle_rad)
                    
                    role = f"LEFT-{level}"
                    
                else:
                    # Right wing of the wedge
                    level = i - leader_pos_in_order  # 1, 2, 3, ...
                    x_offset = level * interval   # Positive x (right side)
                    y_offset = -level * interval  # Negative y (behind leader)
                    
                    # Apply rotation transformation
                    angle_rad = math.radians(angle)
                    x = x_offset * math.cos(angle_rad) - y_offset * math.sin(angle_rad)
                    y = x_offset * math.sin(angle_rad) + y_offset * math.cos(angle_rad)
                    
                    role = f"RIGHT-{level}"
                
                positions_dict[node_id] = (x, y)
                logger.info(f"  {node_id} ({role}): position ({x:.1f}, {y:.1f})")
        
        else:
            logger.warning(f"Unsupported formation type: {formation_type}. Using line formation.")
            return self.calculate_formation_positions_leader_centered('line', interval, angle)
        
        # Return positions in the same order as active_peers (original peer order, but only active ones)
        final_positions = []
        for peer in active_peers:
            # Map peer to node_id for lookup
            peer_node_id = f"drone{self.peers.index(peer) + 1}" if peer in self.peers else self.node_id
            if peer_node_id in positions_dict:
                final_positions.append(positions_dict[peer_node_id])
            else:
                # This shouldn't happen, but handle gracefully
                logger.warning(f"No position calculated for active node {self._peer_key(peer)}")
                final_positions.append((0.0, 0.0))
        
        # Log summary
        logger.info("Formation calculation complete:")
        logger.info(f"  Total active nodes: {len(active_peers)}")
        logger.info(f"  Excluded failed nodes: {len(failed_peers)}")
        logger.info(f"  Positions calculated: {len(final_positions)}")
        
        return final_positions

    async def handle_client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming client connections"""
        client_addr = writer.get_extra_info('peername')
        logger.debug(f"New connection from {client_addr}")
        
        try:
            while True:
                message = await self.network_comm._receive_message(reader)
                if not message:
                    break
                
                await self.process_message(message, reader, writer)
        except Exception as e:
            logger.debug(f"Connection error with {client_addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    def create_message(self, msg_type: str, data: Dict) -> Message:
        """Helper method to create messages with proper sender"""
        return Message(
            msg_type=msg_type,
            data=data,
            sender=self.node_metadata
        )
    
    async def process_message(self, message: Message, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Process incoming messages"""
        try:
            if message.msg_type == MessageType.VOTE_REQUEST.value:
                response = await self.handle_vote_request(VoteRequest(**message.data))
                response_msg = self.create_message(
                    MessageType.VOTE_RESPONSE.value,
                    asdict(response)
                )
                await self.network_comm._send_message(writer, response_msg)
            
            elif message.msg_type == MessageType.APPEND_ENTRIES.value:
                # Convert serialized entries back to LogEntry objects
                entries_data = message.data.get('entries', [])
                entries = []
                for entry_data in entries_data:
                    if isinstance(entry_data, dict):
                        entries.append(LogEntry(**entry_data))
                    else:
                        # Already a LogEntry object, convert to dict first
                        if hasattr(entry_data, '__dict__'):
                            entries.append(LogEntry(**asdict(entry_data)))
                        else:
                            logger.error(f"Invalid entry data: {entry_data}")
                            continue
                
                # Create a copy of message data with converted entries
                append_data = message.data.copy()
                append_data['entries'] = entries
                
                append_request = AppendEntriesRequest(**append_data)
                response = await self.handle_append_entries(append_request)
                response_msg = self.create_message(
                    MessageType.APPEND_RESPONSE.value,
                    asdict(response)
                )
                await self.network_comm._send_message(writer, response_msg)
            
            elif message.msg_type == MessageType.CLIENT_REQUEST.value:
                response = await self.handle_client_request(message.data)
                response_msg = self.create_message(
                    MessageType.CLIENT_RESPONSE.value,
                    response
                )
                await self.network_comm._send_message(writer, response_msg)
            
            elif message.msg_type == MessageType.SWARM_COMMAND.value:
                response = await self.handle_swarm_command(message.data)
                response_msg = self.create_message(
                    MessageType.SWARM_RESPONSE.value,
                    response
                )
                await self.network_comm._send_message(writer, response_msg)
            
            elif message.msg_type == MessageType.STATUS_REQUEST.value:
                response = await self.handle_status_request()
                response_msg = self.create_message(
                    MessageType.STATUS_RESPONSE.value,
                    response
                )
                await self.network_comm._send_message(writer, response_msg)
        
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    async def handle_swarm_command(self, command_data: Dict) -> Dict:
        """Handle swarm coordination commands"""
        if self.state != NodeState.LEADER:
            return {'error': 'Not leader', 'leader_id': None}
        
        try:
            swarm_cmd = SwarmCommand(**command_data)

            if swarm_cmd.command_type == SwarmCommandType.CONNECT_ALL.value:
                return await self.execute_connect_all()
            
            elif swarm_cmd.command_type == SwarmCommandType.ARM_ALL.value:
                return await self.execute_arm_all()
            
            elif swarm_cmd.command_type == SwarmCommandType.DISARM_ALL.value:
                return await self.execute_disarm_all()
            
            elif swarm_cmd.command_type == SwarmCommandType.TAKEOFF_ALL.value:
                altitude = swarm_cmd.parameters.get('altitude', 10.0)
                return await self.execute_takeoff_all(altitude)
            
            elif swarm_cmd.command_type == SwarmCommandType.LAND_ALL.value:
                return await self.execute_land_all()
            
            elif swarm_cmd.command_type == SwarmCommandType.SET_FORMATION.value:
                return await self.execute_set_formation(swarm_cmd.parameters)
            
            elif swarm_cmd.command_type == SwarmCommandType.EXECUTE_FORMATION.value:
                return await self.execute_formation_movement()
            
            elif swarm_cmd.command_type == SwarmCommandType.GET_SWARM_STATUS.value:
                return await self.get_swarm_status()
            
            elif swarm_cmd.command_type == SwarmCommandType.INDIVIDUAL_COMMAND.value:
                target_node = swarm_cmd.parameters.get('target_node')
                command = swarm_cmd.parameters.get('command')
                return await self.execute_individual_command(target_node, command)
            
            else:
                return {'error': f'Unknown swarm command: {swarm_cmd.command_type}'}
        
        except Exception as e:
            logger.error(f"Error handling swarm command: {e}")
            return {'error': str(e)}
    
    async def execute_connect_all(self) -> Dict:
        """Execute connect command on all nodes"""
        # This is handled locally, no need for consensus
        if not self.drone_connected:
            await self.connect_drone()
        
        return {'success': True, 'message': 'Connect command processed'}
    
    async def execute_arm_all(self) -> Dict:
        """Execute ARM command on all drones through consensus"""
        command = "ARM_ALL:{}"
        success = await self.replicate_command(command)
        return {'success': success, 'command': 'ARM_ALL'}
    
    async def execute_disarm_all(self) -> Dict:
        """Execute DISARM command on all drones through consensus"""
        command = "DISARM_ALL:{}"
        success = await self.replicate_command(command)
        return {'success': success, 'command': 'DISARM_ALL'}
    
    async def execute_takeoff_all(self, altitude: float) -> Dict:
        """Execute TAKEOFF command on all drones through consensus"""
        command = f"TAKEOFF_ALL:{json.dumps({'altitude': altitude})}"
        success = await self.replicate_command(command)
        return {'success': success, 'command': 'TAKEOFF_ALL', 'altitude': altitude}
    
    async def execute_land_all(self) -> Dict:
        """Execute LAND command on all drones through consensus"""
        command = "LAND_ALL:{}"
        success = await self.replicate_command(command)
        return {'success': success, 'command': 'LAND_ALL'}
    
    async def execute_set_formation(self, parameters: Dict) -> Dict:
        """
        Set formation for the swarm using circular distribution - LEADER-CENTERED VERSION
        NOW WITH DYNAMIC PEER MANAGEMENT
        """
        formation_type = parameters.get('formation_type')
        interval = parameters.get('interval', 10.0)
        angle = parameters.get('angle', 0.0)
        execute_immediately = parameters.get('execute', False)
        
        # Store formation config for potential recalculation
        self.last_formation_config = parameters.copy()
        self.last_formation_time = time.time()
        
        # Get only active peers
        active_peers = self.get_active_peers()
        failed_peers = self.health_tracker.get_failed_peers()
        
        if failed_peers:
            logger.warning(f"Formation excludes failed nodes: {failed_peers}")
        
        if len(active_peers) <= 1:
            return {
                'error': 'Not enough active nodes for formation',
                'active_nodes': len(active_peers),
                'failed_nodes': list(failed_peers)
            }
        
        # Get leader's current GPS position
        leader_status = self.get_drone_status()
        leader_position = leader_status.get('position')
        
        if not leader_position:
            return {'error': 'Leader GPS position not available', 'leader_id': self.node_id}
        
        leader_lat, leader_lon = leader_position
        leader_alt = leader_status.get('altitude', 0)
        
        logger.info(f"Setting formation with {len(active_peers)} active nodes:")
        logger.info(f"Leader {self.node_id} GPS: {leader_lat:.6f}, {leader_lon:.6f}, {leader_alt:.1f}m")
        logger.info(f"Formation: {formation_type.upper()} with {interval}m spacing")
        if failed_peers:
            logger.info(f"Excluded failed nodes: {failed_peers}")
        
        # Calculate formation positions using only active nodes
        relative_positions = self.calculate_formation_positions_leader_centered(
            formation_type, interval, angle
        )
        
        # Convert relative positions to GPS coordinates for active nodes only
        formation_assignment = {}
        
        for i, peer in enumerate(active_peers):
            peer_key = self._peer_key(peer)
            if peer == self.node_metadata:
                # Leader stays at current position
                formation_assignment[self.node_id] = {
                    'type': 'leader_center',
                    'lat': leader_lat,
                    'lon': leader_lon,
                    'alt': leader_alt,
                    'relative_pos': (0.0, 0.0),
                    'is_leader': True
                }
            elif i < len(relative_positions):
                # Calculate GPS coordinates for active follower drones
                rel_x, rel_y = relative_positions[i]
                target_lat, target_lon = Calculator.relative_to_gps(leader_lat, leader_lon, rel_x, rel_y)
                
                # Map peer to node_id
                peer_node_id = f"drone{self.peers.index(peer) + 1}" if peer in self.peers else f"node_{peer.get_port()}"
                
                formation_assignment[peer_node_id] = {
                    'type': 'follower_relative',
                    'lat': target_lat,
                    'lon': target_lon,
                    'alt': leader_alt,
                    'relative_pos': (rel_x, rel_y),
                    'is_leader': False
                }
        
        # Send formation command with GPS coordinates (only to active nodes)
        active_node_ids = [self.node_id if peer == self.node_metadata else f"drone{self.peers.index(peer) + 1}" for peer in active_peers]
        
        command = f"SET_FORMATION_GPS:{json.dumps({
            'formation_type': formation_type,
            'leader_id': self.node_id,
            'leader_gps': {'lat': leader_lat, 'lon': leader_lon, 'alt': leader_alt},
            'assignments': formation_assignment,
            'active_nodes': active_node_ids,
            'excluded_failed_nodes': list(failed_peers),
            'recalculation_time': time.time()
        })}"
        
        success = await self.replicate_command(command)
        
        result = {
            'success': success,
            'command': 'SET_FORMATION_GPS_DYNAMIC',
            'formation_type': formation_type,
            'leader_id': self.node_id,
            'leader_position': {'lat': leader_lat, 'lon': leader_lon, 'alt': leader_alt},
            'positions_set': len(formation_assignment) if success else 0,
            'active_nodes': len(active_peers),
            'excluded_failed_nodes': list(failed_peers),
            'interval': interval,
            'angle': angle
        }
        
        # If execute immediately is requested
        if execute_immediately and success:
            await asyncio.sleep(1)
            execute_result = await self.execute_formation_movement_gps()
            result['movement_executed'] = execute_result['success']
            result['moved_nodes'] = execute_result.get('moved_nodes', 0)
        
        return result

    async def execute_formation_movement_gps(self) -> Dict:
        """Execute GPS-based formation movement"""
        success_count = 0
        total_nodes = len(self.peers)
        
        # Send formation execution command
        command = f"EXECUTE_GPS_FORMATION:{json.dumps({})}"
        if await self.replicate_command(command):
            success_count = total_nodes  # Assume success for all nodes
        
        return {
            'success': success_count == total_nodes,
            'command': 'EXECUTE_GPS_FORMATION',
            'moved_nodes': success_count,
            'total_nodes': total_nodes
        }
    
    async def execute_formation_movement(self) -> Dict:
        """Execute movement to formation positions"""
        return await self.execute_formation_movement_gps()
    
    async def execute_individual_command(self, target_node: str, command: str) -> Dict:
        """Execute command on specific node"""
        if target_node == self.node_id:
            # Execute locally
            success = await self.replicate_command(command)
            return {'success': success, 'target': target_node, 'command': command}
        else:
            return {'error': 'Individual commands not implemented for remote nodes'}
    
    async def get_swarm_status(self) -> Dict:
        """Get status of entire swarm"""
        swarm_status = {
            'leader': self.node_id,
            'term': self.current_term,
            'nodes': {}
        }
        
        # Add own status
        swarm_status['nodes'][self.node_id] = {
            'state': self.state.value,
            'drone_connected': self.drone_connected,
            'drone_status': self.get_drone_status() if self.drone_connected else None,
            'formation_state': self.swarm_state
        }
        
        return swarm_status
    
    def get_drone_status(self) -> Dict:
        """Get drone status"""
        if self.drone_connected:
            return self.drone_controller.get_drone_status()
        return {'connected': False}
    
    async def replicate_command(self, command: str) -> bool:
        """Replicate command to all nodes through Raft consensus"""
        if self.state != NodeState.LEADER:
            return False
        
        # Create log entry
        log_entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command
        )
        self.log.append(log_entry)
        
        # Send to followers
        success_count = 1  # Count self
        append_tasks = []
        
        for peer in self.peers:
            if peer != self.node_metadata:
                task = asyncio.create_task(self.send_append_entries_with_entry(peer, log_entry))
                append_tasks.append(task)
        
        if append_tasks:
            responses = await asyncio.gather(*append_tasks, return_exceptions=True)
            
            for response in responses:
                if isinstance(response, AppendEntriesResponse) and response.success:
                    success_count += 1
        
        # Check if majority succeeded
        majority = len(self.peers) // 2 + 1
        if success_count >= majority:
            # Commit the entry
            self.commit_index = log_entry.index
            logger.info(f"Command committed: {command}")
            return True
        else:
            # Remove the entry if not committed
            self.log.pop()
            logger.warning(f"Command failed to replicate: {command}")
            return False
    
    async def send_append_entries_with_entry(self, peer_metadata: NodeMetadata, entry: LogEntry) -> Optional[AppendEntriesResponse]:
        """Send append entries with specific entry to a peer"""
        try:
            prev_log_index = entry.index - 1
            prev_log_term = 0
            if prev_log_index >= 0 and prev_log_index < len(self.log) - 1:
                prev_log_term = self.log[prev_log_index].term
            
            # Convert LogEntry to dict for serialization
            entry_dict = asdict(entry)
            
            append_request = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=[entry_dict],  # Send as dict, not LogEntry object
                leader_commit=self.commit_index
            )
            
            message = self.create_message(
                MessageType.APPEND_ENTRIES.value,
                asdict(append_request)
            )
            
            response_msg = await self.send_message_to_peer(peer_metadata, message)
            if response_msg and response_msg.msg_type == MessageType.APPEND_RESPONSE.value:
                return AppendEntriesResponse(**response_msg.data)
        
        except Exception as e:
            peer_key = self._peer_key(peer_metadata)
            logger.debug(f"Failed to send append entries to {peer_key}: {e}")
        
        return None
    
    async def send_message_to_peer(self, peer_metadata: NodeMetadata, message: Message) -> Optional[Message]:
        """Send message to a peer and wait for response - WITH HEALTH TRACKING"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer_metadata.get_host(), peer_metadata.get_port()),
                timeout=2.0  # Increased timeout
            )
            
            try:
                return await self.network_comm.send_message_with_response(peer_metadata, message)
            finally:
                writer.close()
                await writer.wait_closed()
        
        except Exception as e:
            peer_key = self._peer_key(peer_metadata)
            logger.debug(f"Failed to send message to {peer_key}: {e}")
            # Mark communication failure
            self.handle_peer_communication_failure(peer_metadata)
            return None
    
    async def election_timer(self):
        """Monitor election timeout and start elections"""
        while True:
            try:
                await asyncio.sleep(0.1)
                
                if self.state != NodeState.LEADER:
                    time_since_heartbeat = time.time() - self.last_heartbeat
                    if time_since_heartbeat > self.election_timeout:
                        await self.start_election()
            except Exception as e:
                logger.error(f"Error in election timer: {e}")
    
    async def start_election(self):
        """Start a new election"""
        logger.info(f"Starting election for term {self.current_term + 1}")
        
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()
        
        votes_received = 1
        votes_needed = len(self.peers) // 2 + 1
        
        vote_tasks = []
        for peer in self.peers:
            if peer != self.node_metadata:
                task = asyncio.create_task(self.request_vote_from_peer(peer))
                vote_tasks.append(task)
        
        if vote_tasks:
            responses = await asyncio.gather(*vote_tasks, return_exceptions=True)
            
            for response in responses:
                if isinstance(response, VoteResponse):
                    if response.vote_granted:
                        votes_received += 1
                    elif response.term > self.current_term:
                        await self.step_down(response.term)
                        return
        
        if self.state == NodeState.CANDIDATE and votes_received >= votes_needed:
            await self.become_leader()
        else:
            logger.info(f"Election failed. Got {votes_received}/{votes_needed} votes")
            self.state = NodeState.FOLLOWER
    
    async def request_vote_from_peer(self, peer_metadata: NodeMetadata) -> Optional[VoteResponse]:
        """Request vote from a peer"""
        try:
            last_log_index = len(self.log) - 1 if self.log else -1
            last_log_term = self.log[-1].term if self.log else 0
            
            vote_request = VoteRequest(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term
            )
            
            message = self.create_message(
                MessageType.VOTE_REQUEST.value,
                asdict(vote_request)
            )
            
            response_msg = await self.send_message_to_peer(peer_metadata, message)
            if response_msg and response_msg.msg_type == MessageType.VOTE_RESPONSE.value:
                return VoteResponse(**response_msg.data)
        
        except Exception as e:
            peer_key = self._peer_key(peer_metadata)
            logger.debug(f"Failed to request vote from {peer_key}: {e}")
        
        return None
    
    async def become_leader(self):
        """Become the leader"""
        logger.info(f"Became leader for term {self.current_term}")
        self.state = NodeState.LEADER
        
        for peer in self.peers:
            if peer != self.node_metadata:
                peer_key = self._peer_key(peer)
                self.next_index[peer_key] = len(self.log)
                self.match_index[peer_key] = -1
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        self.heartbeat_task = asyncio.create_task(self.send_heartbeats())
    
    async def send_heartbeats(self):
        """Send periodic heartbeats to followers"""
        while self.state == NodeState.LEADER:
            try:
                heartbeat_tasks = []
                for peer in self.peers:
                    if peer != self.node_metadata:
                        task = asyncio.create_task(self.send_append_entries(peer))
                        heartbeat_tasks.append(task)
                
                if heartbeat_tasks:
                    await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
                
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error sending heartbeats: {e}")
    
    async def send_append_entries(self, peer_metadata: NodeMetadata):
        """Send append entries (heartbeat) to a peer"""
        try:
            peer_key = self._peer_key(peer_metadata)
            prev_log_index = self.next_index.get(peer_key, 0) - 1
            prev_log_term = 0
            if prev_log_index >= 0 and prev_log_index < len(self.log):
                prev_log_term = self.log[prev_log_index].term
            
            # For heartbeat, send empty entries (or pending entries)
            entries = []
            
            # If there are entries to replicate to this peer
            next_idx = self.next_index.get(peer_key, len(self.log))
            if next_idx < len(self.log):
                # Send pending entries as dicts
                entries = [asdict(entry) for entry in self.log[next_idx:]]
            
            append_request = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index
            )
            
            message = self.create_message(
                MessageType.APPEND_ENTRIES.value,
                asdict(append_request)
            )
            
            response_msg = await self.send_message_to_peer(peer_metadata, message)
            if response_msg and response_msg.msg_type == MessageType.APPEND_RESPONSE.value:
                # Mark successful communication
                self.handle_successful_peer_communication(peer_metadata)
                
                append_response = AppendEntriesResponse(**response_msg.data)
                
                if append_response.success:
                    # Update next_index and match_index for successful replication
                    if entries:
                        self.next_index[peer_key] = len(self.log)
                        self.match_index[peer_key] = len(self.log) - 1
                else:
                    # Decrement next_index on failure for log consistency
                    if peer_key in self.next_index and self.next_index[peer_key] > 0:
                        self.next_index[peer_key] -= 1
                
                if append_response.term > self.current_term:
                    await self.step_down(append_response.term)
            else:
                # Mark communication failure
                self.handle_peer_communication_failure(peer_metadata)
        
        except Exception as e:
            peer_key = self._peer_key(peer_metadata)
            logger.debug(f"Failed to send heartbeat to {peer_key}: {e}")
            # Mark communication failure
            self.handle_peer_communication_failure(peer_metadata)
    
    async def step_down(self, new_term: int):
        """Step down from leadership and update term"""
        logger.info(f"Stepping down. New term: {new_term}")
        self.current_term = new_term
        self.voted_for = None
        self.state = NodeState.FOLLOWER
        self.last_heartbeat = time.time()
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None
    
    async def handle_vote_request(self, vote_request: VoteRequest) -> VoteResponse:
        """Handle incoming vote request"""
        vote_granted = False
        
        if vote_request.term < self.current_term:
            pass
        else:
            if vote_request.term > self.current_term:
                await self.step_down(vote_request.term)
            
            last_log_index = len(self.log) - 1 if self.log else -1
            last_log_term = self.log[-1].term if self.log else 0
            
            log_ok = (vote_request.last_log_term > last_log_term or 
                     (vote_request.last_log_term == last_log_term and 
                      vote_request.last_log_index >= last_log_index))
            
            if (self.voted_for is None or self.voted_for == vote_request.candidate_id) and log_ok:
                vote_granted = True
                self.voted_for = vote_request.candidate_id
                self.last_heartbeat = time.time()
        
        logger.debug(f"Vote request from {vote_request.candidate_id}: granted={vote_granted}")
        return VoteResponse(term=self.current_term, vote_granted=vote_granted)
    
    async def handle_append_entries(self, append_request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle incoming append entries"""
        success = False
        
        if append_request.term < self.current_term:
            # Reply false if term < currentTerm
            pass
        else:
            if append_request.term > self.current_term:
                await self.step_down(append_request.term)
            
            self.last_heartbeat = time.time()
            if self.state == NodeState.CANDIDATE:
                self.state = NodeState.FOLLOWER
            
            # Check log consistency
            if (append_request.prev_log_index == -1 or 
                (append_request.prev_log_index < len(self.log) and 
                 len(self.log) > append_request.prev_log_index and
                 self.log[append_request.prev_log_index].term == append_request.prev_log_term)):
                
                success = True
                
                # Append new entries if any
                if append_request.entries:
                    start_index = append_request.prev_log_index + 1
                    
                    # Remove conflicting entries
                    if start_index < len(self.log):
                        self.log = self.log[:start_index]
                    
                    # Append new entries (they come as dicts, convert to LogEntry)
                    for entry_data in append_request.entries:
                        if isinstance(entry_data, dict):
                            entry = LogEntry(**entry_data)
                        else:
                            # Should not happen, but handle gracefully
                            entry = entry_data
                        
                        self.log.append(entry)
                        logger.debug(f"Appended log entry: {entry.command}")
                
                # Update commit index
                if append_request.leader_commit > self.commit_index:
                    self.commit_index = min(append_request.leader_commit, len(self.log) - 1)
                    logger.debug(f"Updated commit index to: {self.commit_index}")
            
            else:
                logger.debug(f"Log consistency check failed. prev_log_index: {append_request.prev_log_index}, log_length: {len(self.log)}")
            
            logger.debug(f"Append entries from leader {append_request.leader_id}: success={success}, entries={len(append_request.entries)}")
        
        return AppendEntriesResponse(term=self.current_term, success=success)
    
    async def handle_client_request(self, request_data: Dict) -> Dict:
        """Handle client request (only if leader)"""
        if self.state != NodeState.LEADER:
            return {'error': 'Not leader', 'leader_id': None}
        
        command = request_data.get('command', '')
        success = await self.replicate_command(command)
        
        if success:
            return {'success': True, 'index': len(self.log) - 1}
        else:
            return {'error': 'Failed to replicate command'}
    
    async def handle_status_request(self) -> Dict:
        """Handle status request - WITH FORMATION AND HEALTH INFO"""
        drone_status = self.get_drone_status() if self.drone_connected else None
        
        # Get peer health info
        active_peers = self.get_active_peers()
        failed_peers = self.health_tracker.get_failed_peers()
        
        return {
            'node_id': self.node_id,
            'state': self.state.value,
            'term': self.current_term,
            'voted_for': self.voted_for,
            'log_length': len(self.log),
            'commit_index': self.commit_index,
            'address': f"{self.node_metadata.get_host()}:{self.node_metadata.get_port()}",
            'drone_index': self.drone_index,
            'drone_connected': self.drone_connected,
            'drone_status': drone_status,
            'swarm_state': self.swarm_state,
            'peer_health': {
                'active_peers': [self._peer_key(p) for p in active_peers],
                'failed_peers': list(failed_peers),
                'total_peers': len(self.peers),
                'active_count': len(active_peers)
            },
            'formation_state': {
                'last_config': self.last_formation_config,
                'last_calculation_time': self.last_formation_time
            }
        }
    
    async def stop(self):
        """Stop the node and disconnect drone"""
        await self.network_comm.stop()
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        if self.election_task:
            self.election_task.cancel()
        
        if self.drone_monitor_task:
            self.drone_monitor_task.cancel()
        
        if self.drone_controller:
            self.drone_controller.cleanup()

async def create_single_raft_drone_node(host: str, port: int):
    """Create and start a single Raft-Drone node using NodeMetadata"""
    try:
        # Get configuration from config manager
        all_nodes = config_manager.get_all_nodes()
        all_ports = config_manager.get_all_ports()
        
        if port not in all_ports:
            raise ValueError(f"Port {port} not found in configuration. Available ports: {all_ports}")
        
        # Get node information
        drone_index = config_manager.get_drone_index_by_port(port)
        node_id = config_manager.get_node_id_by_port(port)
        
        # Create NodeMetadata for this node
        node_metadata = NodeMetadata(host, port)
        
        # Create peer list as NodeMetadata objects
        peers = [NodeMetadata(host, node_port) for node_id, node_port, _ in all_nodes]
        
        logger.info(f"Creating node {node_id} (drone{drone_index}) at {host}:{port}")
        logger.info(f"Peers: {[f'{p.get_host()}:{p.get_port()}' for p in peers]}")
        
        # Create and start the node
        node = RaftDroneNode(node_metadata, drone_index, peers)
        await node.start()
        
        return node
        
    except Exception as e:
        logger.error(f"Error creating node: {e}")
        raise


async def main():
    """Main function with command line argument parsing"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python raft_node.py <port> [host]")
        try:
            available_ports = config_manager.get_all_ports()
            print(f"Available ports: {available_ports}")
        except Exception as e:
            print(f"Error reading configuration: {e}")
        return
    
    try:
        port = int(sys.argv[1])
        host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'
        
        await create_single_raft_drone_node(host, port)
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except ValueError as e:
        print(f"Error: {e}")
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
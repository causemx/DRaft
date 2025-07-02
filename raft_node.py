#!/usr/bin/env python3
"""
Integrated Raft-Drone Node - Combines Raft consensus with drone control
Each Raft node controls a specific drone, enabling distributed consensus for swarm operations
Enhanced with leader-centered formation system
"""

import asyncio
import json
import random
import time
import logging
import struct
import threading
from enum import Enum
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple, Any
from libs.utils import DroneController, FlightMode

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Drone connection strings for SITL instances
DRONE_CONNECTIONS = [
    "udp:172.21.128.1:14550",  # Drone 0
    "udp:172.21.128.1:14560",  # Drone 1
    "udp:172.21.128.1:14570",  # Drone 2
    "udp:172.21.128.1:14580",  # Drone 3
    "udp:172.21.128.1:14590"   # Drone 4
]

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class MessageType(Enum):
    VOTE_REQUEST = "vote_request"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_RESPONSE = "append_response"
    CLIENT_REQUEST = "client_request"
    CLIENT_RESPONSE = "client_response"
    STATUS_REQUEST = "status_request"
    STATUS_RESPONSE = "status_response"
    SWARM_COMMAND = "swarm_command"
    SWARM_RESPONSE = "swarm_response"

class SwarmCommandType(Enum):
    CONNECT_ALL = "connect_all"
    ARM_ALL = "arm_all"
    DISARM_ALL = "disarm_all"
    TAKEOFF_ALL = "takeoff_all"
    LAND_ALL = "land_all"
    SET_FORMATION = "set_formation"
    EXECUTE_FORMATION = "execute_formation"
    GET_SWARM_STATUS = "get_swarm_status"
    INDIVIDUAL_COMMAND = "individual_command"

@dataclass
class LogEntry:
    term: int
    index: int
    command: str
    timestamp: float = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()

@dataclass
class Message:
    msg_type: str
    data: Dict[str, Any]
    sender_id: str

@dataclass
class VoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

@dataclass
class VoteResponse:
    term: int
    vote_granted: bool

@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict]  # Serialized LogEntry objects
    leader_commit: int

@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int = -1

@dataclass
class SwarmCommand:
    command_type: str
    parameters: Dict[str, Any]
    target_nodes: List[str] = None  # None means all nodes

class SocketProtocol:
    """Simple protocol for sending/receiving messages over TCP"""
    
    @staticmethod
    async def send_message(writer: asyncio.StreamWriter, message: Message):
        """Send a message over the socket connection"""
        try:
            # Serialize message to JSON
            json_data = json.dumps({
                'msg_type': message.msg_type,
                'data': message.data,
                'sender_id': message.sender_id
            }).encode('utf-8')
            
            # Send length prefix + message
            length = struct.pack('!I', len(json_data))
            writer.write(length + json_data)
            await writer.drain()
        except Exception as e:
            logging.error(f"Error sending message: {e}")
    
    @staticmethod
    async def receive_message(reader: asyncio.StreamReader) -> Optional[Message]:
        """Receive a message from the socket connection"""
        try:
            # Read length prefix
            length_data = await reader.readexactly(4)
            if not length_data:
                return None
            
            length = struct.unpack('!I', length_data)[0]
            
            # Read message data
            json_data = await reader.readexactly(length)
            data = json.loads(json_data.decode('utf-8'))
            
            return Message(
                msg_type=data['msg_type'],
                data=data['data'],
                sender_id=data['sender_id']
            )
        except (asyncio.IncompleteReadError, json.JSONDecodeError, struct.error) as e:
            logging.debug(f"Error receiving message: {e}")
            return None

class RaftDroneNode:
    def __init__(self, node_id: str, port: int, drone_index: int, peers: List[Tuple[str, int]]):
        self.node_id = node_id
        self.port = port
        self.drone_index = drone_index
        self.peers = peers  # List of (node_id, port) tuples
        
        # Drone controller
        self.drone_controller = DroneController(DRONE_CONNECTIONS[drone_index])
        self.drone_connected = False
        
        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER
        
        # Leader state
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
        
        # Swarm coordination
        self.swarm_state = {
            'formation_type': None,
            'formation_role': None,
            'target_gps': None,
            'leader_id': None,
            'leader_gps': None
        }
        
        self.logger = logging.getLogger(f"RaftDrone-{self.node_id}")
        
    def _random_election_timeout(self) -> float:
        """Random election timeout between 1.5-3 seconds"""
        return random.uniform(1.5, 3.0)
    
    async def start(self):
        """Start the Raft node and drone connection"""
        self.logger.info(f"Starting Raft-Drone node {self.node_id} on port {self.port}")
        
        # Connect to drone
        await self.connect_drone()
        
        # Start TCP server
        self.server = await asyncio.start_server(
            self.handle_client_connection,
            'localhost',
            self.port
        )
        
        # Start election timer
        self.election_task = asyncio.create_task(self.election_timer())
        
        # Start drone monitoring
        self.drone_monitor_task = asyncio.create_task(self.drone_monitor_loop())
        
        self.logger.info(f"Node {self.node_id} is listening on port {self.port}")
    
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
                self.logger.info(f"Connected to drone {self.drone_index}")
            else:
                self.logger.error(f"Failed to connect to drone {self.drone_index}")
        except Exception as e:
            self.logger.error(f"Error connecting to drone: {e}")
    
    async def drone_monitor_loop(self):
        """Monitor drone status and apply committed commands"""
        while True:
            try:
                # Apply any committed but not applied log entries
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    if self.last_applied <= len(self.log):
                        entry = self.log[self.last_applied - 1]
                        await self.apply_command(entry.command)
                
                await asyncio.sleep(0.1)  # Check every 100ms
            except Exception as e:
                self.logger.error(f"Error in drone monitor loop: {e}")
                await asyncio.sleep(1)
    
    async def apply_command(self, command: str):
        """Apply a committed command to the drone"""
        try:
            if not self.drone_connected:
                self.logger.warning(f"Cannot apply command '{command}' - drone not connected")
                return
            
            # Parse command
            parts = command.split(':', 1)
            if len(parts) != 2:
                self.logger.warning(f"Invalid command format: {command}")
                return
            
            cmd_type, cmd_data = parts
            data = json.loads(cmd_data) if cmd_data else {}
            
            loop = asyncio.get_event_loop()
            
            if cmd_type in ["ARM", "ARM_ALL"]:
                result = await loop.run_in_executor(None, self.drone_controller.arm)
                self.logger.info(f"Applied ARM command to drone {self.drone_index}: {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type in ["DISARM", "DISARM_ALL"]:
                result = await loop.run_in_executor(None, self.drone_controller.disarm)
                self.logger.info(f"Applied DISARM command to drone {self.drone_index}: {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type in ["TAKEOFF", "TAKEOFF_ALL"]:
                altitude = data.get('altitude', 10.0)
                result = await loop.run_in_executor(None, self.drone_controller.takeoff, altitude)
                self.logger.info(f"Applied TAKEOFF command to drone {self.drone_index} at {altitude}m: {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type in ["LAND", "LAND_ALL"]:
                result = await loop.run_in_executor(None, self.drone_controller.land)
                self.logger.info(f"Applied LAND command to drone {self.drone_index}: {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type == "SET_MODE":
                mode = data.get('mode', 'GUIDED')
                result = await loop.run_in_executor(None, self.drone_controller.set_flight_mode, mode)
                self.logger.info(f"Applied SET_MODE command to drone {self.drone_index}: {mode} - {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type == "FLY_TO":
                distance = data.get('distance', 0)
                angle = data.get('angle', 0)
                result = await loop.run_in_executor(None, self.drone_controller.fly_to_here, distance, angle)
                self.logger.info(f"Applied FLY_TO command to drone {self.drone_index}: {distance}m at {angle}° - {'SUCCESS' if result else 'FAILED'}")
            
            elif cmd_type == "SET_FORMATION_GPS":
                leader_id = data.get('leader_id')
                leader_gps = data.get('leader_gps', {})
                assignments = data.get('assignments', {})
                
                # Find this node's assignment
                my_assignment = assignments.get(self.node_id)
                if my_assignment:
                    if my_assignment.get('is_leader'):
                        self.logger.info(f"Leader {self.node_id} maintains center position at GPS: {leader_gps}")
                        self.swarm_state['formation_role'] = 'leader_center'
                    else:
                        target_lat = my_assignment['lat']
                        target_lon = my_assignment['lon']
                        target_alt = my_assignment['alt']
                        rel_pos = my_assignment['relative_pos']
                        
                        self.logger.info(f"Follower {self.node_id} target GPS: {target_lat:.6f}, {target_lon:.6f}")
                        self.logger.info(f"Relative to leader: {rel_pos[0]:.1f}m East, {rel_pos[1]:.1f}m North")
                        
                        self.swarm_state['formation_role'] = 'follower'
                    
                    # Store formation data
                    self.swarm_state['formation_type'] = data.get('formation_type')
                    self.swarm_state['target_gps'] = my_assignment
                    self.swarm_state['leader_id'] = leader_id
                    self.swarm_state['leader_gps'] = leader_gps
                else:
                    self.logger.warning(f"No formation assignment for {self.node_id}")
            
            elif cmd_type == "EXECUTE_GPS_FORMATION":
                # Execute movement to GPS formation position
                target_gps = self.swarm_state.get('target_gps')
                if target_gps and not target_gps.get('is_leader'):
                    # Only followers move, leader stays in place
                    target_lat = target_gps['lat']
                    target_lon = target_gps['lon']
                    target_alt = target_gps['alt']
                    
                    # Get current position
                    current_status = self.get_drone_status()
                    current_pos = current_status.get('position')
                    
                    if current_pos:
                        current_lat, current_lon = current_pos
                        
                        # Calculate distance and bearing to target
                        distance, bearing = self.calculate_distance_bearing(
                            current_lat, current_lon, target_lat, target_lon
                        )
                        
                        self.logger.info(f"Moving to formation: {distance:.1f}m at {bearing:.1f}°")
                        
                        # Execute movement
                        result = await loop.run_in_executor(
                            None, self.drone_controller.fly_to_here, distance, bearing
                        )
                        
                        success_msg = "SUCCESS" if result else "FAILED"
                        self.logger.info(f"GPS formation movement: {success_msg}")
                    else:
                        self.logger.error("Current GPS position not available")
                else:
                    self.logger.info(f"Leader {self.node_id} remains at center position")
            
            else:
                self.logger.warning(f"Unknown command type: {cmd_type}")
        
        except Exception as e:
            self.logger.error(f"Error applying command '{command}': {e}")

    def calculate_distance_bearing(self, lat1: float, lon1: float, lat2: float, lon2: float) -> Tuple[float, float]:
        """
        Calculate distance and bearing between two GPS points
        
        Returns:
            (distance_meters, bearing_degrees): Distance and bearing to target
        """
        import math
        
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        # Calculate distance using Haversine formula
        a = (math.sin(delta_lat/2)**2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = 6378137.0 * c  # Earth radius in meters
        
        # Calculate bearing
        y = math.sin(delta_lon) * math.cos(lat2_rad)
        x = (math.cos(lat1_rad) * math.sin(lat2_rad) - 
             math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon))
        bearing = math.atan2(y, x)
        bearing = math.degrees(bearing)
        bearing = (bearing + 360) % 360  # Normalize to 0-360
        
        return distance, bearing

    def relative_to_gps(self, base_lat: float, base_lon: float, rel_x: float, rel_y: float) -> Tuple[float, float]:
        """
        Convert relative X,Y position (meters) to GPS coordinates
        
        Args:
            base_lat, base_lon: Base GPS position (leader's position)
            rel_x, rel_y: Relative position in meters (X=East, Y=North)
        
        Returns:
            (target_lat, target_lon): Target GPS coordinates
        """
        import math
        
        # Earth radius in meters
        earth_radius = 6378137.0
        
        # Convert relative position to lat/lon offset
        lat_offset = rel_y / earth_radius * (180.0 / math.pi)
        lon_offset = rel_x / (earth_radius * math.cos(math.radians(base_lat))) * (180.0 / math.pi)
        
        target_lat = base_lat + lat_offset
        target_lon = base_lon + lon_offset
        
        return target_lat, target_lon

    def calculate_formation_positions_leader_centered(self, formation_type: str, interval: float, angle: float = 0.0) -> List[Tuple[float, float]]:
        """
        Calculate formation positions with LEADER AT CENTER
        Returns relative positions for all drones (leader gets (0,0))
        
        Args:
            formation_type: Only 'line' is supported
            interval: Distance between adjacent drones in meters
            angle: Rotation angle of the entire line in degrees
        
        Returns:
            List of (x, y) relative positions for each drone
            Leader position is always (0, 0)
        """
        import math
        
        num_drones = len(self.peers)
        positions = []
        
        # Only support line formation
        if formation_type.lower() != 'line':
            self.logger.warning(f"Unsupported formation type: {formation_type}. Using line formation.")
        
        # Find leader index in peers list
        leader_index = None
        for i, (node_id, _) in enumerate(self.peers):
            if node_id == self.node_id:
                leader_index = i
                break
        
        if leader_index is None:
            self.logger.error("Leader not found in peers list!")
            return []
        
        # Calculate line formation with leader at center
        for i in range(num_drones):
            if i == leader_index:
                # Leader stays at center (0, 0)
                x, y = 0.0, 0.0
            else:
                # Calculate relative position for this drone
                # Position index relative to leader
                relative_index = i - leader_index
                
                # Position along the line
                x_offset = relative_index * interval
                y_offset = 0.0
                
                # Apply rotation transformation
                angle_rad = math.radians(angle)
                x = x_offset * math.cos(angle_rad) - y_offset * math.sin(angle_rad)
                y = x_offset * math.sin(angle_rad) + y_offset * math.cos(angle_rad)
            
            positions.append((x, y))
            node_id = self.peers[i][0]
            
            if i == leader_index:
                self.logger.info(f"Leader {node_id}: CENTER position (0.0, 0.0)")
            else:
                self.logger.info(f"Follower {node_id}: relative position ({x:.1f}, {y:.1f})")
        
        return positions
    
    async def handle_client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming client connections"""
        client_addr = writer.get_extra_info('peername')
        self.logger.debug(f"New connection from {client_addr}")
        
        try:
            while True:
                message = await SocketProtocol.receive_message(reader)
                if not message:
                    break
                
                await self.process_message(message, reader, writer)
        except Exception as e:
            self.logger.debug(f"Connection error with {client_addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def process_message(self, message: Message, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Process incoming messages"""
        try:
            if message.msg_type == MessageType.VOTE_REQUEST.value:
                response = await self.handle_vote_request(VoteRequest(**message.data))
                response_msg = Message(
                    msg_type=MessageType.VOTE_RESPONSE.value,
                    data=asdict(response),
                    sender_id=self.node_id
                )
                await SocketProtocol.send_message(writer, response_msg)
            
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
                            self.logger.error(f"Invalid entry data: {entry_data}")
                            continue
                
                # Create a copy of message data with converted entries
                append_data = message.data.copy()
                append_data['entries'] = entries
                
                append_request = AppendEntriesRequest(**append_data)
                response = await self.handle_append_entries(append_request)
                response_msg = Message(
                    msg_type=MessageType.APPEND_RESPONSE.value,
                    data=asdict(response),
                    sender_id=self.node_id
                )
                await SocketProtocol.send_message(writer, response_msg)
            
            elif message.msg_type == MessageType.CLIENT_REQUEST.value:
                response = await self.handle_client_request(message.data)
                response_msg = Message(
                    msg_type=MessageType.CLIENT_RESPONSE.value,
                    data=response,
                    sender_id=self.node_id
                )
                await SocketProtocol.send_message(writer, response_msg)
            
            elif message.msg_type == MessageType.SWARM_COMMAND.value:
                response = await self.handle_swarm_command(message.data)
                response_msg = Message(
                    msg_type=MessageType.SWARM_RESPONSE.value,
                    data=response,
                    sender_id=self.node_id
                )
                await SocketProtocol.send_message(writer, response_msg)
            
            elif message.msg_type == MessageType.STATUS_REQUEST.value:
                response = await self.handle_status_request()
                response_msg = Message(
                    msg_type=MessageType.STATUS_RESPONSE.value,
                    data=response,
                    sender_id=self.node_id
                )
                await SocketProtocol.send_message(writer, response_msg)
        
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
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
            self.logger.error(f"Error handling swarm command: {e}")
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
        Set formation for the swarm - LEADER-CENTERED VERSION
        Leader stays at current position, others move relative to leader's GPS coordinates
        """
        formation_type = parameters.get('formation_type')
        interval = parameters.get('interval', 10.0)
        angle = parameters.get('angle', 0.0)
        execute_immediately = parameters.get('execute', False)
        
        # Get leader's current GPS position
        leader_status = self.get_drone_status()
        leader_position = leader_status.get('position')
        
        if not leader_position:
            return {'error': 'Leader GPS position not available', 'leader_id': self.node_id}
        
        leader_lat, leader_lon = leader_position
        leader_alt = leader_status.get('altitude', 0)
        
        self.logger.info(f"Leader {self.node_id} GPS: {leader_lat:.6f}, {leader_lon:.6f}, {leader_alt:.1f}m")
        
        # Calculate formation positions relative to leader
        relative_positions = self.calculate_formation_positions_leader_centered(
            formation_type, interval, angle
        )
        
        # Convert relative positions to GPS coordinates
        formation_assignment = {}
        
        for i, (node_id, _) in enumerate(self.peers):
            if node_id == self.node_id:
                # Leader stays at current position
                formation_assignment[node_id] = {
                    'type': 'leader_center',
                    'lat': leader_lat,
                    'lon': leader_lon,
                    'alt': leader_alt,
                    'relative_pos': (0.0, 0.0),
                    'is_leader': True
                }
            elif i < len(relative_positions):
                # Calculate GPS coordinates for follower drones
                rel_x, rel_y = relative_positions[i]
                target_lat, target_lon = self.relative_to_gps(leader_lat, leader_lon, rel_x, rel_y)
                
                formation_assignment[node_id] = {
                    'type': 'follower_relative',
                    'lat': target_lat,
                    'lon': target_lon,
                    'alt': leader_alt,  # Same altitude as leader
                    'relative_pos': (rel_x, rel_y),
                    'is_leader': False
                }
        
        # Send formation command with GPS coordinates
        command = f"SET_FORMATION_GPS:{json.dumps({
            'formation_type': formation_type,
            'leader_id': self.node_id,
            'leader_gps': {'lat': leader_lat, 'lon': leader_lon, 'alt': leader_alt},
            'assignments': formation_assignment
        })}"
        
        success = await self.replicate_command(command)
        
        result = {
            'success': success,
            'command': 'SET_FORMATION_GPS',
            'formation_type': formation_type,
            'leader_id': self.node_id,
            'leader_position': {'lat': leader_lat, 'lon': leader_lon, 'alt': leader_alt},
            'positions_set': len(formation_assignment) if success else 0,
            'interval': interval,
            'angle': angle
        }
        
        # If execute immediately is requested
        if execute_immediately and success:
            await asyncio.sleep(1)  # Small delay to ensure SET_FORMATION is applied
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
        
        for peer_id, peer_port in self.peers:
            if peer_id != self.node_id:
                task = asyncio.create_task(self.send_append_entries_with_entry(peer_id, peer_port, log_entry))
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
            self.logger.info(f"Command committed: {command}")
            return True
        else:
            # Remove the entry if not committed
            self.log.pop()
            self.logger.warning(f"Command failed to replicate: {command}")
            return False
    
    async def send_append_entries_with_entry(self, peer_id: str, peer_port: int, entry: LogEntry) -> Optional[AppendEntriesResponse]:
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
            
            message = Message(
                msg_type=MessageType.APPEND_ENTRIES.value,
                data=asdict(append_request),
                sender_id=self.node_id
            )
            
            response_msg = await self.send_message_to_peer(peer_id, peer_port, message)
            if response_msg and response_msg.msg_type == MessageType.APPEND_RESPONSE.value:
                return AppendEntriesResponse(**response_msg.data)
        
        except Exception as e:
            self.logger.debug(f"Failed to send append entries to {peer_id}: {e}")
        
        return None
    
    async def send_message_to_peer(self, peer_id: str, peer_port: int, message: Message) -> Optional[Message]:
        """Send message to a peer and wait for response"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('localhost', peer_port),
                timeout=1.0
            )
            
            try:
                await SocketProtocol.send_message(writer, message)
                response = await asyncio.wait_for(
                    SocketProtocol.receive_message(reader),
                    timeout=1.0
                )
                return response
            finally:
                writer.close()
                await writer.wait_closed()
        
        except Exception as e:
            self.logger.debug(f"Failed to send message to {peer_id}: {e}")
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
                self.logger.error(f"Error in election timer: {e}")
    
    async def start_election(self):
        """Start a new election"""
        self.logger.info(f"Starting election for term {self.current_term + 1}")
        
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()
        
        votes_received = 1
        votes_needed = len(self.peers) // 2 + 1
        
        vote_tasks = []
        for peer_id, peer_port in self.peers:
            if peer_id != self.node_id:
                task = asyncio.create_task(self.request_vote_from_peer(peer_id, peer_port))
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
            self.logger.info(f"Election failed. Got {votes_received}/{votes_needed} votes")
            self.state = NodeState.FOLLOWER
    
    async def request_vote_from_peer(self, peer_id: str, peer_port: int) -> Optional[VoteResponse]:
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
            
            message = Message(
                msg_type=MessageType.VOTE_REQUEST.value,
                data=asdict(vote_request),
                sender_id=self.node_id
            )
            
            response_msg = await self.send_message_to_peer(peer_id, peer_port, message)
            if response_msg and response_msg.msg_type == MessageType.VOTE_RESPONSE.value:
                return VoteResponse(**response_msg.data)
        
        except Exception as e:
            self.logger.debug(f"Failed to request vote from {peer_id}: {e}")
        
        return None
    
    async def become_leader(self):
        """Become the leader"""
        self.logger.info(f"Became leader for term {self.current_term}")
        self.state = NodeState.LEADER
        
        for peer_id, _ in self.peers:
            if peer_id != self.node_id:
                self.next_index[peer_id] = len(self.log)
                self.match_index[peer_id] = -1
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        self.heartbeat_task = asyncio.create_task(self.send_heartbeats())
    
    async def send_heartbeats(self):
        """Send periodic heartbeats to followers"""
        while self.state == NodeState.LEADER:
            try:
                heartbeat_tasks = []
                for peer_id, peer_port in self.peers:
                    if peer_id != self.node_id:
                        task = asyncio.create_task(self.send_append_entries(peer_id, peer_port))
                        heartbeat_tasks.append(task)
                
                if heartbeat_tasks:
                    await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
                
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                self.logger.error(f"Error sending heartbeats: {e}")
    
    async def send_append_entries(self, peer_id: str, peer_port: int):
        """Send append entries (heartbeat) to a peer"""
        try:
            prev_log_index = self.next_index.get(peer_id, 0) - 1
            prev_log_term = 0
            if prev_log_index >= 0 and prev_log_index < len(self.log):
                prev_log_term = self.log[prev_log_index].term
            
            # For heartbeat, send empty entries (or pending entries)
            entries = []
            
            # If there are entries to replicate to this peer
            next_idx = self.next_index.get(peer_id, len(self.log))
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
            
            message = Message(
                msg_type=MessageType.APPEND_ENTRIES.value,
                data=asdict(append_request),
                sender_id=self.node_id
            )
            
            response_msg = await self.send_message_to_peer(peer_id, peer_port, message)
            if response_msg and response_msg.msg_type == MessageType.APPEND_RESPONSE.value:
                append_response = AppendEntriesResponse(**response_msg.data)
                
                if append_response.success:
                    # Update next_index and match_index for successful replication
                    if entries:
                        self.next_index[peer_id] = len(self.log)
                        self.match_index[peer_id] = len(self.log) - 1
                else:
                    # Decrement next_index on failure for log consistency
                    if peer_id in self.next_index and self.next_index[peer_id] > 0:
                        self.next_index[peer_id] -= 1
                
                if append_response.term > self.current_term:
                    await self.step_down(append_response.term)
        
        except Exception as e:
            self.logger.debug(f"Failed to send heartbeat to {peer_id}: {e}")
    
    async def step_down(self, new_term: int):
        """Step down from leadership and update term"""
        self.logger.info(f"Stepping down. New term: {new_term}")
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
        
        self.logger.debug(f"Vote request from {vote_request.candidate_id}: granted={vote_granted}")
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
                        self.logger.debug(f"Appended log entry: {entry.command}")
                
                # Update commit index
                if append_request.leader_commit > self.commit_index:
                    self.commit_index = min(append_request.leader_commit, len(self.log) - 1)
                    self.logger.debug(f"Updated commit index to: {self.commit_index}")
            
            else:
                self.logger.debug(f"Log consistency check failed. prev_log_index: {append_request.prev_log_index}, log_length: {len(self.log)}")
            
            self.logger.debug(f"Append entries from leader {append_request.leader_id}: success={success}, entries={len(append_request.entries)}")
        
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
        """Handle status request"""
        drone_status = self.get_drone_status() if self.drone_connected else None
        
        return {
            'node_id': self.node_id,
            'state': self.state.value,
            'term': self.current_term,
            'voted_for': self.voted_for,
            'log_length': len(self.log),
            'commit_index': self.commit_index,
            'port': self.port,
            'drone_index': self.drone_index,
            'drone_connected': self.drone_connected,
            'drone_status': drone_status,
            'swarm_state': self.swarm_state
        }
    
    async def stop(self):
        """Stop the node and disconnect drone"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        if self.election_task:
            self.election_task.cancel()
        
        if self.drone_monitor_task:
            self.drone_monitor_task.cancel()
        
        if self.drone_controller:
            self.drone_controller.cleanup()

# Client utilities for swarm control
class RaftDroneClient:
    """Client for interacting with Raft-Drone cluster"""
    
    @staticmethod
    async def send_swarm_command(host: str, port: int, command_type: str, parameters: Dict = None) -> Optional[Dict]:
        """Send a swarm command to the cluster leader"""
        try:
            swarm_command = SwarmCommand(
                command_type=command_type,
                parameters=parameters or {}
            )
            
            message = Message(
                msg_type=MessageType.SWARM_COMMAND.value,
                data=asdict(swarm_command),
                sender_id='client'
            )
            
            reader, writer = await asyncio.open_connection(host, port)
            try:
                await SocketProtocol.send_message(writer, message)
                response = await SocketProtocol.receive_message(reader)
                
                if response and response.msg_type == MessageType.SWARM_RESPONSE.value:
                    return response.data
            finally:
                writer.close()
                await writer.wait_closed()
        
        except Exception as e:
            logging.error(f"Swarm command failed: {e}")
        
        return None
    
    @staticmethod
    async def get_node_status(host: str, port: int) -> Optional[Dict]:
        """Get status from a Raft-Drone node"""
        try:
            message = Message(
                msg_type=MessageType.STATUS_REQUEST.value,
                data={},
                sender_id='client'
            )
            
            reader, writer = await asyncio.open_connection(host, port)
            try:
                await SocketProtocol.send_message(writer, message)
                response = await SocketProtocol.receive_message(reader)
                
                if response and response.msg_type == MessageType.STATUS_RESPONSE.value:
                    return response.data
            finally:
                writer.close()
                await writer.wait_closed()
        
        except Exception as e:
            logging.debug(f"Status request failed: {e}")
        
        return None
    
    @staticmethod
    async def find_leader(ports: List[int]) -> Optional[Tuple[str, int]]:
        """Find the current leader in the cluster"""
        for port in ports:
            status = await RaftDroneClient.get_node_status('localhost', port)
            if status and status.get('state') == 'leader':
                return status.get('node_id'), port
        return None

async def create_single_raft_drone_node(port: int):
    """Create and start a single Raft-Drone node"""
    ports = [8001, 8002, 8003, 8004, 8005]
    node_ids = ['drone1', 'drone2', 'drone3', 'drone4', 'drone5']
    
    # Create peer list for the entire cluster
    peers = [(node_id, p) for node_id, p in zip(node_ids, ports)]
    
    # Find the node_id and drone_index for this port
    try:
        port_index = ports.index(port)
        node_id = node_ids[port_index]
        drone_index = port_index
    except ValueError:
        raise ValueError(f"Port {port} not in allowed ports: {ports}")
    
    # Create and start the node
    node = RaftDroneNode(node_id, port, drone_index, peers)
    await node.start()
    
    return node

async def main():
    """Main function with command line argument parsing"""
    import sys
    
    if len(sys.argv) == 1:
        print("Usage: python raft_node.py <port>")
        print("Available ports: 8001, 8002, 8003, 8004, 8005")
        return
    
    try:
        port = int(sys.argv[1])
        if port not in [8001, 8002, 8003, 8004, 8005]:
            print("Error: Port must be one of: 8001, 8002, 8003, 8004, 8005")
            return
        await create_single_raft_drone_node(port)
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except ValueError:
        print(f"Error: Invalid port '{sys.argv[1]}'. Must be a number.")
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    asyncio.run(main())
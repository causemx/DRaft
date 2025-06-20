import asyncio
import json
import random
import time
import logging
import struct
from enum import Enum
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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

@dataclass
class LogEntry:
    term: int
    index: int
    command: str

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

class RaftNode:
    def __init__(self, node_id: str, port: int, peers: List[Tuple[str, int]]):
        self.node_id = node_id
        self.port = port
        self.peers = peers  # List of (node_id, port) tuples
        
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
        self.client_connections: Dict[str, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        
        # Tasks
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.election_task: Optional[asyncio.Task] = None
        
        self.logger = logging.getLogger(f"RaftNode-{self.node_id}")
        
    def _random_election_timeout(self) -> float:
        """Random election timeout between 1.5-3 seconds"""
        return random.uniform(1.5, 3.0)
    
    async def start(self):
        """Start the Raft node"""
        self.logger.info(f"Starting Raft node {self.node_id} on port {self.port}")
        
        # Start TCP server
        self.server = await asyncio.start_server(
            self.handle_client_connection,
            'localhost',
            self.port
        )
        
        # Start election timer
        self.election_task = asyncio.create_task(self.election_timer())
        
        self.logger.info(f"Node {self.node_id} is listening on port {self.port}")
    
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
                entries = [LogEntry(**entry) for entry in entries_data]
                message.data['entries'] = entries
                
                append_request = AppendEntriesRequest(**message.data)
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
    
    async def send_message_to_peer(self, peer_id: str, peer_port: int, message: Message) -> Optional[Message]:
        """Send message to a peer and wait for response"""
        try:
            # Create connection
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('localhost', peer_port),
                timeout=1.0
            )
            
            try:
                # Send message
                await SocketProtocol.send_message(writer, message)
                
                # Wait for response
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
                await asyncio.sleep(0.1)  # Check every 100ms
                
                if self.state != NodeState.LEADER:
                    time_since_heartbeat = time.time() - self.last_heartbeat
                    if time_since_heartbeat > self.election_timeout:
                        await self.start_election()
            except Exception as e:
                self.logger.error(f"Error in election timer: {e}")
    
    async def start_election(self):
        """Start a new election"""
        self.logger.info(f"Starting election for term {self.current_term + 1}")
        
        # Transition to candidate
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()
        
        # Vote for self
        votes_received = 1
        votes_needed = len(self.peers) // 2 + 1
        
        # Request votes from peers
        vote_tasks = []
        for peer_id, peer_port in self.peers:
            if peer_id != self.node_id:
                task = asyncio.create_task(self.request_vote_from_peer(peer_id, peer_port))
                vote_tasks.append(task)
        
        # Wait for vote responses
        if vote_tasks:
            responses = await asyncio.gather(*vote_tasks, return_exceptions=True)
            
            for response in responses:
                if isinstance(response, VoteResponse):
                    if response.vote_granted:
                        votes_received += 1
                    elif response.term > self.current_term:
                        # Found higher term, step down
                        await self.step_down(response.term)
                        return
        
        # Check if won election
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
        
        # Initialize leader state
        for peer_id, _ in self.peers:
            if peer_id != self.node_id:
                self.next_index[peer_id] = len(self.log)
                self.match_index[peer_id] = -1
        
        # Start sending heartbeats
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        self.heartbeat_task = asyncio.create_task(self.send_heartbeats())
    
    async def send_heartbeats(self):
        """Send periodic heartbeats to followers"""
        while self.state == NodeState.LEADER:
            try:
                # Send heartbeat to all peers
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
            
            # For heartbeat, send empty entries
            entries = []
            
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
        
        # Reply false if term < currentTerm
        if vote_request.term < self.current_term:
            pass
        else:
            # Update term if necessary
            if vote_request.term > self.current_term:
                await self.step_down(vote_request.term)
            
            # Check if we can grant vote
            last_log_index = len(self.log) - 1 if self.log else -1
            last_log_term = self.log[-1].term if self.log else 0
            
            log_ok = (vote_request.last_log_term > last_log_term or 
                     (vote_request.last_log_term == last_log_term and 
                      vote_request.last_log_index >= last_log_index))
            
            if (self.voted_for is None or self.voted_for == vote_request.candidate_id) and log_ok:
                vote_granted = True
                self.voted_for = vote_request.candidate_id
                self.last_heartbeat = time.time()  # Reset election timer
        
        self.logger.debug(f"Vote request from {vote_request.candidate_id}: granted={vote_granted}")
        return VoteResponse(term=self.current_term, vote_granted=vote_granted)
    
    async def handle_append_entries(self, append_request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle incoming append entries (heartbeat)"""
        success = False
        
        # Reply false if term < currentTerm
        if append_request.term < self.current_term:
            pass
        else:
            # Update term and step down if necessary
            if append_request.term > self.current_term:
                await self.step_down(append_request.term)
            
            # Valid leader for this term
            self.last_heartbeat = time.time()
            if self.state == NodeState.CANDIDATE:
                self.state = NodeState.FOLLOWER
            
            success = True
            self.logger.debug(f"Heartbeat from leader {append_request.leader_id}")
        
        return AppendEntriesResponse(term=self.current_term, success=success)
    
    async def handle_client_request(self, request_data: Dict) -> Dict:
        """Handle client request (only if leader)"""
        if self.state != NodeState.LEADER:
            return {'error': 'Not leader', 'leader_id': None}
        
        command = request_data.get('command', '')
        
        # Add to log (simplified - in real implementation would replicate to followers)
        log_entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command
        )
        self.log.append(log_entry)
        
        return {'success': True, 'index': log_entry.index}
    
    async def handle_status_request(self) -> Dict:
        """Handle status request"""
        return {
            'node_id': self.node_id,
            'state': self.state.value,
            'term': self.current_term,
            'voted_for': self.voted_for,
            'log_length': len(self.log),
            'commit_index': self.commit_index,
            'port': self.port
        }
    
    async def stop(self):
        """Stop the node"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        if self.election_task:
            self.election_task.cancel()

# Client utilities for testing
class RaftClient:
    """Simple client for interacting with Raft cluster"""
    
    @staticmethod
    async def send_client_request(host: str, port: int, command: str) -> Optional[Dict]:
        """Send a client request to a Raft node"""
        try:
            message = Message(
                msg_type=MessageType.CLIENT_REQUEST.value,
                data={'command': command},
                sender_id='client'
            )
            
            reader, writer = await asyncio.open_connection(host, port)
            try:
                await SocketProtocol.send_message(writer, message)
                response = await SocketProtocol.receive_message(reader)
                
                if response and response.msg_type == MessageType.CLIENT_RESPONSE.value:
                    return response.data
            finally:
                writer.close()
                await writer.wait_closed()
        
        except Exception as e:
            logging.error(f"Client request failed: {e}")
        
        return None
    
    @staticmethod
    async def get_node_status(host: str, port: int) -> Optional[Dict]:
        """Get status from a Raft node"""
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

async def create_single_node(port: int):
    """Create and start a single Raft node"""
    # Define the cluster topology
    ports = [8000, 8001, 8002, 8003, 8004]
    node_ids = ['node1', 'node2', 'node3', 'node4', 'node5']
    
    # Create peer list for the entire cluster
    peers = [(node_id, p) for node_id, p in zip(node_ids, ports)]
    
    # Find the node_id for this port
    try:
        port_index = ports.index(port)
        node_id = node_ids[port_index]
    except ValueError:
        raise ValueError(f"Port {port} not in allowed ports: {ports}")
    
    # Create and start the node
    node = RaftNode(node_id, port, peers)
    await node.start()
    
    return node

async def create_cluster():
    """Create and start a 5-node Raft cluster (for testing)"""
    nodes = []
    ports = [8000, 8001, 8002, 8003, 8004]
    node_ids = ['node1', 'node2', 'node3', 'node4', 'node5']
    
    # Create peer list
    peers = [(node_id, port) for node_id, port in zip(node_ids, ports)]
    
    # Create nodes
    for i, (node_id, port) in enumerate(zip(node_ids, ports)):
        node = RaftNode(node_id, port, peers)
        nodes.append(node)
    
    # Start all nodes
    start_tasks = [node.start() for node in nodes]
    await asyncio.gather(*start_tasks)
    
    return nodes

async def simulate_leader_failure(nodes: List[RaftNode]):
    """Simulate leader failure scenario"""
    print("\n=== Starting Leader Failure Simulation ===")
    
    # Wait for initial leader election
    await asyncio.sleep(5)
    
    # Find current leader
    leader = None
    for node in nodes:
        if node.state == NodeState.LEADER:
            leader = node
            break
    
    if leader:
        print(f"\nCurrent leader: {leader.node_id} (term {leader.current_term})")
        print("Simulating leader failure...")
        
        # Simulate leader failure by stopping heartbeats
        if leader.heartbeat_task:
            leader.heartbeat_task.cancel()
        leader.state = NodeState.FOLLOWER  # Force step down
        
        print("Leader failed! Waiting for new leader election...")
        
        # Wait for new leader election
        await asyncio.sleep(8)
        
        # Check new leader
        new_leader = None
        for node in nodes:
            if node.state == NodeState.LEADER:
                new_leader = node
                break
        
        if new_leader:
            print(f"New leader elected: {new_leader.node_id} (term {new_leader.current_term})")
        else:
            print("No new leader elected yet")
    else:
        print("No leader found initially")

async def print_cluster_status(nodes: List[RaftNode]):
    """Print status of all nodes"""
    print("\n=== Cluster Status ===")
    for node in nodes:
        print(f"{node.node_id}: state={node.state.value}, term={node.current_term}, "
              f"voted_for={node.voted_for}, port={node.port}")

async def test_client_operations():
    """Test client operations"""
    print("\n=== Testing Client Operations ===")
    
    # Try to find leader and send request
    ports = [8000, 8001, 8002, 8003, 8004]
    
    for port in ports:
        status = await RaftClient.get_node_status('localhost', port)
        if status and status.get('state') == 'leader':
            print(f"Found leader on port {port}")
            
            # Send client request
            result = await RaftClient.send_client_request('localhost', port, 'set x=100')
            if result:
                print(f"Client request result: {result}")
            break
    else:
        print("No leader found for client request")

async def run_single_node(port: int):
    """Run a single Raft node"""
    print(f"Starting Raft node on port {port}")
    print(f"Cluster ports: 8000, 8001, 8002, 8003, 8004")
    
    try:
        node = await create_single_node(port)
        print(f"Node {node.node_id} is running on port {port}")
        print("Waiting for other nodes to join the cluster...")
        print("Press Ctrl+C to stop this node")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\nShutting down node on port {port}...")
        await node.stop()
    except Exception as e:
        print(f"Error: {e}")

async def run_test_cluster():
    """Run full cluster for testing (original behavior)"""
    print("Starting 5-node Raft cluster with TCP sockets...")
    nodes = await create_cluster()
    
    try:
        # Monitor cluster for a while
        for i in range(3):
            await asyncio.sleep(3)
            await print_cluster_status(nodes)
        
        # Test client operations
        await test_client_operations()
        
        # Simulate leader failure
        await simulate_leader_failure(nodes)
        
        # Continue monitoring
        for i in range(3):
            await asyncio.sleep(3)
            await print_cluster_status(nodes)
        
        print("\nCluster is running. You can test with:")
        print("python -c \"")
        print("import asyncio")
        print("from raft_node import RaftClient")
        print("print(asyncio.run(RaftClient.get_node_status('localhost', 8000)))")
        print("\"")
        print("\nPress Ctrl+C to stop")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down cluster...")
        # Clean shutdown
        for node in nodes:
            await node.stop()

def print_usage():
    """Print usage instructions"""
    print("Raft Consensus Algorithm - Socket Implementation")
    print()
    print("Usage:")
    print("  python raft_node.py <port>        # Run single node on specified port")
    print("  python raft_node.py --test        # Run full cluster for testing")
    print()
    print("Single Node Mode:")
    print("  Available ports: 8000, 8001, 8002, 8003, 8004")
    print("  Example:")
    print("    Terminal 1: python raft_node.py 8000")
    print("    Terminal 2: python raft_node.py 8001")
    print("    Terminal 3: python raft_node.py 8002")
    print("    Terminal 4: python raft_node.py 8003")
    print("    Terminal 5: python raft_node.py 8004")
    print()
    print("Client Testing:")
    print("  python -c \"import asyncio; from raft_node import RaftClient; print(asyncio.run(RaftClient.get_node_status('localhost', 8000)))\"")

async def main():
    """Main function with command line argument parsing"""
    import sys
    
    if len(sys.argv) == 1:
        print_usage()
        return
    
    arg = sys.argv[1]
    
    if arg == "--test":
        await run_test_cluster()
    elif arg == "--help" or arg == "-h":
        print_usage()
    else:
        try:
            port = int(arg)
            if port not in [8000, 8001, 8002, 8003, 8004]:
                print(f"Error: Port must be one of: 8000, 8001, 8002, 8003, 8004")
                print("Use --help for usage information")
                return
            await run_single_node(port)
        except ValueError:
            print(f"Error: Invalid port '{arg}'. Must be a number.")
            print("Use --help for usage information")

if __name__ == "__main__":
    asyncio.run(main())
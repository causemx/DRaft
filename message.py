import json
import time
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from node_metadata import NodeMetadata


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
    sender: NodeMetadata  # Changed from sender_id(str) to NodeMetadata

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


class MessageTranslator:
    """
    TODO
    MessageTranslator provides utility functions to translate
    messages between the JSON that they are transmitted as over the network
    and Message objects
    """
    @staticmethod
    def message_to_json(message: Message) -> Optional[str]:
        try:
            # Convert Message to dictionary with NodeMetadata serialization
            message_dict = {
                'msg_type': message.msg_type,
                'data': message.data,
                'sender': {
                    'host': message.sender.get_host(),
                    'port': message.sender.get_port()
                }
            }
            
            return json.dumps(message_dict)
            
        except Exception as e:
            print(f"Error serializing message to JSON: {e}")
            return None
    
    @staticmethod
    def json_to_message(json_str: str) -> Optional[Message]:
        try:
            # Parse JSON string
            data = json.loads(json_str)
            
            # Reconstruct NodeMetadata from sender data
            sender_data = data.get('sender', {})
            sender = NodeMetadata(
                host=sender_data.get('host', 'unknown'),
                port=sender_data.get('port', 0)
            )
            
            # Create Message object
            return Message(
                msg_type=data.get('msg_type', ''),
                data=data.get('data', {}),
                sender=sender
            )
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"Error parsing JSON to message: {e}")
            return None
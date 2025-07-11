import time
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, List


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
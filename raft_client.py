#!/usr/bin/env python3
"""
Raft Client Module
Extracted from raft_node.py for better organization
"""

import asyncio
import json
import struct
from typing import Optional, Dict, List
from dataclasses import asdict

from message import Message, MessageType, SwarmCommand
from node_metadata import NodeMetadata
from util.logger_helper import LoggerFactory

logger = LoggerFactory.get_logger(__name__)


class RaftDroneClient:
    """Client for interacting with Raft-Drone cluster using NetworkComm protocol"""
    
    @staticmethod
    async def _send_message_with_length_prefix(writer: asyncio.StreamWriter, message: Message):
        """Send message using NetworkComm's length-prefixed protocol"""
        try:
            # Convert message to JSON (same as NetworkComm._send_message)
            json_data = json.dumps({
                'msg_type': message.msg_type,
                'data': message.data,
                'sender': {
                    'host': message.sender.get_host(),
                    'port': message.sender.get_port()
                }
            }).encode('utf-8')
            
            # Send length prefix + message
            length = struct.pack('!I', len(json_data))
            writer.write(length + json_data)
            await writer.drain()
        except Exception as e:
            logger.error(f"Error sending message: {e}")

    @staticmethod
    async def _receive_message_with_length_prefix(reader: asyncio.StreamReader) -> Optional[Message]:
        """Receive message using NetworkComm's length-prefixed protocol"""
        try:
            # Read length prefix
            length_data = await reader.readexactly(4)
            if not length_data:
                return None
            
            length = struct.unpack('!I', length_data)[0]
            
            # Read message data
            json_data = await reader.readexactly(length)
            data = json.loads(json_data.decode('utf-8'))
            
            # Reconstruct message
            sender_data = data['sender']
            sender = NodeMetadata(sender_data['host'], sender_data['port'])
            
            return Message(
                msg_type=data['msg_type'],
                data=data['data'],
                sender=sender
            )
        except (asyncio.IncompleteReadError, json.JSONDecodeError, struct.error) as e:
            logger.debug(f"Error receiving message: {e}")
            return None
    
    @staticmethod
    async def send_swarm_command(node_metadata: NodeMetadata, command_type: str, parameters: Dict = None) -> Optional[Dict]:
        """Send a swarm command to the cluster leader"""
        try:
            swarm_command = SwarmCommand(
                command_type=command_type,
                parameters=parameters or {}
            )
            
            # Create a dummy client NodeMetadata for sender
            client_metadata = NodeMetadata('client', 0)
            
            message = Message(
                msg_type=MessageType.SWARM_COMMAND.value,
                data=asdict(swarm_command),
                sender=client_metadata
            )
            
            reader, writer = await asyncio.open_connection(node_metadata.get_host(), node_metadata.get_port())
            try:
                # Use NetworkComm's protocol instead of SocketProtocol
                await RaftDroneClient._send_message_with_length_prefix(writer, message)
                response = await RaftDroneClient._receive_message_with_length_prefix(reader)
                
                if response and response.msg_type == MessageType.SWARM_RESPONSE.value:
                    return response.data
            finally:
                writer.close()
                await writer.wait_closed()
        
        except Exception as e:
            logger.error(f"Swarm command failed: {e}")
        
        return None
    
    @staticmethod
    async def get_node_status(node_metadata: NodeMetadata) -> Optional[Dict]:
        """Get status from a Raft-Drone node"""
        try:
            client_metadata = NodeMetadata('client', 0)
            
            message = Message(
                msg_type=MessageType.STATUS_REQUEST.value,
                data={},
                sender=client_metadata
            )
            
            reader, writer = await asyncio.open_connection(node_metadata.get_host(), node_metadata.get_port())
            try:
                # Use NetworkComm's protocol instead of SocketProtocol
                await RaftDroneClient._send_message_with_length_prefix(writer, message)
                response = await RaftDroneClient._receive_message_with_length_prefix(reader)
                
                if response and response.msg_type == MessageType.STATUS_RESPONSE.value:
                    return response.data
            finally:
                writer.close()
                await writer.wait_closed()
        
        except Exception as e:
            logger.debug(f"Status request failed: {e}")
        
        return None
    
    @staticmethod
    async def find_leader(node_metadatas: List[NodeMetadata]) -> Optional[NodeMetadata]:
        """Find the current leader in the cluster"""
        for node_metadata in node_metadatas:
            status = await RaftDroneClient.get_node_status(node_metadata)
            if status and status.get('state') == 'leader':
                return node_metadata
        return None
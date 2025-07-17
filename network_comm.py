"""
NetworkComm.py provides a network communication layer using asyncio
that abstracts the details of network communication to other nodes
The NetworkComm class uses async/await patterns for non-blocking I/O
and interacts with the message queue to exchange data with the rest of the
system
"""

import struct
import json
import asyncio
import queue
from typing import Callable, Optional, List
from concurrent.futures import ThreadPoolExecutor

from util.logger_helper import LoggerFactory
from message import Message, MessageTranslator
from net import network_utils
from node_metadata import NodeMetadata

LOG = LoggerFactory.get_logger(__name__)


class NetworkComm:
    """Simplified NetworkComm that supports both queue-based and direct request-response"""

    def __init__(self, nodes: List[NodeMetadata], port: int, send_timeout: float = None):
        self._nodes: List[NodeMetadata] = nodes
        self._port: int = port
        self._server: Optional[asyncio.Server] = None
        self._send_timeout: float = send_timeout or 5.0
        self._running = False
        
        # Keep the original message queue for compatibility
        self._send_queue = queue.Queue()
        self._recv_queue = queue.Queue()
        self._sender_task: Optional[asyncio.Task] = None
        self._executor = ThreadPoolExecutor(max_workers=2)
        
        # Handler for incoming connections (Raft integration)
        self._connection_handler: Optional[Callable] = None

    async def run(self):
        """Run the network comm layer"""
        LOG.info("Starting simple network communication layer")
        self._running = True
        
        # Start the TCP server
        await self._run_server()
        
        # Start the sender task for queue-based messages
        self._sender_task = asyncio.create_task(self._process_sender_loop())

    async def stop(self):
        """Stop the network comm layer"""
        LOG.info("Stopping network communication layer")
        self._running = False
        
        if self._sender_task:
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass
        
        await self._shutdown_server()
        self._executor.shutdown(wait=True)

    async def _run_server(self):
        """Start the TCP server"""
        LOG.info(f"Starting TCP server on port {self._port}")
        try:
            self._server = await asyncio.start_server(
                self._handle_client,
                '',  # Listen on all interfaces
                self._port
            )
            LOG.info("Server started successfully")
        except Exception as e:
            LOG.error(f"Failed to start server: {e}")
            raise

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming client connections"""
        client_addr = writer.get_extra_info('peername')
        LOG.debug(f"New connection from {client_addr}")
        
        try:
            if self._connection_handler:
                # Use Raft's connection handler (handles request-response)
                await self._connection_handler(reader, writer)
            else:
                # Default behavior: single message handling
                message = await self._receive_message(reader)
                if message:
                    self._recv_queue.put_nowait(message)
        except Exception as e:
            LOG.debug(f"Connection error with {client_addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    def set_connection_handler(self, handler: Callable):
        """Set custom connection handler (for Raft integration)"""
        self._connection_handler = handler

    async def _shutdown_server(self):
        """Shutdown the TCP server"""
        if self._server:
            LOG.info("Shutting down server")
            self._server.close()
            await self._server.wait_closed()

    async def _process_sender_loop(self):
        """Process messages from send queue (original functionality)"""
        LOG.info("Starting sender loop")
        while self._running:
            try:
                await self._send_messages_in_queue()
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                LOG.info("Sender loop cancelled")
                break
            except Exception as e:
                LOG.error(f"Error in sender loop: {e}")
                await asyncio.sleep(1)

    async def _send_messages_in_queue(self):
        """Send all messages currently in the send queue"""
        while not self._send_queue.empty():
            try:
                msg = self._send_queue.get_nowait()
                if msg:
                    data = await asyncio.get_event_loop().run_in_executor(
                        self._executor,
                        MessageTranslator.message_to_json,
                        msg
                    )
                    if data:
                        LOG.debug(f"Sending queued data: {data}")
                        await self._send_data_to_all_nodes(data)
            except queue.Empty:
                break

    async def _send_data_to_all_nodes(self, data: str):
        """Send data to all nodes in the cluster"""
        tasks = []
        for node in self._nodes:
            if not self.is_node_me(node):
                task = asyncio.create_task(self._send_raw_data_to_node(data, node))
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_raw_data_to_node(self, data: str, node: NodeMetadata):
        """Send raw data to a specific node (original implementation)"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(node.get_host(), node.get_port()),
                timeout=self._send_timeout
            )
            
            writer.write(bytes(data + "\n", "utf-8"))
            await writer.drain()
            
            writer.close()
            await writer.wait_closed()
            
        except asyncio.TimeoutError:
            LOG.debug(f"Timeout connecting to {node.get_host()}:{node.get_port()}")
        except ConnectionRefusedError as e:
            LOG.debug(f"Failed to connect to {node.get_host()}:{node.get_port()} - {e}")
        except Exception as e:
            LOG.debug(f"Failed to send data to {node.get_host()}:{node.get_port()} - {e}")

    # NEW: Direct request-response methods for Raft
    async def send_message_with_response(self, target_node: NodeMetadata, message: Message, timeout: float = 5.0) -> Optional[Message]:
        """Send message and wait for response (for Raft request-response pattern)"""
        if self.is_node_me(target_node):
            raise ValueError("Cannot send message to myself!")
        
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(target_node.get_host(), target_node.get_port()),
                timeout=timeout
            )
            
            try:
                # Send message using length-prefixed protocol
                await self._send_message(writer, message)
                
                # Wait for response
                response = await asyncio.wait_for(
                    self._receive_message(reader),
                    timeout=timeout
                )
                return response
                
            finally:
                writer.close()
                await writer.wait_closed()
                
        except asyncio.TimeoutError:
            LOG.debug(f"Timeout sending message to {target_node.get_host()}:{target_node.get_port()}")
            return None
        except Exception as e:
            LOG.debug(f"Failed to send message to {target_node.get_host()}:{target_node.get_port()}: {e}")
            return None

    async def _send_message(self, writer: asyncio.StreamWriter, message: Message):
        """Send a message using length-prefixed protocol"""
        try:
            # Serialize message to JSON
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
            LOG.error(f"Error sending message: {e}")

    async def _receive_message(self, reader: asyncio.StreamReader) -> Optional[Message]:
        """Receive a message using length-prefixed protocol"""
        try:
            # Read length prefix
            length_data = await reader.readexactly(4)
            if not length_data:
                return None
            
            length = struct.unpack('!I', length_data)[0]
            
            # Read message data
            json_data = await reader.readexactly(length)
            data = json.loads(json_data.decode('utf-8'))
            
            # Reconstruct NodeMetadata from sender data
            sender_data = data['sender']
            sender = NodeMetadata(sender_data['host'], sender_data['port'])
            
            return Message(
                msg_type=data['msg_type'],
                data=data['data'],
                sender=sender
            )
        except (asyncio.IncompleteReadError, json.JSONDecodeError, struct.error) as e:
            LOG.debug(f"Error receiving message: {e}")
            return None

    def is_node_me(self, node: NodeMetadata) -> bool:
        """Check if the given node represents this instance"""
        return network_utils.is_ippaddr_localhost(node.get_host()) and node.get_port() == self._port

    # Original queue-based methods (backward compatibility)
    def enqueue_message_for_sending(self, message: Message):
        """Add a message to the send queue"""
        try:
            self._send_queue.put_nowait(message)
        except queue.Full:
            LOG.warning("Send queue is full, dropping message")

    def get_received_message(self) -> Optional[Message]:
        """Get a received message from the queue"""
        try:
            return self._recv_queue.get_nowait()
        except queue.Empty:
            return None

    def has_received_messages(self) -> bool:
        """Check if there are received messages in the queue"""
        return not self._recv_queue.empty()
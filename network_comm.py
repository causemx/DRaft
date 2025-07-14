"""
NetworkComm.py provides a network communication layer using asyncio
that abstracts the details of network communication to other nodes
The NetworkComm class uses async/await patterns for non-blocking I/O
and interacts with the message queue to exchange data with the rest of the
system
"""

import asyncio
import logging
import queue
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

from util.logger_helper import get_logger
from message import Message, MessageTranslator
from net.network_utils import NetworkUtil
from node_metadata import NodeMetadata

LOG = get_logger(__name__)
LOG.setLevel(logging.ERROR)

class MessageQueue:
    """Thread-safe message queue implementation using queue.Queue"""
    
    def __init__(self, maxsize: int = 0):
        self._send_queue = queue.Queue(maxsize=maxsize)
        self._recv_queue = queue.Queue(maxsize=maxsize)
    
    def send_enqueue(self, message: Message):
        """Add message to send queue"""
        try:
            self._send_queue.put_nowait(message)
        except queue.Full:
            LOG.warning("Send queue is full, dropping message")
    
    def send_dequeue(self) -> Optional[Message]:
        """Get message from send queue (non-blocking)"""
        try:
            return self._send_queue.get_nowait()
        except queue.Empty:
            return None
    
    def send_empty(self) -> bool:
        """Check if send queue is empty"""
        return self._send_queue.empty()
    
    def recv_enqueue(self, message: Message):
        """Add message to receive queue"""
        try:
            self._recv_queue.put_nowait(message)
        except queue.Full:
            LOG.warning("Receive queue is full, dropping message")
    
    def recv_dequeue(self) -> Optional[Message]:
        """Get message from receive queue (non-blocking)"""
        try:
            return self._recv_queue.get_nowait()
        except queue.Empty:
            return None
    
    def recv_empty(self) -> bool:
        """Check if receive queue is empty"""
        return self._recv_queue.empty()

class NetworkComm:

    def __init__(self, nodes: List[NodeMetadata], port: int, send_timeout: float = None):
        self._nodes: List[NodeMetadata] = nodes
        self._port: int = port
        self._server: Optional[asyncio.Server] = None
        self._send_timeout: float = send_timeout or 5.0
        self._message_queue = MessageQueue()
        self._running = False
        self._sender_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._executor = ThreadPoolExecutor(max_workers=2)

    async def run(self):
        """Run the network comm layer - starts server and sender tasks"""
        LOG.info("Starting asyncio network communication layer")
        self._running = True
        self._loop = asyncio.get_event_loop()
        
        # Start the TCP server
        await self._run_server()
        
        # Start the sender task
        self._sender_task = asyncio.create_task(self._process_sender_loop())

    async def stop(self):
        """Completely shut down and kill the network comm layer"""
        LOG.info("Stopping network communication layer")
        self._running = False
        
        # Stop sender task
        if self._sender_task:
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass
        
        # Stop server
        await self._shutdown_server()
        
        # Shutdown executor
        self._executor.shutdown(wait=True)

    async def _run_server(self):
        """Start the TCP server"""
        LOG.info("Starting TCP server on port {}".format(self._port))
        try:
            self._server = await asyncio.start_server(
                self._handle_client,
                '',  # Listen on all interfaces
                self._port
            )
            LOG.info("Server started successfully")
        except Exception as e:
            LOG.error("Failed to start server: {}".format(e))
            raise

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming client connections"""
        client_addr = writer.get_extra_info('peername')
        LOG.info("New connection from {}".format(client_addr))
        
        try:
            # Read data from client
            data = await reader.read(1024)
            if data:
                message_str = data.decode('utf-8').strip()
                LOG.info("Received data {} from {}".format(message_str, client_addr))
                
                # Parse message in thread pool to avoid blocking
                message = await self._loop.run_in_executor(
                    self._executor,
                    MessageTranslator.json_to_message,
                    message_str
                )
                
                if message is not None:
                    self._message_queue.recv_enqueue(message)
                    
        except Exception as e:
            LOG.error("Error handling client {}: {}".format(client_addr, e))
        finally:
            writer.close()
            await writer.wait_closed()

    async def _shutdown_server(self):
        """Shutdown the TCP server"""
        if self._server:
            LOG.info("Shutting down server")
            self._server.close()
            await self._server.wait_closed()

    async def _process_sender_loop(self):
        """Process messages from send queue"""
        LOG.info("Starting sender loop")
        while self._running:
            try:
                await self._send_messages_in_queue()
                await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
            except asyncio.CancelledError:
                LOG.info("Sender loop cancelled")
                break
            except Exception as e:
                LOG.error("Error in sender loop: {}".format(e))
                await asyncio.sleep(1)  # Wait before retrying

    async def _send_messages_in_queue(self):
        """Send all messages currently in the send queue"""
        while not self._message_queue.send_empty():
            msg = self._message_queue.send_dequeue()
            if msg is not None:
                # Convert message to JSON in thread pool
                data = await self._loop.run_in_executor(
                    self._executor,
                    MessageTranslator.message_to_json,
                    msg
                )
                if data is not None:
                    LOG.info("Sending data {}".format(data))
                    await self._send_data_to_all_nodes(data)

    async def _send_data_to_all_nodes(self, data: str):
        """Send data to all nodes in the cluster"""
        tasks = []
        for node in self._nodes:
            if not self.is_node_me(node):
                task = asyncio.create_task(self._send_data_to_node(data, node))
                tasks.append(task)
        
        if tasks:
            # Wait for all sends to complete
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_data_to_node(self, data: str, node: NodeMetadata):
        """Send data to a specific node"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(node.get_host(), node.get_port()),
                timeout=self._send_timeout
            )
            
            # Send data
            writer.write(bytes(data + "\n", "utf-8"))
            await writer.drain()
            
            # Close connection
            writer.close()
            await writer.wait_closed()
            
        except asyncio.TimeoutError:
            LOG.info("Timeout connecting to {}:{}".format(node.get_host(), node.get_port()))
        except ConnectionRefusedError as e:
            LOG.info("Failed to connect to {}:{} - {}".format(node.get_host(), node.get_port(), e))
        except Exception as e:
            LOG.info("Failed to send data to {}:{} - {}".format(node.get_host(), node.get_port(), e))

    def is_node_me(self, node: NodeMetadata) -> bool:
        """Check if the given node represents this instance"""
        return NetworkUtil.is_ippaddr_localhost(node.get_host()) and node.get_port() == self._port

    async def send_data(self, data: Message, recipient: NodeMetadata):
        """Send data to a specific recipient"""
        if self.is_node_me(recipient):
            raise ValueError("Cannot send data to myself!")
        
        # Convert message to JSON in thread pool
        msg_data = await self._loop.run_in_executor(
            self._executor,
            MessageTranslator.message_to_json,
            data
        )
        
        if msg_data is not None:
            await self._send_data_to_node(msg_data, recipient)

    def enqueue_message_for_sending(self, message: Message):
        """Add a message to the send queue"""
        self._message_queue.send_enqueue(message)

    def get_received_message(self) -> Optional[Message]:
        """Get a received message from the queue"""
        return self._message_queue.recv_dequeue()

    def has_received_messages(self) -> bool:
        """Check if there are received messages in the queue"""
        return not self._message_queue.recv_empty()

    @property
    def message_queue(self) -> MessageQueue:
        """Get the message queue instance"""
        return self._message_queue

import json
import asyncio
import socket
import queue
import threading
from dataclasses import dataclass
from typing import Optional, List
from message import Message
from concurrent.futures import ThreadPoolExecutor
from util.logger_helper import LoggerFactory

logger = LoggerFactory.get_logger("network_comm")

@dataclass
class NodeMetadata:
    host: str
    port: int

class MessageQueue:
    def __init__(self):
        self.send_queue: queue.Queue = queue.Queue()
        self.recv_queue: queue.Queue = queue.Queue()
    
    def send_enqueue(self, message: Message):
        try:
            self.send_queue.put(message)
        except queue.Full:
            logger.warning("Send queue is full")
    
    def send_dequeue(self) -> Optional[Message]:
        try:
            self.send_queue.get_nowait()
        except queue.Empty:
            logger.warning("Send queue is empty")
    
    def send_empty(self) -> bool:
        """Check if send queue is empty"""
        return self._send_queue.empty()
    
    def recv_enqueue(self, message: Message):
        try:
            self._recv_queue.put_nowait(message)
        except queue.Full:
            logger.warning("Receive queue is full, dropping message")
    
    def recv_dequeue(self) -> Optional[Message]:
        try:
            return self._recv_queue.get_nowait()
        except queue.Empty:
            return None
    
    def recv_empty(self) -> bool:
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
        logger.info("Start communication layer work")
        self._running = True
        self._loop = asyncio.get_event_loop()
        await self.run_server()
    
    async def run_server(self):
        await asyncio.start_server(
            self.handle_client,
            '',
            self._port
        )
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client_addr = writer.get_extra_info("peername")
        logger.info(f"New connection from: {client_addr}")
        try:
            # Read data from client
            json_data = await reader.read(1024)
            data = json.loads(json_data.decode('utf-8'))
            
            message = Message(
                msg_type=data['msg_type'],
                data=data['data'],
                sender_id=data['sender_id']
            )

            if message is not None:
                self._message_queue.recv_enqueue(message)

        except Exception as e:
            logger.error(f"Error handling client: {client_addr}, {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def stop(self):
        pass

async def main():
    nodes = [
        NodeMetadata("localhost", 8001),
        NodeMetadata("localhost", 8002),
        NodeMetadata("localhost", 8003)
    ]
    
    comm = NetworkComm(nodes, 8001)
    
    try:
        await comm.run()
        # Keep running
        await asyncio.sleep(10)
    finally:
        await comm.stop()    
    
if __name__ == "__main__":
    asyncio.run(main())
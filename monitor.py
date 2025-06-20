#!/usr/bin/env python3
"""
Raft Cluster Monitor - Interactive command-line tool for monitoring and controlling Raft nodes
Usage: python monitor.py
"""

import asyncio
import cmd
import json
import os
import signal
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple
import psutil

# Import from raft_node.py
try:
    from raft_node import RaftClient
except ImportError:
    print("Error: Cannot import RaftClient from raft_node.py")
    print("Make sure raft_node.py is in the same directory")
    sys.exit(1)

class RaftMonitor(cmd.Cmd):
    """Interactive Raft cluster monitor and controller"""
    
    intro = """
====================================================================
                     Raft Cluster Monitor                        
                                                                
  Monitor and control your Raft consensus cluster              
  Type 'help' for available commands                           
====================================================================
"""
    
    prompt = '(raft-monitor) '
    
    def __init__(self):
        super().__init__()
        self.cluster_ports = [8000, 8001, 8002, 8003, 8004]
        self.node_names = ['node1', 'node2', 'node3', 'node4', 'node5']
        self.node_processes: Dict[int, subprocess.Popen] = {}
    
    def _print_separator(self):
        """Print a separator line"""
        print("-" * 70)
    
    async def _get_node_status(self, port: int) -> Optional[Dict]:
        """Get status of a single node"""
        try:
            return await RaftClient.get_node_status('localhost', port)
        except Exception:
            return None
    
    async def _get_cluster_status(self) -> List[Tuple[int, Optional[Dict]]]:
        """Get status of all nodes in the cluster"""
        tasks = [self._get_node_status(port) for port in self.cluster_ports]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        cluster_status = []
        for port, result in zip(self.cluster_ports, results):
            if isinstance(result, Exception):
                cluster_status.append((port, None))
            else:
                cluster_status.append((port, result))
        
        return cluster_status
    
    def _find_process_by_port(self, port: int) -> Optional[psutil.Process]:
        """Find process using a specific port"""
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                for conn in proc.net_connections():
                    if conn.laddr.port == port and conn.status == 'LISTEN':
                        return proc
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return None
    
    def do_status(self, args):
        """Show current status of all nodes in the cluster
        Usage: status [--json] [--watch <seconds>]
        
        Options:
          --json     Output in JSON format
          --watch N  Continuously monitor every N seconds
        """
        parts = args.split()
        json_output = '--json' in parts
        watch_mode = False
        watch_interval = 1
        
        # Parse watch argument
        if '--watch' in parts:
            watch_mode = True
            try:
                watch_idx = parts.index('--watch')
                if watch_idx + 1 < len(parts):
                    watch_interval = int(parts[watch_idx + 1])
            except (ValueError, IndexError):
                pass
        
        async def show_status():
            while True:
                if not json_output and not watch_mode:
                    os.system('clear' if os.name == 'posix' else 'cls')
                
                cluster_status = await self._get_cluster_status()
                
                if json_output:
                    status_data = {}
                    for port, status in cluster_status:
                        status_data[f"node_{port}"] = status
                    print(json.dumps(status_data, indent=2))
                else:
                    self._print_status_table(cluster_status)
                
                if not watch_mode:
                    break
                
                print(f"\nNext update in {watch_interval} seconds... (Press Ctrl+C to stop)")
                try:
                    await asyncio.sleep(watch_interval)
                except KeyboardInterrupt:
                    break
        
        try:
            asyncio.run(show_status())
        except KeyboardInterrupt:
            if watch_mode:
                print("\nWatch mode stopped.")
    
    def _print_status_table(self, cluster_status: List[Tuple[int, Optional[Dict]]]):
        """Print status in a formatted table"""
        print(f"\nRaft Cluster Status - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        self._print_separator()
        
        # Header
        print(f"{'Port':<6} {'Node ID':<8} {'State':<12} {'Term':<6} {'Voted For':<10} {'Log':<5} {'Process':<8}")
        self._print_separator()
        
        leader_count = 0
        active_nodes = 0
        
        for port, status in cluster_status:
            if status:
                active_nodes += 1
                state = status['state'].upper()
                term = status['term']
                voted_for = str(status.get('voted_for', 'None'))
                log_len = status.get('log_length', 0)
                node_id = status['node_id']
                
                # Check if process is running
                process = self._find_process_by_port(port)
                proc_status = "Running" if process else "Unknown"
                
                # Count leaders
                if status['state'] == 'leader':
                    leader_count += 1
                
                print(f"{port:<6} {node_id:<8} {state:<12} {term:<6} {voted_for:<10} {log_len:<5} {proc_status:<8}")
            else:
                # Node not responding
                process = self._find_process_by_port(port)
                proc_status = "Running" if process else "Stopped"
                node_id = f"node{self.cluster_ports.index(port) + 1}"
                
                print(f"{port:<6} {node_id:<8} {'OFFLINE':<12} {'N/A':<6} {'N/A':<10} {'N/A':<5} {proc_status:<8}")
        
        self._print_separator()
        
        # Summary
        print(f"Active Nodes: {active_nodes}/5")
        if leader_count == 0:
            print("WARNING: No leader found!")
        elif leader_count == 1:
            print("Status: Healthy cluster - 1 leader")
        else:
            print(f"ERROR: Split brain detected - {leader_count} leaders!")
        
        print()
    
    def do_start(self, args):
        """Start a Raft node
        Usage: start <port> [--bg]
        
        Options:
          --bg    Run in background (detached)
        
        Examples:
          start 8000
          start 8001 --bg
        """
        parts = args.split()
        if not parts:
            print("Error: Please specify a port number")
            return
        
        try:
            port = int(parts[0])
            if port not in self.cluster_ports:
                print(f"Error: Port must be one of {self.cluster_ports}")
                return
        except ValueError:
            print("Error: Invalid port number")
            return
        
        background = '--bg' in parts
        
        # Check if already running
        if self._find_process_by_port(port):
            print(f"Node on port {port} is already running")
            return
        
        try:
            if background:
                # Start in background
                process = subprocess.Popen([
                    sys.executable, 'raft_node.py', str(port)
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.node_processes[port] = process
                print(f"Started node on port {port} in background (PID: {process.pid})")
            else:
                # Start in foreground (will block)
                print(f"Starting node on port {port}... (Press Ctrl+C to return to monitor)")
                try:
                    subprocess.run([sys.executable, 'raft_node.py', str(port)])
                except KeyboardInterrupt:
                    print(f"\nReturned to monitor")
        
        except FileNotFoundError:
            print("Error: raft_node.py not found in current directory")
        except Exception as e:
            print(f"Error starting node: {e}")
    
    def do_stop(self, args):
        """Stop a Raft node gracefully
        Usage: stop <port|all>
        
        Examples:
          stop 8000
          stop all
        """
        if not args:
            print("Error: Please specify a port number or 'all'")
            return
        
        if args.strip() == 'all':
            ports_to_stop = self.cluster_ports
        else:
            try:
                port = int(args.strip())
                if port not in self.cluster_ports:
                    print(f"Error: Port must be one of {self.cluster_ports}")
                    return
                ports_to_stop = [port]
            except ValueError:
                print("Error: Invalid port number")
                return
        
        for port in ports_to_stop:
            process = self._find_process_by_port(port)
            if process:
                try:
                    process.terminate()
                    print(f"Sent SIGTERM to node on port {port} (PID: {process.pid})")
                    # Wait a bit for graceful shutdown
                    try:
                        process.wait(timeout=3)
                        print(f"Node on port {port} stopped gracefully")
                    except psutil.TimeoutExpired:
                        print(f"Node on port {port} didn't stop gracefully, forcing...")
                        process.kill()
                except psutil.NoSuchProcess:
                    print(f"Process for port {port} no longer exists")
                except psutil.AccessDenied:
                    print(f"Permission denied stopping process on port {port}")
            else:
                print(f"No process found listening on port {port}")
    
    def do_kill(self, args):
        """Forcefully kill a Raft node (simulates crash)
        Usage: kill <port|all>
        
        Examples:
          kill 8000
          kill all
        """
        if not args:
            print("Error: Please specify a port number or 'all'")
            return
        
        if args.strip() == 'all':
            ports_to_kill = self.cluster_ports
        else:
            try:
                port = int(args.strip())
                if port not in self.cluster_ports:
                    print(f"Error: Port must be one of {self.cluster_ports}")
                    return
                ports_to_kill = [port]
            except ValueError:
                print("Error: Invalid port number")
                return
        
        for port in ports_to_kill:
            process = self._find_process_by_port(port)
            if process:
                try:
                    process.kill()
                    print(f"Killed node on port {port} (PID: {process.pid})")
                except psutil.NoSuchProcess:
                    print(f"Process for port {port} no longer exists")
                except psutil.AccessDenied:
                    print(f"Permission denied killing process on port {port}")
            else:
                print(f"No process found listening on port {port}")
    
    def do_send(self, args):
        """Send a command to the leader
        Usage: send <command>
        
        Examples:
          send "set x=100"
          send "get x"
        """
        if not args:
            print("Error: Please specify a command")
            return
        
        async def send_command():
            # Find leader
            cluster_status = await self._get_cluster_status()
            leader_port = None
            
            for port, status in cluster_status:
                if status and status.get('state') == 'leader':
                    leader_port = port
                    break
            
            if not leader_port:
                print("Error: No leader found in the cluster")
                return
            
            print(f"Sending command to leader on port {leader_port}...")
            result = await RaftClient.send_client_request('localhost', leader_port, args)
            
            if result:
                if result.get('success'):
                    print("Command executed successfully")
                    print(f"Index: {result.get('index')}")
                else:
                    print(f"Command failed: {result.get('error', 'Unknown error')}")
            else:
                print("No response from leader")
        
        try:
            asyncio.run(send_command())
        except Exception as e:
            print(f"Error: {e}")
    
    def do_leader(self, args):
        """Find and show current leader information"""
        async def find_leader():
            cluster_status = await self._get_cluster_status()
            leader_found = False
            
            for port, status in cluster_status:
                if status and status.get('state') == 'leader':
                    leader_found = True
                    print(f"Current leader: {status['node_id']} on port {port}")
                    print(f"Term: {status['term']}")
                    print(f"Log entries: {status.get('log_length', 0)}")
                    break
            
            if not leader_found:
                print("No leader found in the cluster")
        
        try:
            asyncio.run(find_leader())
        except Exception as e:
            print(f"Error: {e}")
    
    def do_logs(self, args):
        """Show log information for all nodes"""
        async def show_logs():
            cluster_status = await self._get_cluster_status()
            
            print(f"\nNode Log Information")
            self._print_separator()
            
            for port, status in cluster_status:
                if status:
                    node_id = status['node_id']
                    log_len = status.get('log_length', 0)
                    commit_idx = status.get('commit_index', 0)
                    state = status['state']
                    
                    print(f"{node_id} (port {port}): {log_len} entries, committed: {commit_idx} [{state.upper()}]")
                else:
                    node_id = f"node{self.cluster_ports.index(port) + 1}"
                    print(f"{node_id} (port {port}): OFFLINE")
        
        try:
            asyncio.run(show_logs())
        except Exception as e:
            print(f"Error: {e}")
    
    def do_scenario(self, args):
        """Run predefined test scenarios
        Usage: scenario <name>
        
        Available scenarios:
          basic       - Start all nodes and show initial election
          failure     - Simulate leader failure
          partition   - Simulate network partition (3-2 split)
          recovery    - Test node recovery after failure
        """
        if not args:
            print("Available scenarios: basic, failure, partition, recovery")
            return
        
        scenario = args.strip().lower()
        
        if scenario == 'basic':
            self._run_basic_scenario()
        elif scenario == 'failure':
            self._run_failure_scenario()
        elif scenario == 'partition':
            self._run_partition_scenario()
        elif scenario == 'recovery':
            self._run_recovery_scenario()
        else:
            print(f"Unknown scenario: {scenario}")
            print("Available scenarios: basic, failure, partition, recovery")
    
    def _run_basic_scenario(self):
        """Run basic cluster startup scenario"""
        print("Running basic scenario: Starting all nodes")
        print("Starting nodes in background...")
        
        for port in self.cluster_ports:
            if not self._find_process_by_port(port):
                self.onecmd(f"start {port} --bg")
                time.sleep(0.5)  # Small delay between starts
        
        print("\nWaiting for leader election...")
        time.sleep(3)
        self.onecmd("status")
        self.onecmd("leader")
    
    def _run_failure_scenario(self):
        """Run leader failure scenario"""
        print("Running failure scenario: Leader failure and re-election")
        
        # Find current leader
        async def get_leader():
            cluster_status = await self._get_cluster_status()
            for port, status in cluster_status:
                if status and status.get('state') == 'leader':
                    return port, status['node_id']
            return None, None
        
        try:
            leader_port, leader_id = asyncio.run(get_leader())
            if leader_port:
                print(f"Current leader: {leader_id} on port {leader_port}")
                print("Killing the leader...")
                self.onecmd(f"kill {leader_port}")
                
                print("Waiting for re-election...")
                time.sleep(5)
                self.onecmd("status")
                self.onecmd("leader")
            else:
                print("No leader found to kill")
        except Exception as e:
            print(f"Error: {e}")
    
    def _run_partition_scenario(self):
        """Run network partition scenario"""
        print("Running partition scenario: 3-2 network split")
        print("Killing 2 nodes to simulate partition...")
        
        # Kill last 2 nodes
        self.onecmd("kill 8003")
        self.onecmd("kill 8004")
        
        print("Remaining 3 nodes should maintain/elect leader...")
        time.sleep(3)
        self.onecmd("status")
    
    def _run_recovery_scenario(self):
        """Run node recovery scenario"""
        print("Running recovery scenario: Node restart and rejoin")
        
        # Kill one node, then restart it
        print("Killing node on port 8001...")
        self.onecmd("kill 8001")
        
        print("Waiting...")
        time.sleep(2)
        self.onecmd("status")
        
        print("Restarting node...")
        self.onecmd("start 8001 --bg")
        
        print("Waiting for node to rejoin...")
        time.sleep(3)
        self.onecmd("status")
    
    def do_clear(self, args):
        """Clear the screen"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def do_exit(self, args):
        """Exit the monitor"""
        print("Goodbye!")
        return True
    
    def do_quit(self, args):
        """Exit the monitor"""
        return self.do_exit(args)
    
    def do_EOF(self, args):
        """Handle Ctrl+D"""
        print()
        return self.do_exit(args)
    
    def emptyline(self):
        """Do nothing on empty line"""
        pass
    
    def default(self, line):
        """Handle unknown commands"""
        print(f"Unknown command: {line}")
        print("Type 'help' for available commands")

def main():
    """Main function"""
    try:
        monitor = RaftMonitor()
        monitor.cmdloop()
    except KeyboardInterrupt:
        print("\nGoodbye!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
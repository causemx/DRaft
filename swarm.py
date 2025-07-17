#!/usr/bin/env python3
"""
Swarm Control CLI - Command line interface for controlling the Raft-Drone swarm
Enhanced with leader-centered formation system and NodeMetadata support
"""

import asyncio
import click
import json
from typing import Optional, Dict, List

# Import from raft_node.py and config_manager.py
try:
    from raft_node import RaftDroneClient, SwarmCommandType, Message, MessageType
    from confs.config_manager import get_config_manager
    from node_metadata import NodeMetadata
except ImportError:
    print("Error: Cannot import required modules")
    print("Make sure raft_node.py, config_manager.py, and node_metadata.py are in the same directory")
    exit(1)

class SwarmCLI:
    def __init__(self, host: str = 'localhost'):
        # Load configuration
        self.config_manager = get_config_manager()
        self.host = host
        self.cluster_ports = self.config_manager.get_all_ports()
        self.cluster_nodes = [NodeMetadata(host, port) for port in self.cluster_ports]
        self.current_leader = None
        
        print(f"Loaded cluster configuration: {self.config_manager.get_drone_count()} nodes")
        print(f"Cluster nodes: {[f'{host}:{port}' for port in self.cluster_ports]}")
    
    async def find_leader(self) -> bool:
        """Find the current leader in the cluster"""
        leader_node = await RaftDroneClient.find_leader(self.cluster_nodes)
        if leader_node:
            self.current_leader = leader_node
            return True
        return False
    
    async def send_command(self, command_type: str, parameters: Dict = None) -> Optional[Dict]:
        """Send command to the leader"""
        if not self.current_leader:
            if not await self.find_leader():
                click.echo("ERROR: No leader found in cluster")
                return None
        
        result = await RaftDroneClient.send_swarm_command(
            self.current_leader, 
            command_type, 
            parameters or {}
        )
        
        # If command failed and it's a "not leader" error, try to find new leader
        if result and result.get('error') == 'Not leader':
            if await self.find_leader():
                result = await RaftDroneClient.send_swarm_command(
                    self.current_leader, 
                    command_type, 
                    parameters or {}
                )
        
        return result
    
    async def get_cluster_status(self) -> List[Dict]:
        """Get status of all nodes in the cluster"""
        statuses = []
        for node_metadata in self.cluster_nodes:
            status = await RaftDroneClient.get_node_status(node_metadata)
            statuses.append({
                'node_metadata': node_metadata,
                'status': status
            })
        return statuses

# Create global CLI instance
swarm_cli = None

@click.group()
@click.option('--host', default='localhost', help='Host address for cluster nodes')
@click.pass_context
def cli(ctx, host):
    """Raft-Drone Swarm Control CLI
    
    Control a distributed drone swarm using Raft consensus algorithm.
    Each drone is controlled by a Raft node for fault-tolerant coordination.
    Leader-centered formation keeps the leader at formation center.
    
    Configuration is loaded from config.ini file.
    """
    global swarm_cli
    swarm_cli = SwarmCLI(host)
    ctx.ensure_object(dict)
    ctx.obj['host'] = host

@cli.command()
async def config():
    """Show current configuration"""
    click.echo("Current Configuration:")
    click.echo("=" * 50)
    
    try:
        all_nodes = swarm_cli.config_manager.get_all_nodes()
        click.echo(f"{'Node ID':<10} {'Host:Port':<15} {'Drone Connection':<25}")
        click.echo("-" * 60)
        
        for node_id, port, connection in all_nodes:
            click.echo(f"{node_id:<10} {swarm_cli.host}:{port:<10} {connection:<25}")
        
        click.echo(f"\nTotal nodes: {len(all_nodes)}")
        click.echo(f"Host: {swarm_cli.host}")
        
    except Exception as e:
        click.echo(f"Error reading configuration: {e}")

@cli.command()
async def status():
    """Show status of all nodes and drones in the cluster"""
    click.echo("Checking cluster status...")
    
    cluster_status = await swarm_cli.get_cluster_status()
    
    # Find leader
    leader_node = None
    online_nodes = 0
    
    click.echo("\n" + "="*90)
    click.echo("                        RAFT-DRONE CLUSTER STATUS")
    click.echo("="*90)
    
    for node_info in cluster_status:
        node_metadata = node_info['node_metadata']
        status = node_info['status']
        host_port = f"{node_metadata.get_host()}:{node_metadata.get_port()}"
        
        if status:
            online_nodes += 1
            node_id = status['node_id']
            state = status['state'].upper()
            term = status['term']
            drone_connected = "YES" if status.get('drone_connected') else "NO"
            
            if state == 'LEADER':
                leader_node = node_id
                click.echo(f"[LEADER] {node_id:<8} | {host_port:<15} | {state:<9} | Term {term:<3} | Drone {drone_connected}")
            else:
                click.echo(f"         {node_id:<8} | {host_port:<15} | {state:<9} | Term {term:<3} | Drone {drone_connected}")
            
            # Show drone status if connected
            drone_status = status.get('drone_status')
            if drone_status and drone_status.get('connected'):
                armed = "Armed" if drone_status.get('armed') else "Disarmed"
                mode = drone_status.get('mode', 'Unknown')
                alt = drone_status.get('altitude', 0)
                click.echo(f"         Drone: {armed} | Mode: {mode} | Alt: {alt:.1f}m")
        else:
            # Get node ID from port using config
            try:
                node_id = swarm_cli.config_manager.get_node_id_by_port(node_metadata.get_port())
                click.echo(f"         {node_id:<8} | {host_port:<15} | OFFLINE   | ---   | ---")
            except Exception:
                click.echo(f"         Node{node_metadata.get_port()}    | {host_port:<15} | OFFLINE   | ---   | ---")
    
    click.echo("="*90)
    click.echo(f"Cluster: {online_nodes}/{len(swarm_cli.cluster_nodes)} nodes online")
    if leader_node:
        click.echo(f"Leader: {leader_node}")
        if swarm_cli.current_leader:
            leader_addr = f"{swarm_cli.current_leader.get_host()}:{swarm_cli.current_leader.get_port()}"
            click.echo(f"Leader Address: {leader_addr}")
    else:
        click.echo("Leader: None (election in progress)")
    click.echo()

@cli.command()
@click.option('--watch', '-w', is_flag=True, help='Continuously watch status')
@click.option('--interval', '-i', default=2, help='Update interval for watch mode')
async def monitor(watch, interval):
    """Monitor cluster status (use --watch for continuous monitoring)"""
    if watch:
        click.echo("Starting continuous monitoring (Press Ctrl+C to stop)...")
        try:
            while True:
                # Clear screen
                click.clear()
                await status.callback()
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            click.echo("\nMonitoring stopped")
    else:
        await status.callback()

@cli.command()
async def connect():
    """Connect all nodes to their drones"""
    click.echo("Connecting all drones...")
    
    result = await swarm_cli.send_command(SwarmCommandType.CONNECT_ALL.value)
    
    if result and result.get('success'):
        click.echo("SUCCESS: Connect command processed")
    else:
        click.echo(f"ERROR: Connect failed: {result.get('error') if result else 'No response'}")

@cli.command()
async def arm():
    """Arm all drones in the swarm"""
    click.echo("Arming all drones...")
    
    result = await swarm_cli.send_command(SwarmCommandType.ARM_ALL.value)
    
    if result and result.get('success'):
        click.echo("SUCCESS: All drones armed successfully")
    else:
        click.echo(f"ERROR: Arm command failed: {result.get('error') if result else 'No response'}")

@cli.command()
async def disarm():
    """Disarm all drones in the swarm"""
    click.echo("Disarming all drones...")
    
    result = await swarm_cli.send_command(SwarmCommandType.DISARM_ALL.value)
    
    if result and result.get('success'):
        click.echo("SUCCESS: All drones disarmed successfully")
    else:
        click.echo(f"ERROR: Disarm command failed: {result.get('error') if result else 'No response'}")

@cli.command()
@click.argument('altitude', type=float)
async def takeoff(altitude):
    """Take off all drones to specified altitude (meters)"""
    if altitude <= 0:
        click.echo("ERROR: Altitude must be positive")
        return
    
    click.echo(f"Taking off all drones to {altitude}m...")
    
    result = await swarm_cli.send_command(
        SwarmCommandType.TAKEOFF_ALL.value,
        {'altitude': altitude}
    )
    
    if result and result.get('success'):
        click.echo(f"SUCCESS: Takeoff command sent - target altitude: {altitude}m")
    else:
        click.echo(f"ERROR: Takeoff failed: {result.get('error') if result else 'No response'}")

@cli.command()
async def land():
    """Land all drones"""
    click.echo("Landing all drones...")
    
    result = await swarm_cli.send_command(SwarmCommandType.LAND_ALL.value)
    
    if result and result.get('success'):
        click.echo("SUCCESS: Land command sent to all drones")
    else:
        click.echo(f"ERROR: Land command failed: {result.get('error') if result else 'No response'}")

@cli.command()
@click.argument('formation_type', type=click.Choice(['line', 'wedge']))
@click.argument('interval', type=float)
@click.option('--angle', '-a', default=0.0, help='Formation rotation angle in degrees')
@click.option('--execute', '-e', is_flag=True, help='Execute movement immediately after setting formation')
async def formation(formation_type, interval, angle, execute):
    """Set LEADER-CENTERED formation (LINE or WEDGE)
    
    FORMATION_TYPE: 'line' or 'wedge'
    INTERVAL: Distance between adjacent drones in meters
    
    LINE FORMATION:
    The leader drone stays at center, other drones form a straight line.
    
    WEDGE FORMATION:
    The leader drone stays at tip (front center), other drones form a V-shape behind.
    Pattern: Left wing drones at (-X, -Y), Right wing drones at (+X, -Y)
    
    Examples:
        formation line 10.0           # Line formation, 10m spacing
        formation wedge 15.0          # Wedge formation, 15m spacing
        formation line 20.0 -a 45     # Diagonal line at 45°
        formation wedge 25.0 -e       # Wedge formation, execute immediately
    """
    if interval <= 0:
        click.echo("ERROR: Interval must be positive")
        return
    
    click.echo(f"Setting LEADER-CENTERED {formation_type.upper()} formation:")
    click.echo(f"   Type: {formation_type.upper()}")
    click.echo(f"   Spacing: {interval}m")
    click.echo(f"   Angle: {angle}° rotation") 
    click.echo("   Leader: Stays at current GPS position")
    click.echo("   Followers: Move relative to leader's GPS")
    click.echo(f"   Execute: {'Yes' if execute else 'No'}")
    
    if formation_type.lower() == 'wedge':
        click.echo("   Pattern: V-shape with leader at tip (front)")
    else:
        click.echo("   Pattern: Straight line with leader at center")
    
    result = await swarm_cli.send_command(
        SwarmCommandType.SET_FORMATION.value,
        {
            'formation_type': formation_type,
            'interval': interval,
            'angle': angle,
            'execute': execute
        }
    )
    
    if result and result.get('success'):
        leader_id = result.get('leader_id')
        leader_pos = result.get('leader_position', {})
        
        click.echo("SUCCESS: Leader-centered formation configured")
        click.echo(f"   Leader: {leader_id} at GPS {leader_pos.get('lat', 0):.6f}, {leader_pos.get('lon', 0):.6f}")
        click.echo(f"   Formation: {result.get('positions_set', 0)} drones positioned")
        
        if execute and result.get('movement_executed'):
            click.echo(f"   Movement: {result.get('moved_nodes', 0)} followers moved to position")
        elif execute:
            click.echo("   WARNING: Formation set but movement execution failed")
    else:
        click.echo(f"ERROR: Formation failed: {result.get('error') if result else 'No response'}")

@cli.command()
async def execute_formation():
    """Execute movement to previously set formation positions"""
    click.echo("Executing formation movement...")
    
    result = await swarm_cli.send_command(SwarmCommandType.EXECUTE_FORMATION.value)
    
    if result and result.get('success'):
        moved_nodes = result.get('moved_nodes', 0)
        total_nodes = result.get('total_nodes', 0)
        click.echo("SUCCESS: Formation movement executed")
        click.echo(f"   Moved {moved_nodes}/{total_nodes} drones to formation positions")
    else:
        click.echo(f"ERROR: Formation execution failed: {result.get('error') if result else 'No response'}")

@cli.command()
@click.argument('interval', type=float)
@click.option('--angle', '-a', default=0.0, help='Formation rotation angle in degrees')
async def formation_step(interval, angle):
    """Set LINE formation and execute in separate steps (for debugging)
    
    INTERVAL: Distance between adjacent drones in meters
    
    This command performs formation setup and execution as separate steps,
    useful for debugging and monitoring intermediate states.
    """
    if interval <= 0:
        click.echo("ERROR: Interval must be positive")
        return
    
    # Step 1: Set formation positions
    click.echo("Step 1: Configuring LINE formation...")
    click.echo(f"   Interval: {interval}m")
    click.echo(f"   Angle: {angle}°")
    
    result = await swarm_cli.send_command(
        SwarmCommandType.SET_FORMATION.value,
        {
            'formation_type': 'line',
            'interval': interval,
            'angle': angle,
            'execute': False
        }
    )
    
    if result and result.get('success'):
        click.echo(f"SUCCESS: Formation positions calculated for {result.get('positions_set', 0)} drones")
    else:
        click.echo(f"ERROR: Failed to set formation: {result.get('error') if result else 'No response'}")
        return
    
    # Status check after configuration
    click.echo("\nChecking formation configuration...")
    await asyncio.sleep(2)
    await status.callback()
    
    # Step 2: Execute movement
    click.echo("\nStep 2: Executing movement to LINE formation...")
    await execute_formation.callback()
    
    # Final status check
    await asyncio.sleep(3)
    click.echo("\nFinal status after formation execution:")
    await status.callback()

@cli.command() 
async def formation_status():
    """Show GPS-based formation status for all nodes"""
    click.echo("Checking GPS formation status...")
    
    cluster_status = await swarm_cli.get_cluster_status()
    
    click.echo("\n" + "="*90)
    click.echo("                              GPS FORMATION STATUS")
    click.echo("="*90)
    
    leader_found = False
    
    for node_info in cluster_status:
        node_metadata = node_info['node_metadata']
        status = node_info['status']
        host_port = f"{node_metadata.get_host()}:{node_metadata.get_port()}"
        
        if status:
            node_id = status['node_id']
            state = status['state']
            swarm_state = status.get('swarm_state', {})
            
            # Get current GPS position
            drone_status = status.get('drone_status', {})
            current_pos = drone_status.get('position')
            current_alt = drone_status.get('altitude', 0)
            
            if state == 'leader':
                leader_found = True
                click.echo(f"LEADER  {node_id:<8} | {host_port:<15} | GPS: {current_pos[0]:.6f}, {current_pos[1]:.6f} | Alt: {current_alt:.1f}m" if current_pos else f"LEADER  {node_id:<8} | {host_port:<15} | GPS: Not available")
            else:
                click.echo(f"FOLLOWER {node_id:<8} | {host_port:<15} | GPS: {current_pos[0]:.6f}, {current_pos[1]:.6f} | Alt: {current_alt:.1f}m" if current_pos else f"FOLLOWER {node_id:<8} | {host_port:<15} | GPS: Not available")
            
            # Show formation assignment
            target_gps = swarm_state.get('target_gps')
            if target_gps:
                if target_gps.get('is_leader'):
                    click.echo("     Role: Formation CENTER (stays in place)")
                else:
                    target_lat = target_gps.get('lat', 0)
                    target_lon = target_gps.get('lon', 0)
                    rel_pos = target_gps.get('relative_pos', (0, 0))
                    click.echo(f"     Target: {target_lat:.6f}, {target_lon:.6f}")
                    click.echo(f"     Relative: {rel_pos[0]:+.1f}m East, {rel_pos[1]:+.1f}m North")
        else:
            try:
                node_id = swarm_cli.config_manager.get_node_id_by_port(node_metadata.get_port())
                click.echo(f"OFFLINE  {node_id:<8} | {host_port:<15} | Status: Disconnected")
            except Exception:
                click.echo(f"OFFLINE  Node{node_metadata.get_port()}    | {host_port:<15} | Status: Disconnected")
    
    click.echo("="*90)
    if leader_found:
        click.echo("Leader-centered formation system active")
    else:
        click.echo("No leader found - formation system inactive")

@cli.command()
@click.argument('formation_type', type=click.Choice(['line', 'wedge']))
@click.argument('interval', type=float)
@click.option('--angle', '-a', default=0.0, help='Formation rotation angle')
async def preview_gps(formation_type, interval, angle):
    """Preview GPS formation positions relative to current leader
    
    FORMATION_TYPE: 'line' or 'wedge'
    INTERVAL: Distance between drones in meters
    
    Shows where each drone will be positioned relative to the leader's 
    current GPS coordinates.
    """
    if interval <= 0:
        click.echo("ERROR: Interval must be positive")
        return
    
    # Find current leader
    if not await swarm_cli.find_leader():
        click.echo("ERROR: No leader found to use as formation center")
        return
    
    # Get leader status
    leader_status = await RaftDroneClient.get_node_status(swarm_cli.current_leader)
    if not leader_status:
        click.echo("ERROR: Cannot get leader status")
        return
    
    drone_status = leader_status.get('drone_status', {})
    leader_pos = drone_status.get('position')
    leader_alt = drone_status.get('altitude', 0)
    
    if not leader_pos:
        click.echo("ERROR: Leader GPS position not available")
        return
    
    leader_lat, leader_lon = leader_pos
    
    click.echo(f"\nGPS Formation Preview ({formation_type.upper()}):")
    click.echo(f"   Leader GPS: {leader_lat:.6f}, {leader_lon:.6f}, {leader_alt:.1f}m")
    click.echo(f"   Spacing: {interval}m between drones")
    click.echo(f"   Angle: {angle}° rotation")
    click.echo("\n" + "="*85)
    click.echo("Node     │ Role     │ GPS Latitude  │ GPS Longitude │ Relative Position")
    click.echo("─"*85)
    
    # Calculate positions for all nodes
    import math
    
    # Get all nodes from config
    all_nodes = swarm_cli.config_manager.get_all_nodes()
    leader_id = leader_status['node_id']
    
    # Find leader index
    leader_index = None
    for i, (node_id, _, _) in enumerate(all_nodes):
        if node_id == leader_id:
            leader_index = i
            break
    
    if leader_index is None:
        click.echo("ERROR: Cannot determine leader index")
        return
    
    _num_drones = len(all_nodes)
    
    for i, (node_name, _, _) in enumerate(all_nodes):
        if i == leader_index:
            # Leader stays at center
            role = "CENTER" if formation_type.lower() == 'line' else "TIP"
            click.echo(f"{node_name:<8} │ {role:<8} │ {leader_lat:>12.6f} │ {leader_lon:>12.6f} │ (  0.0,   0.0) [LEADER]")
        else:
            # Calculate follower position based on formation type
            if formation_type.lower() == 'line':
                # Line formation logic
                if i < leader_index:
                    position_offset = -(leader_index - i)
                else:
                    position_offset = i - leader_index
                
                x_offset = position_offset * interval
                y_offset = 0.0
                
            elif formation_type.lower() == 'wedge':
                # Wedge formation logic
                if i < leader_index:
                    # Left wing
                    position_level = leader_index - i
                    x_offset = -position_level * interval
                    y_offset = -position_level * interval
                else:
                    # Right wing
                    position_level = i - leader_index
                    x_offset = position_level * interval
                    y_offset = -position_level * interval
            
            # Apply rotation
            angle_rad = math.radians(angle)
            rel_x = x_offset * math.cos(angle_rad) - y_offset * math.sin(angle_rad)
            rel_y = x_offset * math.sin(angle_rad) + y_offset * math.cos(angle_rad)
            
            # Convert to GPS
            earth_radius = 6378137.0
            lat_offset = rel_y / earth_radius * (180.0 / math.pi)
            lon_offset = rel_x / (earth_radius * math.cos(math.radians(leader_lat))) * (180.0 / math.pi)
            
            target_lat = leader_lat + lat_offset
            target_lon = leader_lon + lon_offset
            
            wing = "LEFT" if (formation_type.lower() == 'wedge' and i < leader_index) else "RIGHT" if (formation_type.lower() == 'wedge' and i > leader_index) else "FOLLOW"
            click.echo(f"{node_name:<8} │ {wing:<8} │ {target_lat:>12.6f} │ {target_lon:>12.6f} │ ({rel_x:+5.1f}, {rel_y:+5.1f})")
    
    click.echo("="*85)
    click.echo("Note: Leader stays at current GPS position, followers move to calculated positions")

@cli.command()
async def swarm_status():
    """Get detailed swarm status from leader"""
    click.echo("Getting swarm status...")
    
    result = await swarm_cli.send_command(SwarmCommandType.GET_SWARM_STATUS.value)
    
    if result:
        click.echo("\n" + "="*60)
        click.echo("                    SWARM STATUS")
        click.echo("="*60)
        
        leader = result.get('leader')
        term = result.get('term')
        nodes = result.get('nodes', {})
        
        click.echo(f"Leader: {leader}")
        click.echo(f"Term: {term}")
        click.echo(f"Nodes: {len(nodes)}")
        
        click.echo("\nNode Details:")
        for node_id, node_data in nodes.items():
            state = node_data.get('state', 'unknown')
            drone_connected = node_data.get('drone_connected', False)
            drone_status = node_data.get('drone_status')
            
            click.echo(f"\n  {node_id}:")
            click.echo(f"    State: {state}")
            click.echo(f"    Drone Connected: {'Yes' if drone_connected else 'No'}")
            
            if drone_status:
                armed = drone_status.get('armed', False)
                mode = drone_status.get('mode', 'Unknown')
                alt = drone_status.get('altitude', 0)
                click.echo(f"    Drone Status: {'Armed' if armed else 'Disarmed'} | {mode} | {alt:.1f}m")
    else:
        click.echo("ERROR: Failed to get swarm status")

@cli.command()
async def leader():
    """Find and display current leader information"""
    click.echo("Finding cluster leader...")
    
    if await swarm_cli.find_leader():
        leader_addr = f"{swarm_cli.current_leader.get_host()}:{swarm_cli.current_leader.get_port()}"
        click.echo(f"SUCCESS: Current leader at {leader_addr}")
        
        # Get detailed status of leader
        status = await RaftDroneClient.get_node_status(swarm_cli.current_leader)
        if status:
            click.echo(f"   Node ID: {status.get('node_id')}")
            click.echo(f"   Term: {status.get('term')}")
            click.echo(f"   Log entries: {status.get('log_length', 0)}")
            click.echo(f"   Committed: {status.get('commit_index', 0)}")
            
            drone_status = status.get('drone_status')
            if drone_status and drone_status.get('connected'):
                click.echo(f"   Drone: Connected and {'Armed' if drone_status.get('armed') else 'Disarmed'}")
            else:
                click.echo("   Drone: Not connected")
    else:
        click.echo("ERROR: No leader found in cluster")

@cli.command()
@click.argument('command')
async def raw(command):
    """Send raw command to leader
    
    COMMAND: Raw command string to execute
    """
    click.echo(f"Sending raw command: {command}")
    
    # Send as a client request (not swarm command)
    if not swarm_cli.current_leader:
        if not await swarm_cli.find_leader():
            click.echo("ERROR: No leader found")
            return
    
    try:
        client_metadata = NodeMetadata('client', 0)
        
        message = Message(
            msg_type=MessageType.CLIENT_REQUEST.value,
            data={'command': command},
            sender=client_metadata
        )
        
        reader, writer = await asyncio.open_connection(
            swarm_cli.current_leader.get_host(), 
            swarm_cli.current_leader.get_port()
        )
        try:
            # Use NetworkComm's protocol instead of SocketProtocol
            await SwarmCLI._send_message_with_length_prefix(writer, message)
            response = await SwarmCLI._receive_message_with_length_prefix(reader)
            
            if response and response.msg_type == MessageType.CLIENT_RESPONSE.value:
                result = response.data
                if result.get('success'):
                    click.echo("SUCCESS: Command executed successfully")
                    click.echo(f"   Index: {result.get('index')}")
                else:
                    click.echo(f"ERROR: Command failed: {result.get('error')}")
            else:
                click.echo("ERROR: No valid response received")
        finally:
            writer.close()
            await writer.wait_closed()
    
    except Exception as e:
        click.echo(f"ERROR: Error sending command: {e}")

@cli.command()
@click.option('--format', 'output_format', default='table', type=click.Choice(['table', 'json']), help='Output format')
async def logs(output_format):
    """Show cluster logs and commit history"""
    click.echo("Retrieving cluster logs...")
    
    cluster_status = await swarm_cli.get_cluster_status()
    logs_data = {}
    
    for node_info in cluster_status:
        node_metadata = node_info['node_metadata']
        status = node_info['status']
        
        if status:
            node_id = status['node_id']
            host_port = f"{node_metadata.get_host()}:{node_metadata.get_port()}"
            logs_data[node_id] = {
                'address': host_port,
                'state': status['state'],
                'term': status['term'],
                'log_length': status.get('log_length', 0),
                'commit_index': status.get('commit_index', 0)
            }
    
    if output_format == 'json':
        click.echo(json.dumps(logs_data, indent=2))
    else:
        click.echo("\n" + "="*80)
        click.echo("                          CLUSTER LOGS")
        click.echo("="*80)
        click.echo(f"{'Node':<8} {'Address':<15} {'State':<9} {'Term':<6} {'Log Len':<8} {'Committed':<10}")
        click.echo("-"*80)
        
        for node_id, data in logs_data.items():
            click.echo(f"{node_id:<8} {data['address']:<15} {data['state']:<9} {data['term']:<6} {data['log_length']:<8} {data['commit_index']:<10}")

@cli.command()
async def health():
    """Perform cluster health check"""
    click.echo("Performing cluster health check...")
    
    # Check node connectivity
    cluster_status = await swarm_cli.get_cluster_status()
    online_nodes = sum(1 for node in cluster_status if node['status'])
    total_nodes = len(cluster_status)
    
    click.echo(f"\nConnectivity: {online_nodes}/{total_nodes} nodes online")
    
    # Check leader status
    leader_found = await swarm_cli.find_leader()
    click.echo(f"Leadership: {'Healthy' if leader_found else 'No leader found'}")
    
    # Check drone connections
    drone_connections = 0
    armed_drones = 0
    
    for node_info in cluster_status:
        status = node_info['status']
        if status and status.get('drone_connected'):
            drone_connections += 1
            drone_status = status.get('drone_status', {})
            if drone_status.get('armed'):
                armed_drones += 1
    
    click.echo(f"Drone Connectivity: {drone_connections}/{total_nodes} drones connected")
    click.echo(f"Drone Readiness: {armed_drones}/{drone_connections} connected drones armed")
    
    # Overall health assessment
    health_score = 0
    if online_nodes >= total_nodes // 2 + 1:
        health_score += 30  # Majority online
    if leader_found:
        health_score += 25  # Leader available
    if drone_connections >= total_nodes // 2:
        health_score += 25  # Majority of drones connected
    if armed_drones == drone_connections and drone_connections > 0:
        health_score += 20  # All connected drones armed
    
    click.echo(f"\nOverall Health Score: {health_score}/100")
    
    if health_score >= 80:
        click.echo("SUCCESS: Cluster is healthy and ready for operations")
    elif health_score >= 60:
        click.echo("WARNING: Cluster has minor issues but is operational")
    elif health_score >= 40:
        click.echo("WARNING: Cluster has significant issues - limited functionality")
    else:
        click.echo("ERROR: Cluster is unhealthy - operations not recommended")

@cli.command()
async def formation_help():
    """Show detailed help about formation systems"""
    help_text = """
LEADER-CENTERED FORMATION SYSTEM

OVERVIEW:
The swarm supports two formation types: LINE and WEDGE
Both formations keep the leader at the center/tip position.

FORMATION TYPES:

1. LINE FORMATION:
2. WEDGE FORMATION:

LEADER-CENTERED CONCEPT:
• Current Raft leader becomes formation reference point
• Leader stays at its current GPS coordinates
• All other drones calculate target positions relative to leader's GPS
• Only follower drones move; leader conserves energy

COORDINATE SYSTEM:
• Origin: Leader's GPS position
• X-axis: East (positive right →)
• Y-axis: North (positive up ↑)  
• Angles: Counter-clockwise from East

COMMANDS:
formation <type> <interval> [--angle N] [--execute]
    Set formation with leader at center/tip
    Types: line, wedge

preview_gps <type> <interval> [--angle N]
    Preview GPS positions for formation type

formation_status
    Show current formation assignments and GPS targets

EXAMPLES:
formation line 10.0                    # Horizontal line, leader at center
formation wedge 15.0                   # V-shaped wedge, leader at tip
formation line 20.0 -a 45             # Diagonal line at 45°
formation wedge 25.0 -a 90 -e          # Sideways wedge, execute immediately
preview_gps wedge 30.0 -a 30           # Preview rotated wedge formation

WEDGE FORMATION DETAILS:
• Leader at tip (front center): (0, 0)
• Left wing: negative X, negative Y coordinates
• Right wing: positive X, negative Y coordinates
• Spacing determines both horizontal and vertical separation
• Perfect for tactical formations and wind management

DISTRIBUTED DEPLOYMENT:
• Supports multiple hosts with --host option
• Example: swarm.py --host 192.168.1.100 status
• Each node can run on different machines
• Full host:port addressing for network flexibility

CONFIGURATION:
• Drone connections and ports loaded from config.ini
• Each drone has its own connection string and node port
• View configuration with 'config' command
    """
    click.echo(help_text)

@cli.command()
@click.option('--nodes', '-n', multiple=True, help='Specific node addresses (host:port) to ping')
async def ping(nodes):
    """Ping cluster nodes to test connectivity
    
    Examples:
        ping                                    # Ping all configured nodes
        ping -n localhost:8001 -n 192.168.1.100:8002  # Ping specific nodes
    """
    click.echo("Pinging cluster nodes...")
    
    # Determine which nodes to ping
    if nodes:
        # Parse specific nodes
        target_nodes = []
        for node_addr in nodes:
            try:
                host, port = node_addr.split(':')
                target_nodes.append(NodeMetadata(host, int(port)))
            except ValueError:
                click.echo(f"ERROR: Invalid node address format: {node_addr} (expected host:port)")
                return
    else:
        # Use all configured nodes
        target_nodes = swarm_cli.cluster_nodes
    
    click.echo(f"Testing connectivity to {len(target_nodes)} nodes...")
    click.echo("-" * 60)
    
    # Test each node
    successful_pings = 0
    for node_metadata in target_nodes:
        host_port = f"{node_metadata.get_host()}:{node_metadata.get_port()}"
        
        try:
            # Try to connect and get status
            start_time = asyncio.get_event_loop().time()
            status = await RaftDroneClient.get_node_status(node_metadata)
            end_time = asyncio.get_event_loop().time()
            
            if status:
                response_time = (end_time - start_time) * 1000  # Convert to ms
                node_id = status.get('node_id', 'unknown')
                state = status.get('state', 'unknown')
                click.echo(f"✓ {host_port:<20} | {node_id:<8} | {state:<9} | {response_time:.1f}ms")
                successful_pings += 1
            else:
                click.echo(f"✗ {host_port:<20} | No response")
        
        except Exception as e:
            click.echo(f"✗ {host_port:<20} | Connection failed: {str(e)[:30]}...")
    
    click.echo("-" * 60)
    click.echo(f"Connectivity: {successful_pings}/{len(target_nodes)} nodes reachable")
    
    if successful_pings == len(target_nodes):
        click.echo("SUCCESS: All nodes are reachable")
    elif successful_pings >= len(target_nodes) // 2 + 1:
        click.echo("WARNING: Majority of nodes reachable, cluster should be operational")
    else:
        click.echo("ERROR: Majority of nodes unreachable, cluster may be down")

@cli.command()
@click.option('--interval', '-i', default=5, help='Update interval in seconds')
@click.option('--count', '-c', default=0, help='Number of updates (0 = infinite)')
async def network_monitor(interval, count):
    """Monitor network connectivity to all cluster nodes
    
    Continuously monitors the health of network connections to all nodes
    in the cluster. Shows response times and connection status.
    """
    click.echo("Starting network connectivity monitor...")
    click.echo(f"Update interval: {interval}s")
    click.echo(f"Monitoring {len(swarm_cli.cluster_nodes)} nodes")
    click.echo("Press Ctrl+C to stop\n")
    
    update_counter = 0
    
    try:
        while count == 0 or update_counter < count:
            # Clear screen and show header
            if update_counter > 0:
                click.clear()
            
            click.echo(f"Network Monitor - Update #{update_counter + 1}")
            click.echo(f"Timestamp: {asyncio.get_event_loop().time():.0f}")
            click.echo("=" * 70)
            click.echo(f"{'Address':<20} | {'Node ID':<8} | {'State':<9} | {'Response':<10}")
            click.echo("-" * 70)
            
            # Test all nodes
            successful_connections = 0
            total_response_time = 0
            
            for node_metadata in swarm_cli.cluster_nodes:
                host_port = f"{node_metadata.get_host()}:{node_metadata.get_port()}"
                
                try:
                    start_time = asyncio.get_event_loop().time()
                    status = await RaftDroneClient.get_node_status(node_metadata)
                    end_time = asyncio.get_event_loop().time()
                    
                    if status:
                        response_time = (end_time - start_time) * 1000
                        total_response_time += response_time
                        node_id = status.get('node_id', 'unknown')
                        state = status.get('state', 'unknown')
                        click.echo(f"{host_port:<20} | {node_id:<8} | {state:<9} | {response_time:.1f}ms")
                        successful_connections += 1
                    else:
                        click.echo(f"{host_port:<20} | {'ERROR':<8} | {'NO_RESP':<9} | {'TIMEOUT':<10}")
                
                except Exception as e:
                    error_msg = str(e)[:8] if len(str(e)) > 8 else str(e)
                    click.echo(f"{host_port:<20} | {'ERROR':<8} | {'FAILED':<9} | {error_msg:<10}")
            
            # Summary
            click.echo("-" * 70)
            avg_response = total_response_time / successful_connections if successful_connections > 0 else 0
            click.echo(f"Connected: {successful_connections}/{len(swarm_cli.cluster_nodes)} | "
                      f"Avg Response: {avg_response:.1f}ms")
            
            # Health indicator
            if successful_connections == len(swarm_cli.cluster_nodes):
                click.echo("Status: ✓ All nodes healthy")
            elif successful_connections >= len(swarm_cli.cluster_nodes) // 2 + 1:
                click.echo("Status: ⚠ Majority healthy")
            else:
                click.echo("Status: ✗ Cluster unhealthy")
            
            update_counter += 1
            
            if count == 0 or update_counter < count:
                await asyncio.sleep(interval)
            
    except KeyboardInterrupt:
        click.echo(f"\nNetwork monitoring stopped after {update_counter} updates")

@cli.command()
@click.argument('host', default='localhost')
@click.argument('start_port', type=int)
@click.argument('end_port', type=int)
async def scan(host, start_port, end_port):
    """Scan for active Raft nodes on a host and port range
    
    HOST: Target host to scan (default: localhost)
    START_PORT: Starting port number
    END_PORT: Ending port number
    
    Examples:
        scan localhost 8000 8010        # Scan localhost ports 8000-8010
        scan 192.168.1.100 8001 8005    # Scan remote host specific range
    """
    if start_port > end_port:
        click.echo("ERROR: Start port must be <= end port")
        return
    
    port_range = end_port - start_port + 1
    if port_range > 100:
        click.echo("ERROR: Port range too large (max 100 ports)")
        return
    
    click.echo(f"Scanning {host}:{start_port}-{end_port} for Raft nodes...")
    click.echo("-" * 60)
    
    found_nodes = []
    
    for port in range(start_port, end_port + 1):
        try:
            node_metadata = NodeMetadata(host, port)
            status = await RaftDroneClient.get_node_status(node_metadata)
            
            if status:
                node_id = status.get('node_id', 'unknown')
                state = status.get('state', 'unknown')
                term = status.get('term', 0)
                drone_connected = "YES" if status.get('drone_connected') else "NO"
                
                click.echo(f"✓ {host}:{port:<5} | {node_id:<8} | {state:<9} | Term {term} | Drone {drone_connected}")
                found_nodes.append((node_metadata, status))
            else:
                click.echo(f"- {host}:{port:<5} | No Raft node")
        
        except Exception:
            # Silently skip ports that can't be connected to
            pass
    
    click.echo("-" * 60)
    click.echo(f"Found {len(found_nodes)} active Raft nodes")
    
    if found_nodes:
        # Find leader among discovered nodes
        leader_node = None
        for node_metadata, status in found_nodes:
            if status.get('state') == 'leader':
                leader_node = node_metadata
                break
        
        if leader_node:
            leader_addr = f"{leader_node.get_host()}:{leader_node.get_port()}"
            click.echo(f"Leader found at: {leader_addr}")
        else:
            click.echo("No leader found among discovered nodes")

# Async wrapper for click commands
def async_command(f):
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper

# Apply async wrapper to all commands
for name, command in cli.commands.items():
    if asyncio.iscoroutinefunction(command.callback):
        command.callback = async_command(command.callback)

if __name__ == '__main__':
    cli()
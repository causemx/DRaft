#!/usr/bin/env python3
"""
Swarm Control CLI - Command line interface for controlling the Raft-Drone swarm
"""

import asyncio
import click
import json
import time
from typing import Optional, Dict, List, Tuple
from raft_node import RaftDroneClient, SwarmCommandType

class SwarmCLI:
    def __init__(self):
        self.cluster_ports = [8000, 8001, 8002, 8003, 8004]
        self.current_leader = None
        self.current_leader_port = None
    
    async def find_leader(self) -> bool:
        """Find the current leader in the cluster"""
        leader_info = await RaftDroneClient.find_leader(self.cluster_ports)
        if leader_info:
            self.current_leader, self.current_leader_port = leader_info
            return True
        return False
    
    async def send_command(self, command_type: str, parameters: Dict = None) -> Optional[Dict]:
        """Send command to the leader"""
        if not self.current_leader_port:
            if not await self.find_leader():
                click.echo("❌ No leader found in cluster")
                return None
        
        result = await RaftDroneClient.send_swarm_command(
            'localhost', 
            self.current_leader_port, 
            command_type, 
            parameters or {}
        )
        
        # If command failed and it's a "not leader" error, try to find new leader
        if result and result.get('error') == 'Not leader':
            if await self.find_leader():
                result = await RaftDroneClient.send_swarm_command(
                    'localhost', 
                    self.current_leader_port, 
                    command_type, 
                    parameters or {}
                )
        
        return result
    
    async def get_cluster_status(self) -> List[Dict]:
        """Get status of all nodes in the cluster"""
        statuses = []
        for port in self.cluster_ports:
            status = await RaftDroneClient.get_node_status('localhost', port)
            statuses.append({
                'port': port,
                'status': status
            })
        return statuses

# Create global CLI instance
swarm_cli = SwarmCLI()

@click.group()
def cli():
    """🚁 Raft-Drone Swarm Control CLI
    
    Control a distributed drone swarm using Raft consensus algorithm.
    Each drone is controlled by a Raft node for fault-tolerant coordination.
    """
    pass

@cli.command()
async def status():
    """📊 Show status of all nodes and drones in the cluster"""
    click.echo("🔍 Checking cluster status...")
    
    cluster_status = await swarm_cli.get_cluster_status()
    
    # Find leader
    leader_node = None
    online_nodes = 0
    
    click.echo("\n" + "="*80)
    click.echo("                        RAFT-DRONE CLUSTER STATUS")
    click.echo("="*80)
    
    for node_info in cluster_status:
        port = node_info['port']
        status = node_info['status']
        
        if status:
            online_nodes += 1
            node_id = status['node_id']
            state = status['state'].upper()
            term = status['term']
            drone_connected = "✅" if status.get('drone_connected') else "❌"
            
            if state == 'LEADER':
                leader_node = node_id
                click.echo(f"👑 {node_id:<8} | Port {port} | {state:<9} | Term {term:<3} | Drone {drone_connected}")
            else:
                click.echo(f"   {node_id:<8} | Port {port} | {state:<9} | Term {term:<3} | Drone {drone_connected}")
            
            # Show drone status if connected
            drone_status = status.get('drone_status')
            if drone_status and drone_status.get('connected'):
                armed = "🟢 Armed" if drone_status.get('armed') else "🔴 Disarmed"
                mode = drone_status.get('mode', 'Unknown')
                alt = drone_status.get('altitude', 0)
                click.echo(f"     └─ Drone: {armed} | Mode: {mode} | Alt: {alt:.1f}m")
        else:
            click.echo(f"   Node{port-7999:<4} | Port {port} | OFFLINE   | ---   | ---")
    
    click.echo("="*80)
    click.echo(f"Cluster: {online_nodes}/5 nodes online")
    if leader_node:
        click.echo(f"Leader: {leader_node}")
    else:
        click.echo("Leader: None (election in progress)")
    click.echo()

@cli.command()
@click.option('--watch', '-w', is_flag=True, help='Continuously watch status')
@click.option('--interval', '-i', default=2, help='Update interval for watch mode')
async def monitor(watch, interval):
    """📺 Monitor cluster status (use --watch for continuous monitoring)"""
    if watch:
        click.echo("🖥️  Starting continuous monitoring (Press Ctrl+C to stop)...")
        try:
            while True:
                # Clear screen
                click.clear()
                await status.callback()
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            click.echo("\n👋 Monitoring stopped")
    else:
        await status.callback()

@cli.command()
async def connect():
    """🔗 Connect all nodes to their drones"""
    click.echo("🔗 Connecting all drones...")
    
    result = await swarm_cli.send_command(SwarmCommandType.CONNECT_ALL.value)
    
    if result and result.get('success'):
        click.echo("✅ Connect command processed")
    else:
        click.echo(f"❌ Connect failed: {result.get('error') if result else 'No response'}")

@cli.command()
async def arm():
    """🔓 Arm all drones in the swarm"""
    click.echo("🔓 Arming all drones...")
    
    result = await swarm_cli.send_command(SwarmCommandType.ARM_ALL.value)
    
    if result and result.get('success'):
        click.echo("✅ All drones armed successfully")
    else:
        click.echo(f"❌ Arm command failed: {result.get('error') if result else 'No response'}")

@cli.command()
async def disarm():
    """🔒 Disarm all drones in the swarm"""
    click.echo("🔒 Disarming all drones...")
    
    result = await swarm_cli.send_command(SwarmCommandType.DISARM_ALL.value)
    
    if result and result.get('success'):
        click.echo("✅ All drones disarmed successfully")
    else:
        click.echo(f"❌ Disarm command failed: {result.get('error') if result else 'No response'}")

@cli.command()
@click.argument('altitude', type=float)
async def takeoff(altitude):
    """🚀 Take off all drones to specified altitude (meters)"""
    if altitude <= 0:
        click.echo("❌ Altitude must be positive")
        return
    
    click.echo(f"🚀 Taking off all drones to {altitude}m...")
    
    result = await swarm_cli.send_command(
        SwarmCommandType.TAKEOFF_ALL.value,
        {'altitude': altitude}
    )
    
    if result and result.get('success'):
        click.echo(f"✅ Takeoff command sent - target altitude: {altitude}m")
    else:
        click.echo(f"❌ Takeoff failed: {result.get('error') if result else 'No response'}")

@cli.command()
async def land():
    """🛬 Land all drones"""
    click.echo("🛬 Landing all drones...")
    
    result = await swarm_cli.send_command(SwarmCommandType.LAND_ALL.value)
    
    if result and result.get('success'):
        click.echo("✅ Land command sent to all drones")
    else:
        click.echo(f"❌ Land command failed: {result.get('error') if result else 'No response'}")

@cli.command()
@click.argument('formation_type', type=click.Choice(['line', 'circle', 'triangle']))
@click.argument('interval', type=float)
@click.option('--angle', '-a', default=0.0, help='Formation rotation angle in degrees')
async def formation(formation_type, interval, angle):
    """✈️ Set swarm formation
    
    FORMATION_TYPE: line, circle, or triangle
    INTERVAL: Distance between drones in meters
    """
    if interval <= 0:
        click.echo("❌ Interval must be positive")
        return
    
    click.echo(f"✈️  Setting {formation_type} formation (interval: {interval}m, angle: {angle}°)...")
    
    result = await swarm_cli.send_command(
        SwarmCommandType.SET_FORMATION.value,
        {
            'formation_type': formation_type,
            'interval': interval,
            'angle': angle
        }
    )
    
    if result and result.get('success'):
        click.echo(f"✅ Formation set: {formation_type}")
        click.echo(f"   Positions calculated for {result.get('positions_set', 0)} drones")
    else:
        click.echo(f"❌ Formation command failed: {result.get('error') if result else 'No response'}")

@cli.command()
async def swarm_status():
    """📋 Get detailed swarm status from leader"""
    click.echo("📋 Getting swarm status...")
    
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
        click.echo("❌ Failed to get swarm status")

@cli.command()
async def leader():
    """👑 Find and display current leader information"""
    click.echo("👑 Finding cluster leader...")
    
    if await swarm_cli.find_leader():
        click.echo(f"✅ Current leader: {swarm_cli.current_leader} on port {swarm_cli.current_leader_port}")
        
        # Get detailed status of leader
        status = await RaftDroneClient.get_node_status('localhost', swarm_cli.current_leader_port)
        if status:
            click.echo(f"   Term: {status.get('term')}")
            click.echo(f"   Log entries: {status.get('log_length', 0)}")
            click.echo(f"   Committed: {status.get('commit_index', 0)}")
            
            drone_status = status.get('drone_status')
            if drone_status and drone_status.get('connected'):
                click.echo(f"   Drone: Connected and {'Armed' if drone_status.get('armed') else 'Disarmed'}")
            else:
                click.echo(f"   Drone: Not connected")
    else:
        click.echo("❌ No leader found in cluster")

@cli.command()
@click.argument('command')
async def raw(command):
    """⚡ Send raw command to leader
    
    COMMAND: Raw command string to execute
    """
    click.echo(f"⚡ Sending raw command: {command}")
    
    # Send as a client request (not swarm command)
    if not swarm_cli.current_leader_port:
        if not await swarm_cli.find_leader():
            click.echo("❌ No leader found")
            return
    
    try:
        from raft_drone_node import SocketProtocol, Message, MessageType
        
        message = {
            'msg_type': MessageType.CLIENT_REQUEST.value,
            'data': {'command': command},
            'sender_id': 'cli'
        }
        
        reader, writer = await asyncio.open_connection('localhost', swarm_cli.current_leader_port)
        try:
            await SocketProtocol.send_message(writer, Message(**message))
            response = await SocketProtocol.receive_message(reader)
            
            if response and response.msg_type == MessageType.CLIENT_RESPONSE.value:
                result = response.data
                if result.get('success'):
                    click.echo(f"✅ Command executed successfully")
                    click.echo(f"   Index: {result.get('index')}")
                else:
                    click.echo(f"❌ Command failed: {result.get('error')}")
            else:
                click.echo("❌ No valid response received")
        finally:
            writer.close()
            await writer.wait_closed()
    
    except Exception as e:
        click.echo(f"❌ Error sending command: {e}")

@cli.command()
async def scenario():
    """🎬 Run demonstration scenarios"""
    scenarios = {
        '1': 'Basic swarm startup',
        '2': 'Formation flying',
        '3': 'Leader failure simulation',
        '4': 'Full mission demo'
    }
    
    click.echo("🎬 Available demonstration scenarios:")
    for key, desc in scenarios.items():
        click.echo(f"   {key}. {desc}")
    
    choice = click.prompt("Select scenario", type=click.Choice(list(scenarios.keys())))
    
    if choice == '1':
        await scenario_basic_startup()
    elif choice == '2':
        await scenario_formation_flying()
    elif choice == '3':
        await scenario_leader_failure()
    elif choice == '4':
        await scenario_full_mission()

async def scenario_basic_startup():
    """Basic swarm startup scenario"""
    click.echo("\n🎬 Scenario 1: Basic Swarm Startup")
    click.echo("="*50)
    
    steps = [
        ("Check cluster status", lambda: status.callback()),
        ("Connect all drones", lambda: connect.callback()),
        ("Arm all drones", lambda: arm.callback()),
        ("Take off to 10m", lambda: takeoff.callback(10.0)),
        ("Check final status", lambda: status.callback())
    ]
    
    for step_name, step_func in steps:
        click.echo(f"\n📍 Step: {step_name}")
        await step_func()
        await asyncio.sleep(2)
    
    click.echo("\n✅ Basic startup scenario complete!")

async def scenario_formation_flying():
    """Formation flying demonstration"""
    click.echo("\n🎬 Scenario 2: Formation Flying")
    click.echo("="*50)
    
    formations = [
        ("line", 15.0, 0),
        ("circle", 20.0, 0),
        ("triangle", 25.0, 45)
    ]
    
    # Initial setup
    click.echo("\n📍 Initial setup...")
    await connect.callback()
    await asyncio.sleep(1)
    await arm.callback()
    await asyncio.sleep(1)
    await takeoff.callback(15.0)
    await asyncio.sleep(5)
    
    # Try different formations
    for form_type, interval, angle in formations:
        click.echo(f"\n📍 Setting {form_type} formation...")
        await formation.callback(form_type, interval, angle)
        await asyncio.sleep(3)
        await status.callback()
        await asyncio.sleep(2)
    
    click.echo("\n✅ Formation flying scenario complete!")

async def scenario_leader_failure():
    """Leader failure simulation"""
    click.echo("\n🎬 Scenario 3: Leader Failure Simulation")
    click.echo("="*50)
    
    click.echo("\n📍 Finding current leader...")
    await leader.callback()
    
    if swarm_cli.current_leader_port:
        click.echo(f"\n⚠️  Simulating leader failure on port {swarm_cli.current_leader_port}")
        click.echo("   (In real scenario, you would kill the leader process)")
        click.echo("   Waiting for re-election...")
        
        # Reset leader to force re-discovery
        swarm_cli.current_leader = None
        swarm_cli.current_leader_port = None
        
        await asyncio.sleep(5)
        await leader.callback()
    
    click.echo("\n✅ Leader failure scenario complete!")

async def scenario_full_mission():
    """Full mission demonstration"""
    click.echo("\n🎬 Scenario 4: Full Mission Demo")
    click.echo("="*50)
    
    mission_steps = [
        ("System check", lambda: status.callback()),
        ("Connect drones", lambda: connect.callback()),
        ("Arm swarm", lambda: arm.callback()),
        ("Mission takeoff", lambda: takeoff.callback(20.0)),
        ("Formation: Line", lambda: formation.callback("line", 20.0, 0)),
        ("Formation: Circle", lambda: formation.callback("circle", 25.0, 0)),
        ("Formation: Triangle", lambda: formation.callback("triangle", 30.0, 0)),
        ("Mission complete - Landing", lambda: land.callback()),
        ("Disarm swarm", lambda: disarm.callback()),
        ("Final status", lambda: status.callback())
    ]
    
    click.echo(f"\n🚁 Starting full mission with {len(mission_steps)} steps...")
    
    for i, (step_name, step_func) in enumerate(mission_steps, 1):
        click.echo(f"\n📍 Step {i}/{len(mission_steps)}: {step_name}")
        await step_func()
        
        if i < len(mission_steps):
            wait_time = 5 if "takeoff" in step_name.lower() else 3
            click.echo(f"   Waiting {wait_time}s before next step...")
            await asyncio.sleep(wait_time)
    
    click.echo("\n🎉 Full mission scenario complete!")

@cli.command()
@click.option('--format', 'output_format', default='table', type=click.Choice(['table', 'json']), help='Output format')
async def logs(output_format):
    """📜 Show cluster logs and commit history"""
    click.echo("📜 Retrieving cluster logs...")
    
    cluster_status = await swarm_cli.get_cluster_status()
    logs_data = {}
    
    for node_info in cluster_status:
        port = node_info['port']
        status = node_info['status']
        
        if status:
            node_id = status['node_id']
            logs_data[node_id] = {
                'port': port,
                'state': status['state'],
                'term': status['term'],
                'log_length': status.get('log_length', 0),
                'commit_index': status.get('commit_index', 0)
            }
    
    if output_format == 'json':
        click.echo(json.dumps(logs_data, indent=2))
    else:
        click.echo("\n" + "="*70)
        click.echo("                          CLUSTER LOGS")
        click.echo("="*70)
        click.echo(f"{'Node':<8} {'Port':<6} {'State':<9} {'Term':<6} {'Log Len':<8} {'Committed':<10}")
        click.echo("-"*70)
        
        for node_id, data in logs_data.items():
            click.echo(f"{node_id:<8} {data['port']:<6} {data['state']:<9} {data['term']:<6} {data['log_length']:<8} {data['commit_index']:<10}")

@cli.command()
async def health():
    """🏥 Perform cluster health check"""
    click.echo("🏥 Performing cluster health check...")
    
    # Check node connectivity
    cluster_status = await swarm_cli.get_cluster_status()
    online_nodes = sum(1 for node in cluster_status if node['status'])
    total_nodes = len(cluster_status)
    
    click.echo(f"\n📊 Connectivity: {online_nodes}/{total_nodes} nodes online")
    
    # Check leader status
    leader_found = await swarm_cli.find_leader()
    click.echo(f"👑 Leadership: {'Healthy' if leader_found else 'No leader found'}")
    
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
    
    click.echo(f"🚁 Drone Connectivity: {drone_connections}/{total_nodes} drones connected")
    click.echo(f"🔓 Drone Readiness: {armed_drones}/{drone_connections} connected drones armed")
    
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
    
    click.echo(f"\n🎯 Overall Health Score: {health_score}/100")
    
    if health_score >= 80:
        click.echo("✅ Cluster is healthy and ready for operations")
    elif health_score >= 60:
        click.echo("⚠️  Cluster has minor issues but is operational")
    elif health_score >= 40:
        click.echo("⚠️  Cluster has significant issues - limited functionality")
    else:
        click.echo("❌ Cluster is unhealthy - operations not recommended")

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
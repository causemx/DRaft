import click
import time
import math
import threading
from typing import List, Dict, Tuple, Optional
from libs.utils import DroneController, FlightMode
from loguru import logger

class SwarmDrone:
    """Individual drone in the swarm with its unique properties"""
    def __init__(self, drone_id: str, connection_string: str):
        self.drone_id = drone_id
        self.connection_string = connection_string
        self.controller = DroneController(connection_string)
        self.target_position = None  # (lat, lon, alt)
        self.formation_position = None  # (x, y) relative to center
        self.is_connected = False
        self.is_ready = False

    def connect(self):
        """Connect to this drone"""
        try:
            if self.controller.connect():
                self.is_connected = True
                logger.success(f"Drone {self.drone_id} connected successfully")
                return True
            else:
                logger.error(f"Failed to connect to drone {self.drone_id}")
                return False
        except Exception as e:
            logger.error(f"Error connecting to drone {self.drone_id}: {str(e)}")
            return False

    def disconnect(self):
        """Disconnect from this drone"""
        if self.controller:
            self.controller.cleanup()
        self.is_connected = False

    def get_status(self):
        """Get current drone status"""
        if self.is_connected:
            return self.controller.get_drone_status()
        return {'connected': False}

class SwarmController:
    """Controller for managing multiple drones in formation"""
    
    def __init__(self):
        self.drones: Dict[str, SwarmDrone] = {}
        self.center_position = None  # (lat, lon, alt)
        self.formation_type = None
        self.formation_interval = 5.0  # meters between drones
        self.formation_angle = 0.0  # degrees
        self.is_monitoring = False
        self.monitor_thread = None

    def add_drone(self, drone_id: str, connection_string: str) -> bool:
        """Add a drone to the swarm"""
        if drone_id in self.drones:
            logger.warning(f"Drone {drone_id} already exists in swarm")
            return False
        
        swarm_drone = SwarmDrone(drone_id, connection_string)
        self.drones[drone_id] = swarm_drone
        logger.info(f"Added drone {drone_id} to swarm")
        return True

    def remove_drone(self, drone_id: str) -> bool:
        """Remove a drone from the swarm"""
        if drone_id not in self.drones:
            logger.warning(f"Drone {drone_id} not found in swarm")
            return False
        
        self.drones[drone_id].disconnect()
        del self.drones[drone_id]
        logger.info(f"Removed drone {drone_id} from swarm")
        return True

    def connect_all(self) -> bool:
        """Connect to all drones in the swarm"""
        if not self.drones:
            logger.error("No drones in swarm to connect")
            return False

        success_count = 0
        for drone_id, drone in self.drones.items():
            if drone.connect():
                success_count += 1
            time.sleep(0.5)  # Small delay between connections

        logger.info(f"Connected to {success_count}/{len(self.drones)} drones")
        return success_count == len(self.drones)

    def disconnect_all(self):
        """Disconnect from all drones"""
        for drone in self.drones.values():
            drone.disconnect()
        logger.info("Disconnected from all drones")

    def arm_all(self) -> bool:
        """Arm all connected drones"""
        success_count = 0
        for drone_id, drone in self.drones.items():
            if drone.is_connected:
                if drone.controller.arm():
                    success_count += 1
                    logger.info(f"Armed drone {drone_id}")
                else:
                    logger.error(f"Failed to arm drone {drone_id}")
            time.sleep(0.5)  # Small delay between arming

        logger.info(f"Armed {success_count}/{len(self.drones)} drones")
        return success_count == len(self.drones)

    def disarm_all(self) -> bool:
        """Disarm all connected drones"""
        success_count = 0
        for drone_id, drone in self.drones.items():
            if drone.is_connected:
                if drone.controller.disarm():
                    success_count += 1
                    logger.info(f"Disarmed drone {drone_id}")
                else:
                    logger.error(f"Failed to disarm drone {drone_id}")
            time.sleep(0.5)

        logger.info(f"Disarmed {success_count}/{len(self.drones)} drones")
        return success_count == len(self.drones)

    def takeoff_all(self, altitude: float) -> bool:
        """Take off all drones to specified altitude"""
        success_count = 0
        for drone_id, drone in self.drones.items():
            if drone.is_connected:
                if drone.controller.takeoff(altitude):
                    success_count += 1
                    logger.info(f"Takeoff command sent to drone {drone_id}")
                else:
                    logger.error(f"Failed to send takeoff to drone {drone_id}")
            time.sleep(1.0)  # Longer delay for takeoff

        logger.info(f"Takeoff sent to {success_count}/{len(self.drones)} drones")
        return success_count == len(self.drones)

    def land_all(self) -> bool:
        """Land all drones"""
        success_count = 0
        for drone_id, drone in self.drones.items():
            if drone.is_connected:
                if drone.controller.land():
                    success_count += 1
                    logger.info(f"Land command sent to drone {drone_id}")
                else:
                    logger.error(f"Failed to send land to drone {drone_id}")
            time.sleep(0.5)

        logger.info(f"Land sent to {success_count}/{len(self.drones)} drones")
        return success_count == len(self.drones)

    def calculate_formation_positions(self, formation_type: str, interval: float, angle: float = 0.0) -> Dict[str, Tuple[float, float]]:
        """
        Calculate relative positions for each drone in the formation
        Returns: Dict with drone_id -> (x, y) relative positions
        """
        drone_ids = list(self.drones.keys())
        num_drones = len(drone_ids)
        positions = {}

        if formation_type.lower() == 'line':
            # Line formation - drones in a straight line
            for i, drone_id in enumerate(drone_ids):
                # Center the line around origin
                x_offset = (i - (num_drones - 1) / 2) * interval
                y_offset = 0
                
                # Apply rotation angle
                x = x_offset * math.cos(math.radians(angle)) - y_offset * math.sin(math.radians(angle))
                y = x_offset * math.sin(math.radians(angle)) + y_offset * math.cos(math.radians(angle))
                
                positions[drone_id] = (x, y)

        elif formation_type.lower() == 'triangle' and num_drones == 3:
            # Equilateral triangle formation
            angles = [0, 120, 240]  # degrees
            for i, drone_id in enumerate(drone_ids):
                drone_angle = math.radians(angles[i] + angle)
                x = interval * math.cos(drone_angle)
                y = interval * math.sin(drone_angle)
                positions[drone_id] = (x, y)

        elif formation_type.lower() == 'square' and num_drones == 4:
            # Square formation
            angles = [45, 135, 225, 315]  # degrees
            for i, drone_id in enumerate(drone_ids):
                drone_angle = math.radians(angles[i] + angle)
                # For square, use interval as distance from center to corner
                distance = interval * math.sqrt(2) / 2
                x = distance * math.cos(drone_angle)
                y = distance * math.sin(drone_angle)
                positions[drone_id] = (x, y)

        elif formation_type.lower() == 'pentagon' and num_drones == 5:
            # Pentagon formation
            for i, drone_id in enumerate(drone_ids):
                drone_angle = math.radians(i * 72 + angle)  # 72 degrees between each drone
                x = interval * math.cos(drone_angle)
                y = interval * math.sin(drone_angle)
                positions[drone_id] = (x, y)

        elif formation_type.lower() == 'hexagon' and num_drones == 6:
            # Hexagon formation
            for i, drone_id in enumerate(drone_ids):
                drone_angle = math.radians(i * 60 + angle)  # 60 degrees between each drone
                x = interval * math.cos(drone_angle)
                y = interval * math.sin(drone_angle)
                positions[drone_id] = (x, y)

        elif formation_type.lower() == 'circle':
            # Circle formation for any number of drones
            angle_step = 360.0 / num_drones
            for i, drone_id in enumerate(drone_ids):
                drone_angle = math.radians(i * angle_step + angle)
                x = interval * math.cos(drone_angle)
                y = interval * math.sin(drone_angle)
                positions[drone_id] = (x, y)

        else:
            logger.error(f"Invalid formation type '{formation_type}' for {num_drones} drones")
            return {}

        return positions

    def xy_to_latlon(self, center_lat: float, center_lon: float, x: float, y: float) -> Tuple[float, float]:
        """
        Convert local x,y coordinates (in meters) to lat/lon coordinates
        """
        # Earth radius in meters
        earth_radius = 6378137.0
        
        # Convert x,y to lat/lon offsets
        lat_offset = y / earth_radius * (180.0 / math.pi)
        lon_offset = x / (earth_radius * math.cos(math.radians(center_lat))) * (180.0 / math.pi)
        
        target_lat = center_lat + lat_offset
        target_lon = center_lon + lon_offset
        
        return target_lat, target_lon

    def set_formation(self, formation_type: str, interval: float, angle: float = 0.0, center_position: Optional[Tuple[float, float, float]] = None) -> bool:
        """
        Set the swarm formation
        Args:
            formation_type: Type of formation (line, triangle, square, pentagon, hexagon, circle)
            interval: Distance between drones in meters
            angle: Rotation angle of the formation in degrees
            center_position: (lat, lon, alt) center position, if None uses first drone's position
        """
        if not self.drones:
            logger.error("No drones in swarm")
            return False

        # Calculate relative positions
        formation_positions = self.calculate_formation_positions(formation_type, interval, angle)
        if not formation_positions:
            return False

        # Get center position
        if center_position:
            center_lat, center_lon, center_alt = center_position
        else:
            # Use first connected drone's position as center
            center_drone = None
            for drone in self.drones.values():
                if drone.is_connected:
                    status = drone.get_status()
                    if status.get('position'):
                        center_lat, center_lon = status['position']
                        center_alt = status.get('altitude', 10.0)
                        center_drone = drone
                        break
            
            if not center_drone:
                logger.error("No connected drone found to use as center position")
                return False

        # Calculate target positions for each drone
        success_count = 0
        for drone_id, (x, y) in formation_positions.items():
            if drone_id in self.drones:
                drone = self.drones[drone_id]
                
                # Convert relative position to lat/lon
                target_lat, target_lon = self.xy_to_latlon(center_lat, center_lon, x, y)
                
                # Store formation info
                drone.formation_position = (x, y)
                drone.target_position = (target_lat, target_lon, center_alt)
                
                logger.info(f"Drone {drone_id}: Formation pos ({x:.1f}, {y:.1f}), Target ({target_lat:.6f}, {target_lon:.6f})")
                success_count += 1

        # Store formation info
        self.formation_type = formation_type
        self.formation_interval = interval
        self.formation_angle = angle
        self.center_position = (center_lat, center_lon, center_alt)

        logger.success(f"Formation '{formation_type}' set for {success_count} drones")
        return True

    def execute_formation(self) -> bool:
        """
        Command all drones to move to their formation positions
        """
        if not self.formation_type:
            logger.error("No formation set. Use set_formation() first")
            return False

        success_count = 0
        for drone_id, drone in self.drones.items():
            if drone.is_connected and drone.target_position:
                target_lat, target_lon, target_alt = drone.target_position
                
                # Get current position to calculate distance and angle
                status = drone.get_status()
                if status.get('position'):
                    current_lat, current_lon = status['position']
                    
                    # Calculate distance and bearing to target
                    distance = self.calculate_distance(current_lat, current_lon, target_lat, target_lon)
                    bearing = self.calculate_bearing(current_lat, current_lon, target_lat, target_lon)
                    
                    # Use fly_to_here with calculated distance and bearing
                    if drone.controller.fly_to_here(distance, bearing):
                        logger.info(f"Formation command sent to drone {drone_id} (distance: {distance:.1f}m, bearing: {bearing:.1f}°)")
                        success_count += 1
                    else:
                        logger.error(f"Failed to send formation command to drone {drone_id}")
                else:
                    logger.error(f"Cannot get current position for drone {drone_id}")
            
            time.sleep(0.5)  # Small delay between commands

        logger.info(f"Formation commands sent to {success_count}/{len(self.drones)} drones")
        return success_count == len(self.drones)

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two lat/lon points in meters"""
        R = 6378137.0  # Earth radius in meters
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        return R * c

    def calculate_bearing(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate bearing from point 1 to point 2 in degrees"""
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lon = math.radians(lon2 - lon1)
        
        y = math.sin(delta_lon) * math.cos(lat2_rad)
        x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon)
        
        bearing = math.atan2(y, x)
        bearing = math.degrees(bearing)
        bearing = (bearing + 360) % 360  # Normalize to 0-360
        
        return bearing

    def get_swarm_status(self) -> Dict:
        """Get status of all drones in the swarm"""
        status = {
            'total_drones': len(self.drones),
            'connected_drones': 0,
            'armed_drones': 0,
            'formation_type': self.formation_type,
            'formation_interval': self.formation_interval,
            'formation_angle': self.formation_angle,
            'drones': {}
        }
        
        for drone_id, drone in self.drones.items():
            drone_status = drone.get_status()
            status['drones'][drone_id] = drone_status
            
            if drone_status.get('connected', False):
                status['connected_drones'] += 1
            if drone_status.get('armed', False):
                status['armed_drones'] += 1
        
        return status

    def start_monitoring(self):
        """Start background monitoring of all drones"""
        if not self.is_monitoring:
            self.is_monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            logger.info("Started swarm monitoring")

    def stop_monitoring(self):
        """Stop background monitoring"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
            logger.info("Stopped swarm monitoring")

    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.is_monitoring:
            try:
                # Could add monitoring logic here
                # For now, just sleep
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {str(e)}")

    def cleanup(self):
        """Clean up all resources"""
        self.stop_monitoring()
        self.disconnect_all()


# Global swarm controller instance
swarm_controller = SwarmController()

def check_swarm():
    """Check if swarm has drones"""
    if not swarm_controller.drones:
        click.echo("Error: No drones in swarm. Use 'add' to add drones first.")
        return False
    return True

# Click commands group
@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Swarm Control CLI - Interactive shell for multi-drone operations"""
    if ctx.invoked_subcommand is None:
        # Enter interactive mode
        click.echo("Welcome to the swarm control shell. Type commands or 'quit' to exit.")
        click.echo("Available commands: add, remove, list, connect, disconnect, arm, disarm, takeoff, land")
        click.echo("Formation commands: formation, execute, status, monitor, help, quit")
        click.echo("Type 'help <command>' for detailed help on any command.\n")
        
        while True:
            try:
                cmd_input = input('(swarm) ').strip()
                
                if not cmd_input:
                    continue
                    
                if cmd_input.lower() in ['quit', 'exit', 'q']:
                    swarm_controller.cleanup()
                    click.echo("Goodbye!")
                    break
                
                # Parse command and arguments
                parts = cmd_input.split()
                command = parts[0]
                args = parts[1:] if len(parts) > 1 else []
                
                # Execute command
                try:
                    if command == 'add':
                        if len(args) < 2:
                            click.echo("Usage: add <drone_id> <connection_string>")
                            continue
                        add_drone_command(args[0], args[1])
                    elif command == 'remove':
                        if not args:
                            click.echo("Usage: remove <drone_id>")
                            continue
                        remove_drone_command(args[0])
                    elif command == 'list':
                        list_drones_command()
                    elif command == 'connect':
                        connect_all_command()
                    elif command == 'disconnect':
                        disconnect_all_command()
                    elif command == 'arm':
                        arm_all_command()
                    elif command == 'disarm':
                        disarm_all_command()
                    elif command == 'takeoff':
                        if not args:
                            click.echo("Usage: takeoff <altitude>")
                            continue
                        try:
                            altitude = float(args[0])
                            takeoff_all_command(altitude)
                        except ValueError:
                            click.echo("Error: Invalid altitude value")
                    elif command == 'land':
                        land_all_command()
                    elif command == 'formation':
                        if len(args) < 2:
                            click.echo("Usage: formation <type> <interval> [angle]")
                            click.echo("Types: line, triangle, square, pentagon, hexagon, circle")
                            continue
                        try:
                            formation_type = args[0]
                            interval = float(args[1])
                            angle = float(args[2]) if len(args) > 2 else 0.0
                            formation_command(formation_type, interval, angle)
                        except ValueError:
                            click.echo("Error: Invalid interval or angle value")
                    elif command == 'execute':
                        execute_formation_command()
                    elif command == 'status':
                        status_command()
                    elif command == 'monitor':
                        monitor_command()
                    elif command == 'help':
                        if args:
                            show_command_help(args[0])
                        else:
                            show_general_help()
                    else:
                        click.echo(f"Unknown command: {command}")
                        click.echo("Type 'help' for available commands")
                        
                except Exception as e:
                    click.echo(f"Error executing command: {str(e)}")
                    
            except (KeyboardInterrupt, EOFError):
                swarm_controller.cleanup()
                click.echo("\nGoodbye!")
                break
            except Exception as e:
                click.echo(f"Error: {str(e)}")
                continue

def show_general_help():
    """Show general help information"""
    click.echo("\nSwarm Management Commands:")
    click.echo("  add <drone_id> <connection>     - Add drone to swarm")
    click.echo("  remove <drone_id>               - Remove drone from swarm")
    click.echo("  list                            - List all drones in swarm")
    click.echo("  connect                         - Connect to all drones")
    click.echo("  disconnect                      - Disconnect from all drones")
    click.echo("\nFlight Commands:")
    click.echo("  arm                             - Arm all drones")
    click.echo("  disarm                          - Disarm all drones")
    click.echo("  takeoff <altitude>              - Take off all drones")
    click.echo("  land                            - Land all drones")
    click.echo("\nFormation Commands:")
    click.echo("  formation <type> <interval> [angle] - Set formation")
    click.echo("  execute                         - Execute current formation")
    click.echo("  status                          - Show swarm status")
    click.echo("  monitor                         - Continuous monitoring")
    click.echo("\nFormation types: line, triangle, square, pentagon, hexagon, circle")
    click.echo("  help [command]                  - Show help")
    click.echo("  quit                            - Exit the shell")

def show_command_help(command):
    """Show help for specific command"""
    help_text = {
        'add': "Add drone to swarm\nUsage: add <drone_id> <connection_string>\nExample: add drone1 udp:127.0.0.1:14550",
        'remove': "Remove drone from swarm\nUsage: remove <drone_id>",
        'formation': "Set swarm formation\nUsage: formation <type> <interval> [angle]\nTypes: line, triangle, square, pentagon, hexagon, circle\nExample: formation triangle 10 45",
        'execute': "Execute current formation\nUsage: execute",
        'monitor': "Continuous status monitoring\nUsage: monitor\nPress Ctrl+C to stop"
    }
    
    if command in help_text:
        click.echo(f"\n{help_text[command]}")
    else:
        click.echo(f"No help available for command: {command}")

def add_drone_command(drone_id: str, connection_string: str):
    """Add a drone to the swarm"""
    if swarm_controller.add_drone(drone_id, connection_string):
        click.echo(f"Added drone {drone_id} to swarm")
    else:
        click.echo(f"Failed to add drone {drone_id}")

def remove_drone_command(drone_id: str):
    """Remove a drone from the swarm"""
    if swarm_controller.remove_drone(drone_id):
        click.echo(f"Removed drone {drone_id} from swarm")
    else:
        click.echo(f"Failed to remove drone {drone_id}")

def list_drones_command():
    """List all drones in the swarm"""
    if not swarm_controller.drones:
        click.echo("No drones in swarm")
        return
    
    click.echo(f"\nDrones in swarm ({len(swarm_controller.drones)}):")
    for drone_id, drone in swarm_controller.drones.items():
        status = "Connected" if drone.is_connected else "Disconnected"
        click.echo(f"  {drone_id}: {drone.connection_string} ({status})")

def connect_all_command():
    """Connect to all drones"""
    if check_swarm():
        click.echo("Connecting to all drones...")
        if swarm_controller.connect_all():
            click.echo("All drones connected successfully")
        else:
            click.echo("Some drones failed to connect")

def disconnect_all_command():
    """Disconnect from all drones"""
    if check_swarm():
        swarm_controller.disconnect_all()
        click.echo("Disconnected from all drones")

def arm_all_command():
    """Arm all drones"""
    if check_swarm():
        click.echo("Arming all drones...")
        if swarm_controller.arm_all():
            click.echo("All drones armed successfully")
        else:
            click.echo("Some drones failed to arm")

def disarm_all_command():
    """Disarm all drones"""
    if check_swarm():
        click.echo("Disarming all drones...")
        if swarm_controller.disarm_all():
            click.echo("All drones disarmed successfully")
        else:
            click.echo("Some drones failed to disarm")

def takeoff_all_command(altitude: float):
    """Take off all drones"""
    if check_swarm():
        click.echo(f"Taking off all drones to {altitude}m...")
        if swarm_controller.takeoff_all(altitude):
            click.echo("Takeoff commands sent to all drones")
        else:
            click.echo("Some drones failed to receive takeoff command")

def land_all_command():
    """Land all drones"""
    if check_swarm():
        click.echo("Landing all drones...")
        if swarm_controller.land_all():
            click.echo("Land commands sent to all drones")
        else:
            click.echo("Some drones failed to receive land command")

def formation_command(formation_type: str, interval: float, angle: float = 0.0):
    """Set swarm formation"""
    if check_swarm():
        click.echo(f"Setting {formation_type} formation with {interval}m interval and {angle}° angle...")
        if swarm_controller.set_formation(formation_type, interval, angle):
            click.echo(f"Formation '{formation_type}' set successfully")
        else:
            click.echo("Failed to set formation")

def execute_formation_command():
    """Execute current formation"""
    if check_swarm():
        click.echo("Executing formation...")
        if swarm_controller.execute_formation():
            click.echo("Formation commands sent to all drones")
        else:
            click.echo("Failed to execute formation")

def status_command():
    """Show swarm status"""
    status = swarm_controller.get_swarm_status()
    
    click.echo("\nSwarm Status:")
    click.echo(f"Total Drones: {status['total_drones']}")
    click.echo(f"Connected: {status['connected_drones']}")
    click.echo(f"Armed: {status['armed_drones']}")
    click.echo(f"Formation: {status['formation_type'] or 'None'}")
    
    if status['formation_type']:
        click.echo(f"Interval: {status['formation_interval']}m")
        click.echo(f"Angle: {status['formation_angle']}°")
    
    click.echo("\nIndividual Drone Status:")
    for drone_id, drone_status in status['drones'].items():
        if drone_status.get('connected', False):
            mode = drone_status.get('mode', 'Unknown')
            armed = "Armed" if drone_status.get('armed', False) else "Disarmed"
            alt = drone_status.get('altitude', 0)
            click.echo(f"  {drone_id}: {armed}, Mode: {mode}, Alt: {alt:.1f}m")
        else:
            click.echo(f"  {drone_id}: Disconnected")

def monitor_command():
    """Continuous monitoring"""
    click.echo("Continuous swarm monitoring started. Press Ctrl+C to stop...")
    try:
        while True:
            # Clear screen
            click.echo('\033[2J\033[H', nl=False)
            status_command()
            time.sleep(2)
    except KeyboardInterrupt:
        click.echo("\nMonitoring stopped")

if __name__ == "__main__":
    try:
        cli()
    except KeyboardInterrupt:
        swarm_controller.cleanup()
        click.echo("\nProgram interrupted by user")
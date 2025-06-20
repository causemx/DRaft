import click
import time
from libs.utils import DroneController, FlightMode

# Global drone controller instance
drone_controller = None

def check_connection():
    """Check if drone is connected"""
    if not drone_controller:
        click.echo("Error: Not connected to drone. Use 'connect' first.")
        return False
    return True

# Click commands group
@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Drone Control CLI - Interactive shell for drone operations"""
    if ctx.invoked_subcommand is None:
        # Enter interactive mode
        click.echo("Welcome to the drone control shell. Type commands or 'quit' to exit.")
        click.echo("Available commands: connect, arm, disarm, takeoff, flytohere, land, mode, throttle, status, quit")
        click.echo("Type 'help <command>' for detailed help on any command.\n")
        
        while True:
            try:
                cmd_input = input('(drone) ').strip()
                
                if not cmd_input:
                    continue
                    
                if cmd_input.lower() in ['quit', 'exit', 'q']:
                    if drone_controller:
                        drone_controller.cleanup()
                    click.echo("Goodbye!")
                    break
                
                # Parse command and arguments
                parts = cmd_input.split()
                command = parts[0]
                args = parts[1:] if len(parts) > 1 else []
                
                # Execute command
                try:
                    if command == 'connect':
                        connection_string = args[0] if args else print("Must provide connect string")
                        connect_command(connection_string)
                    elif command == 'arm':
                        arm_command()
                    elif command == 'disarm':
                        disarm_command()
                    elif command == 'takeoff':
                        if not args:
                            click.echo("Error: Please specify altitude. Usage: takeoff <altitude>")
                            continue
                        try:
                            altitude = float(args[0])
                            takeoff_command(altitude)
                        except ValueError:
                            click.echo("Error: Invalid altitude value")
                    elif command == 'flytohere':
                        if len(args) < 2:
                            click.echo("Error: Please specify distance and angle. Usage: flytohere <distance> <angle>")
                            continue
                        try:
                            distance = float(args[0])
                            angle = float(args[1])
                            flytohere_command(distance, angle)
                        except ValueError:
                            click.echo("Error: Invalid distance or angle values")
                    elif command == 'land':
                        land_command()
                    elif command == 'mode':
                        if not args:
                            click.echo("Error: Please specify mode. Usage: mode <mode_name>")
                            continue
                        mode_command(args[0])
                    elif command == 'throttle':
                        if not args:
                            click.echo("Error: Please specify throttle value. Usage: throttle <value>")
                            continue
                        try:
                            value = int(args[0])
                            throttle_command(value)
                        except ValueError:
                            click.echo("Error: Invalid throttle value")
                    elif command == 'status':
                        duration = 3
                        if args:
                            try:
                                duration = int(args[0])
                            except ValueError:
                                pass
                        status_command(duration)
                    elif command == 'monitor':
                        # New continuous monitoring command
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
                if drone_controller:
                    drone_controller.cleanup()
                click.echo("\nGoodbye!")
                break
            except click.Abort:
                continue

def show_general_help():
    """Show general help information"""
    click.echo("\nAvailable commands:")
    click.echo("  connect [connection_string] - Connect to drone")
    click.echo("  arm                         - Arm the drone")
    click.echo("  disarm                      - Disarm the drone")
    click.echo("  takeoff <altitude>          - Take off to specified altitude")
    click.echo("  flytohere <distance> <angle> - Fly distance at angle from current heading")
    click.echo("  land                        - Land the drone")
    click.echo("  mode <mode_name>            - Set flight mode")
    click.echo("  throttle <value>            - Set throttle (0-100)")
    click.echo("  status [duration]           - Show drone status")
    click.echo("  monitor                     - Continuous status monitoring (Ctrl+C to stop)")
    click.echo("  help [command]              - Show help")
    click.echo("  quit                        - Exit the shell")

def show_command_help(command):
    """Show help for specific command"""
    help_text = {
        'connect': "Connect to drone\nUsage: connect [connection_string]\nDefault: udp:172.21.128.1:14550",
        'arm': "Arm the drone\nUsage: arm",
        'disarm': "Disarm the drone\nUsage: disarm",
        'takeoff': "Take off to specified altitude\nUsage: takeoff <altitude>\nExample: takeoff 10",
        'flytohere': "Fly distance at angle from current heading\nUsage: flytohere <distance> <angle>\nExample: flytohere 10 90 (fly 10m to the right)",
        'land': "Land the drone\nUsage: land",
        'mode': "Set flight mode\nUsage: mode <mode_name>\nAvailable modes: STABILIZE, GUIDED, AUTO, LOITER, RTL, LAND, etc.",
        'throttle': "Set throttle value\nUsage: throttle <value>\nRange: 0-100",
        'status': "Show drone status\nUsage: status [duration]\nExample: status 5 (show for 5 seconds)",
        'monitor': "Continuous status monitoring\nUsage: monitor\nPress Ctrl+C to stop monitoring"
    }
    
    if command in help_text:
        click.echo(f"\n{help_text[command]}")
    else:
        click.echo(f"No help available for command: {command}")

def connect_command(connection_string):
    """Connect to the drone"""
    global drone_controller
    drone_controller = DroneController(connection_string)
    if drone_controller.connect():
        click.echo(f"Successfully connected to {connection_string}")
    else:
        click.echo("Failed to connect")
        drone_controller = None

def arm_command():
    """Arm the drone"""
    if check_connection():
        click.echo("Arming drone...")
        if drone_controller.arm():
            click.echo("Drone armed successfully")
        else:
            click.echo("Arming failed")

def disarm_command():
    """Disarm the drone"""
    if check_connection():
        click.echo("Disarming drone...")
        if drone_controller.disarm():
            click.echo("Drone disarmed successfully")
        else:
            click.echo("Disarming failed")

def takeoff_command(altitude):
    """Take off to specified altitude"""
    if check_connection():
        if not drone_controller.is_armed:
            click.echo("Arming drone...")
            if not drone_controller.arm():
                click.echo("Failed to arm drone. Aborting takeoff.")
                return
            time.sleep(1)

        click.echo(f"Taking off to {altitude}m...")
        if drone_controller.takeoff(altitude):
            click.echo(f"Takeoff command accepted. Climbing to {altitude}m")
        else:
            click.echo("Takeoff command failed")

def flytohere_command(distance, angle):
    """Command the drone to fly specified distance in a specific direction"""
    if not check_connection():
        return
    
    # Check if drone is armed
    if not drone_controller.is_armed:
        click.echo("Arming drone...")
        if not drone_controller.arm():
            click.echo("Failed to arm drone. Aborting flight.")
            return
        time.sleep(1)  # Wait a moment after arming

    click.echo(f"Flying {distance} meters at angle {angle}° from current heading...")
    
    # Call the fly_to_here method with both distance and angle
    if drone_controller.fly_to_here(distance=distance, angle=angle):
        click.echo("Flight command accepted successfully!")
    else:
        click.echo("Flight command failed")

def land_command():
    """Command the drone to land"""
    if check_connection():
        click.echo("Landing...")
        if drone_controller.land():
            click.echo("Land command accepted. Drone is landing...")
        else:
            click.echo("Land command failed")

def mode_command(mode):
    """Set flight mode"""
    if check_connection():
        try:
            # Try to convert to enum to validate
            mode_upper = mode.upper()
            valid_modes = [m.value for m in FlightMode]

            if mode_upper not in valid_modes:
                click.echo(f"Invalid mode: {mode}")
                click.echo("Available modes:")
                for valid_mode in valid_modes:
                    click.echo(f"  {valid_mode}")
                return

            if drone_controller.set_flight_mode(mode_upper):
                click.echo(f"Flight mode set to {mode_upper}")
            else:
                click.echo("Failed to set flight mode")
        except Exception as e:
            click.echo(f"Error setting mode: {str(e)}")

def throttle_command(value):
    """Set throttle value"""
    if check_connection():
        if drone_controller.set_throttle(value):
            click.echo(f"Throttle set to {value}%")
        else:
            click.echo("Failed to set throttle")

def status_command(duration):
    """Show current drone status"""
    if not check_connection():
        return

    click.echo(f"Monitoring drone status for {duration} seconds:")
    start_time = time.time()
    while time.time() - start_time < duration:
        status = drone_controller.get_drone_status()
        display_status(status)
        time.sleep(1)

    click.echo("Status monitoring ended")

def monitor_command():
    """Continuous status monitoring"""
    if not check_connection():
        return

    click.echo("Continuous status monitoring started. Press Ctrl+C to stop...")
    try:
        while True:
            status = drone_controller.get_drone_status()
            display_status(status, clear_screen=True)
            time.sleep(1)
    except KeyboardInterrupt:
        click.echo("\nStatus monitoring stopped")

def display_status(status, clear_screen=False):
    """Display drone status in a formatted way"""
    if clear_screen:
        # Clear screen using ANSI escape codes
        click.echo('\033[2J\033[H', nl=False)
    
    # Format mode display - handle case where mode might be None, string, or enum
    mode_display = status.get('mode', 'Unknown')
    if mode_display is None:
        mode_display = "Unknown"

    # Basic status line
    click.echo(f"Armed: {status.get('armed', False)}, Mode: {mode_display}, Alt: {status.get('altitude', 0):.1f}m")
    
    # Position info if available
    if status.get('position'):
        lat, lon = status['position']
        click.echo(f"Position: Lat {lat:.6f}, Lon {lon:.6f}")
    
    # Heading info if available
    if status.get('heading') is not None:
        click.echo(f"Heading: {status['heading']:.1f}°")
    
    # Speed info if available
    if status.get('groundspeed') is not None:
        click.echo(f"Ground Speed: {status['groundspeed']:.1f} m/s")
    
    # Battery info if available
    if status.get('battery'):
        battery = status['battery']
        if battery.get('percentage') is not None:
            click.echo(f"Battery: {battery['percentage']}%")
        if battery.get('voltage') is not None:
            click.echo(f"Voltage: {battery['voltage']/1000:.2f}V")
    
    # GPS info if available
    if status.get('gps'):
        gps = status['gps']
        click.echo(f"GPS: Fix Type {gps.get('fix_type', 'Unknown')}, Satellites: {gps.get('satellites_visible', 'Unknown')}")
    
    # System status if available
    if status.get('system_status'):
        click.echo(f"System Status: {status['system_status']}")
    
    if not clear_screen:
        click.echo()  # Add blank line between status updates

# Individual command decorators for CLI usage (optional)
@cli.command()
@click.argument('connection_string', default="udp:172.21.128.1:14550")
def connect(connection_string):
    """Connect to the drone"""
    connect_command(connection_string)

@cli.command()
def arm():
    """Arm the drone"""
    arm_command()

@cli.command()
def disarm():
    """Disarm the drone"""
    disarm_command()

@cli.command()
@click.argument('altitude', type=float)
def takeoff(altitude):
    """Take off to specified altitude"""
    takeoff_command(altitude)

@cli.command()
@click.argument('distance', type=float)
@click.argument('angle', type=float)
def flytohere(distance, angle):
    """Fly distance at angle from current heading"""
    flytohere_command(distance, angle)

@cli.command()
def land():
    """Land the drone"""
    land_command()

@cli.command()
@click.argument('mode_name')
def mode(mode_name):
    """Set flight mode"""
    mode_command(mode_name)

@cli.command()
@click.argument('value', type=int)
def throttle(value):
    """Set throttle value (0-100)"""
    throttle_command(value)

@cli.command()
@click.argument('duration', type=int, default=3)
def status(duration):
    """Show drone status for specified duration"""
    status_command(duration)

@cli.command()
def monitor():
    """Continuous status monitoring"""
    monitor_command()

if __name__ == "__main__":
    try:
        cli()
    except KeyboardInterrupt:
        if drone_controller:
            drone_controller.cleanup()
        click.echo("\nProgram interrupted by user")
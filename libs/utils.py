import sys
import time
import threading
import enum
import queue
from pymavlink import mavutil
import pymavlink.dialects.v20.all as dialect
from datetime import datetime
from loguru import logger

# Configure loguru logger for console output only
logger.remove()  # Remove default sink
logger.add(
    sink=sys.stderr,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    colorize=True,
    level="INFO"
)

# Define flight mode as enum class
class FlightMode(enum.Enum):
    STABILIZE = "STABILIZE"
    GUIDED = "GUIDED"
    AUTO = "AUTO"
    LOITER = "LOITER"
    RTL = "RTL"  # Return to Launch
    LAND = "LAND"
    BRAKE = "BRAKE"
    DRIFT = "DRIFT"
    SPORT = "SPORT"
    FLIP = "FLIP"
    AUTOTUNE = "AUTOTUNE"
    POSHOLD = "POSHOLD"
    THROW = "THROW"
    AVOID_ADSB = "AVOID_ADSB"
    GUIDED_NOGPS = "GUIDED_NOGPS"
    CIRCLE = "CIRCLE"

    @classmethod
    def from_string(cls, mode_str):
        """Convert string to FlightMode enum"""
        try:
            return cls(mode_str.upper())
        except ValueError:
            logger.warning(f"Unknown flight mode: {mode_str}")
            return None

    @classmethod
    def to_string(cls, mode_enum):
        """Convert FlightMode enum to string"""
        if isinstance(mode_enum, cls):
            return mode_enum.value
        return str(mode_enum)

    def __str__(self):
        """String representation for enum value"""
        return self.value

    def __repr__(self):
        """String representation for debugging"""
        return f"FlightMode.{self.name}"

    def to_json(self):
        """Return JSON serializable representation"""
        return self.value

class CommandQueue():
    def __init__(self):
        self.queue = queue.Queue()

    def put(self, command:str) -> None:
        self.queue.put(command)
        logger.info(f"put command: {command} success")
        
    def get(self) -> str:
        command = self.queue.get()
        self.queue.task_done()
        return command

class DroneController:
    def __init__(self, connection_string):
        """
        Initialize drone controller with connection string
        Args:
            connection_string (str): MAVLink connection string
        """
        self.connection_string = connection_string
        self.drone = None
        self.is_armed = False
        self.flight_mode = None  # Will store FlightMode enum
        self.altitude = 0

        # Status tracking variables
        self.current_status = {
            'armed': False,
            'mode': None,
            'altitude': 0,
            'battery': None,
            'gps': None,
            'heading': None,
            'groundspeed': None,
            'position': None,
            'system_status': None
        }
        self.tracking = False
        self.tracker_thread = None

    def start_status_tracking(self):
        """Start the background status tracking thread"""
        if not self.tracking:
            self.tracking = True
            self.tracker_thread = threading.Thread(target=self._status_tracker)
            self.tracker_thread.daemon = True  # Thread will close when main program exits
            self.tracker_thread.start()

    def stop_status_tracking(self):
        """Stop the status tracking thread"""
        self.tracking = False
        if self.tracker_thread:
            self.tracker_thread.join()

    def _status_tracker(self):
        """Background thread function to track drone status"""
        while self.tracking and self.drone:
            try:
                # Receive messages
                msg = self.drone.recv_match(blocking=True, timeout=1.0)
                if msg:
                    msg_type = msg.get_type()
                    timestamp = datetime.now().strftime("%H:%M:%S")

                    # Process different message types
                    if msg_type == 'HEARTBEAT':
                        self.current_status['armed'] = bool(msg.base_mode & dialect.MAV_MODE_FLAG_SAFETY_ARMED)
                        self.current_status['system_status'] = dialect.enums['MAV_STATE'][msg.system_status].name

                        # Update flight mode from heartbeat
                        custom_mode = msg.custom_mode
                        flight_mode_str = mavutil.mode_mapping_acm.get(custom_mode)
                        if flight_mode_str:
                            try:
                                self.flight_mode = FlightMode.from_string(flight_mode_str)
                                self.current_status['mode'] = flight_mode_str
                            except (ValueError, AttributeError):
                                # If not a known enum value, store the string directly
                                self.flight_mode = flight_mode_str
                                self.current_status['mode'] = flight_mode_str

                    elif msg_type == 'GLOBAL_POSITION_INT':
                        self.current_status['altitude'] = msg.relative_alt / 1000  # Convert to meters
                        self.current_status['position'] = (msg.lat / 1e7, msg.lon / 1e7)  # Convert to degrees

                         # Update heading if available
                        if hasattr(msg, 'hdg') and msg.hdg != 0 and msg.hdg != 65535:  # Valid heading values
                            heading = msg.hdg / 100.0 if msg.hdg > 360 else msg.hdg  # Convert if needed
                            self.current_status['heading'] = heading
                            logger.debug(f"Updated heading from GLOBAL_POSITION_INT: {heading}°")

                    elif msg_type == 'VFR_HUD':
                        self.current_status['groundspeed'] = msg.groundspeed
                        self.current_status['heading'] = msg.heading

                    elif msg_type == 'GPS_RAW_INT':
                        self.current_status['gps'] = {
                            'fix_type': msg.fix_type,
                            'satellites_visible': msg.satellites_visible
                        }

                    elif msg_type == 'SYS_STATUS':
                        battery_remaining = msg.battery_remaining if hasattr(msg, 'battery_remaining') else None
                        voltage = msg.voltage_battery if hasattr(msg, 'voltage_battery') else None
                        self.current_status['battery'] = {
                            'percentage': battery_remaining,
                            'voltage': voltage
                        }

            except Exception as e:
                logger.error(f"Error in status tracker: {str(e)}")
                time.sleep(1)  # Prevent tight loop in case of errors

    def connect(self, baudrate=None):
        """
        Establish connection with the drone
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if baudrate is not None:
                self.drone = mavutil.mavlink_connection(self.connection_string, baudrate)
            else:
                self.drone = mavutil.mavlink_connection(self.connection_string)

            self.drone.wait_heartbeat()
            logger.success(f"Connected to drone! (system: {self.drone.target_system}, "
                           f"component: {self.drone.target_component})")

            # Request data streams immediately after connection
            self.request_data_streams()
            # Start status tracking after connection
            self.start_status_tracking()

            return True
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            return False

    def request_data_streams(self):
        """
        Request data streams for position and heading information
        """
        if not self.drone:
            logger.error("No drone connection")
            return False
        
        # Define the streams we want with rates in Hz
        stream_rates = {
            mavutil.mavlink.MAV_DATA_STREAM_ALL: 5,        # Position data at 5Hz
            # mavutil.mavlink.MAV_DATA_STREAM_EXTRA1: 5,          # Attitude and heading at 5Hz
            # mavutil.mavlink.MAV_DATA_STREAM_EXTENDED_STATUS: 2, # System status at 2Hz
            # mavutil.mavlink.MAV_DATA_STREAM_RAW_SENSORS: 2,     # Raw sensor data at 2Hz
            # mavutil.mavlink.MAV_DATA_STREAM_RC_CHANNELS: 1      # RC channel data at 1Hz
        }
        
        # Request each stream
        for stream_id, rate in stream_rates.items():
            self.drone.mav.request_data_stream_send(
                self.drone.target_system,
                self.drone.target_component,
                stream_id,
                rate,  # Rate in Hz
                1      # Start/stop (1=start)
            )
            logger.info(f"Requested data stream {stream_id} at {rate}Hz")
        
        # Small delay to allow streams to start
        time.sleep(0.5)
        
        return True

    def arm(self):
        """
        Arm the drone with retry capability
        Returns:
            bool: True if arming successful, False otherwise
        """
        if not self.drone:
            logger.error("No drone connection")
            return False

        # Set mode to GUIDED
        self.set_flight_mode(FlightMode.GUIDED)
        time.sleep(1)  # Wait for mode change

        # Create arm command message
        arm_message = dialect.MAVLink_command_long_message(
            target_system=self.drone.target_system,
            target_component=self.drone.target_component,
            command=dialect.MAV_CMD_COMPONENT_ARM_DISARM,
            confirmation=0,
            param1=1,  # 1 to arm
            param2=0,
            param3=0,
            param4=0,
            param5=0,
            param6=0,
            param7=0
        )

        # Send the arm message
        self.drone.mav.send(arm_message)
        logger.info("Arm the vehicle")

        # Wait for arm acknowledge
        ack = self.drone.recv_match(type='COMMAND_ACK', blocking=True, timeout=1.0)

        if ack and ack.command == dialect.MAV_CMD_COMPONENT_ARM_DISARM:
            success = (ack.result == dialect.MAV_RESULT_ACCEPTED)
            if success:
                self.is_armed = True
                logger.success("Armed successfully!")
                return True
            else:
                # Log the specific failure reason if available
                result_name = dialect.enums['MAV_RESULT'][ack.result].name if ack.result in dialect.enums['MAV_RESULT'] else f"Unknown ({ack.result})"
                logger.warning(f"Arm attempt failed: {result_name}")
        else:
            logger.warning("No acknowledgment received for arm attempt")

        return False

    def disarm(self):
        """
        Disarm the drone with retry capability
        Args:
            max_retries (int): Maximum number of retry attempts
            retry_delay (float): Delay between retries in seconds
        Returns:
            bool: True if disarming successful, False otherwise
        """
        if not self.drone:
            logger.error("No drone connection")
            return False

        # Create disarm command message
        disarm_message = dialect.MAVLink_command_long_message(
            target_system=self.drone.target_system,
            target_component=self.drone.target_component,
            command=dialect.MAV_CMD_COMPONENT_ARM_DISARM,
            confirmation=0,
            param1=0,  # 0 to disarm
            param2=0,
            param3=0,
            param4=0,
            param5=0,
            param6=0,
            param7=0
        )

        # Send the disarm message
        self.drone.mav.send(disarm_message)

        # Wait for acknowledgment
        ack = self.drone.recv_match(type='COMMAND_ACK', blocking=True, timeout=1.0)

        if ack and ack.command == dialect.MAV_CMD_COMPONENT_ARM_DISARM:
            success = (ack.result == dialect.MAV_RESULT_ACCEPTED)
            if success:
                self.is_armed = False
                logger.success("Disarmed successfully!")
                return True
            else:
                # Log the specific failure reason if available
                result_name = dialect.enums['MAV_RESULT'][ack.result].name\
                      if ack.result in dialect.enums['MAV_RESULT'] else f"Unknown ({ack.result})"
                logger.warning(f"Disarm attempt failed: {result_name}")
        else:
            logger.warning("No acknowledgment received for disarm attempt")

        return False

    def takeoff(self, target_altitude, max_retries=3, retry_delay=2):
        """
        Take off to specified altitude with retry capability
        Args:
            target_altitude (float): Target altitude in meters
            max_retries (int): Maximum number of retry attempts
            retry_delay (float): Delay between retries in seconds
        Returns:
            bool: True if takeoff command accepted, False otherwise
        """
        if not self.drone:
            logger.error("Drone not connected")
            return False

        # Create arm command message
        arm_message = dialect.MAVLink_command_long_message(
            target_system=self.drone.target_system,
            target_component=self.drone.target_component,
            command=dialect.MAV_CMD_COMPONENT_ARM_DISARM,
            confirmation=0,
            param1=1,  # 1 to arm
            param2=0,
            param3=0,
            param4=0,
            param5=0,
            param6=0,
            param7=0
        )

        # Send the arm message
        self.drone.mav.send(arm_message)
        logger.info("Arm the vehicle")
        time.sleep(1)

        # Create takeoff command message
        takeoff_message = dialect.MAVLink_command_long_message(
            target_system=self.drone.target_system,
            target_component=self.drone.target_component,
            command=dialect.MAV_CMD_NAV_TAKEOFF,
            confirmation=0,
            param1=0,
            param2=0,
            param3=0,
            param4=0,
            param5=0,
            param6=0,
            param7=target_altitude
        )

        # Try takeoff with retries using while loop
        attempts = 0
        while attempts < max_retries:
            attempts += 1

            # Send the takeoff message
            self.drone.mav.send(takeoff_message)
            logger.info(f"Takeoff attempt {attempts}/{max_retries} to {target_altitude}m")

            # Wait for acknowledgment
            ack = self.drone.recv_match(type='COMMAND_ACK', blocking=True, timeout=1.0)

            if ack and ack.command == dialect.MAV_CMD_NAV_TAKEOFF:
                success = (ack.result == dialect.MAV_RESULT_ACCEPTED)
                if success:
                    logger.success(f"Takeoff command accepted! Target altitude: {target_altitude}m")
                    self.altitude = target_altitude
                    return True
                else:
                    # Log the specific failure reason if available
                    result_name = dialect.enums['MAV_RESULT'][ack.result].name if ack.result in dialect.enums['MAV_RESULT'] else f"Unknown ({ack.result})"
                    logger.warning(f"Takeoff attempt {attempts} failed: {result_name}")
            else:
                logger.warning(f"No acknowledgment received for takeoff attempt {attempts}")

            # Check if we should retry
            if attempts < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Takeoff failed after {max_retries} attempts")

        return False

    def fly_to_here(self, distance=5.0, angle=0.0, max_retries=3):
        """
        Command the drone to fly to a location in a specific direction using SET_POSITION_TARGET_GLOBAL_INT
        
        Args:
            distance (float): Distance to fly in meters (default: 5.0m)
            angle (float): Angle in degrees relative to current heading (default: 0.0)
                        0 = straight ahead, 90 = right, -90 = left, 180 = behind
            max_retries (int): Maximum number of retry attempts for commands
            
        Returns:
            bool: True if command accepted, False otherwise
        """
        import math
        import time
        
        if not self.drone:
            logger.error("No drone connection")
            return False
        
        # Get current position and heading
        status = self.get_drone_status()
        
        if not status.get('position') or not status.get('heading'):
            logger.error("Cannot get current position or heading")
            return False
        
        current_lat, current_lon = status['position']
        heading = status['heading']
        
        if heading is None:
            logger.error("Cannot determine current heading")
            return False
        
        # Calculate target heading by adding the angle to current heading
        target_heading = (heading + angle) % 360
        
        # Convert target heading to radians for calculation
        target_heading_rad = math.radians(target_heading)
        
        # Earth radius in meters
        earth_radius = 6378137.0
        
        # Calculate target position using great circle formula
        # Convert distance from meters to radians
        angular_distance = distance / earth_radius
        
        # Calculate target position
        target_lat = math.asin(
            math.sin(math.radians(current_lat)) * math.cos(angular_distance) +
            math.cos(math.radians(current_lat)) * math.sin(angular_distance) * math.cos(target_heading_rad)
        )
        
        target_lon = math.radians(current_lon) + math.atan2(
            math.sin(target_heading_rad) * math.sin(angular_distance) * math.cos(math.radians(current_lat)),
            math.cos(angular_distance) - math.sin(math.radians(current_lat)) * math.sin(target_lat)
        )
        
        # Convert target position back to degrees
        target_lat = math.degrees(target_lat)
        target_lon = math.degrees(target_lon)
        
        logger.info(f"Current position: Lat {current_lat:.6f}, Lon {current_lon:.6f}, Heading {heading}°")
        logger.info(f"Target position: Lat {target_lat:.6f}, Lon {target_lon:.6f}, Distance {distance}m, Angle {angle}°")
        
        # Set flight mode to GUIDED
        if not self.set_flight_mode(FlightMode.GUIDED):
            logger.error("Failed to set GUIDED mode, aborting flight")
            return False
        
        # Wait for mode change
        time.sleep(1)
        
        # Check if drone is armed
        if not self.is_armed:
            logger.info("Drone not armed, attempting to arm...")
            if not self.arm():
                logger.error("Failed to arm drone, aborting flight")
                return False
            # Wait for arming
            time.sleep(1)
        
        # Convert lat/lon to int format expected by MAVLink (degrees * 1e7)
        lat_int = int(target_lat * 1e7)
        lon_int = int(target_lon * 1e7)
        
        # Default altitude: use current + 2m if available, otherwise 10m
        alt = (status.get('altitude', 0) + 2) if status.get('altitude') is not None else 10.0
        
        # Define which fields to use in the SET_POSITION_TARGET_GLOBAL_INT message
        # We're only setting position (lat, lon, alt)
        mask = (
            dialect.POSITION_TARGET_TYPEMASK_VX_IGNORE |
            dialect.POSITION_TARGET_TYPEMASK_VY_IGNORE |
            dialect.POSITION_TARGET_TYPEMASK_VZ_IGNORE |
            dialect.POSITION_TARGET_TYPEMASK_AX_IGNORE |
            dialect.POSITION_TARGET_TYPEMASK_AY_IGNORE |
            dialect.POSITION_TARGET_TYPEMASK_AZ_IGNORE |
            dialect.POSITION_TARGET_TYPEMASK_FORCE_SET |
            dialect.POSITION_TARGET_TYPEMASK_YAW_IGNORE |
            dialect.POSITION_TARGET_TYPEMASK_YAW_RATE_IGNORE
        )
        
        # Create SET_POSITION_TARGET_GLOBAL_INT message
        position_target_msg = dialect.MAVLink_set_position_target_global_int_message(
            time_boot_ms=0,                             # Not used
            target_system=self.drone.target_system,
            target_component=self.drone.target_component,
            coordinate_frame=dialect.MAV_FRAME_GLOBAL_RELATIVE_ALT_INT,  # Altitude relative to home
            type_mask=mask,                             # Use only the position values
            lat_int=lat_int,                            # Latitude (degrees * 1e7)
            lon_int=lon_int,                            # Longitude (degrees * 1e7)
            alt=alt,                                    # Altitude (meters, relative to home)
            vx=0,                                       # X velocity (not used)
            vy=0,                                       # Y velocity (not used)
            vz=0,                                       # Z velocity (not used)
            afx=0,                                      # X acceleration (not used)
            afy=0,                                      # Y acceleration (not used)
            afz=0,                                      # Z acceleration (not used)
            yaw=0,                                      # Yaw (not used)
            yaw_rate=0                                  # Yaw rate (not used)
        )
        
        attempts = 0
        success = False
        
        # Try to send the SET_POSITION_TARGET_GLOBAL_INT message with retries
        while attempts < max_retries and not success:
            attempts += 1
            
            # Send the position target message
            self.drone.mav.send(position_target_msg)
            logger.info(f"Position target command attempt {attempts}/{max_retries}")
            
            # Unlike mission commands, SET_POSITION_TARGET_GLOBAL_INT typically doesn't get a direct ACK
            # We'll use a brief delay and check if mode is still GUIDED as a basic validation
            time.sleep(0.5)
            
            # Check if still in GUIDED mode
            current_mode = self.get_current_mode()
            if current_mode == FlightMode.GUIDED:
                logger.success("Position target command sent in GUIDED mode!")
                success = True
            else:
                logger.warning(f"Not in GUIDED mode after sending command, mode is {current_mode}")
                
            # If attempt failed and we're not at max retries, wait before trying again
            if not success and attempts < max_retries:
                logger.info("Retrying in 1 second...")
                time.sleep(1)
        
        if success:
            logger.success(f"Successfully sent position target command to fly {distance}m at {angle}° angle")
        else:
            logger.error(f"Failed to send position target command after {max_retries} attempts")
        
        return success

    def land(self, max_retries=3, retry_delay=2):
        """
        Command the drone to land with retry capability
        Args:
            max_retries (int): Maximum number of retry attempts
            retry_delay (float): Delay between retries in seconds
        Returns:
            bool: True if land command accepted, False otherwise
        """
        if not self.drone:
            logger.error("No drone connection")
            return False

        # Create land command message
        land_message = dialect.MAVLink_command_long_message(
            target_system=self.drone.target_system,
            target_component=self.drone.target_component,
            command=dialect.MAV_CMD_NAV_LAND,
            confirmation=0,
            param1=0,
            param2=0,
            param3=0,
            param4=0,
            param5=0,
            param6=0,
            param7=0
        )

        # Try land with retries using while loop
        attempts = 0
        while attempts < max_retries:
            attempts += 1

            # Send the land message
            self.drone.mav.send(land_message)
            logger.info(f"Land attempt {attempts}/{max_retries}")

            # Wait for acknowledgment
            ack = self.drone.recv_match(type='COMMAND_ACK', blocking=True, timeout=1.0)

            if ack and ack.command == dialect.MAV_CMD_NAV_LAND:
                success = (ack.result == dialect.MAV_RESULT_ACCEPTED)
                if success:
                    logger.success("Land command accepted!")
                    return True
                else:
                    # Log the specific failure reason if available
                    result_name = dialect.enums['MAV_RESULT'][ack.result].name if ack.result in dialect.enums['MAV_RESULT'] else f"Unknown ({ack.result})"
                    logger.warning(f"Land attempt {attempts} failed: {result_name}")
            else:
                logger.warning(f"No acknowledgment received for land attempt {attempts}")

            # Check if we should retry
            if attempts < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Land command failed after {max_retries} attempts")

        return False

    def set_flight_mode(self, mode):
        """
        Set the flight mode of the drone
        Args:
            mode (str or FlightMode): Flight mode to set
        Returns:
            bool: True if mode change successful, False otherwise
        """
        if not self.drone:
            logger.error("No drone connection")
            return False

        try:
            # Convert to FlightMode enum if string is provided
            if isinstance(mode, str):
                mode_enum = FlightMode.from_string(mode)
                if not mode_enum:
                    logger.error(f"Invalid flight mode: {mode}")
                    return False
            else:
                mode_enum = mode

            # Get mode string for mavlink
            mode_str = FlightMode.to_string(mode_enum)

            # Using mavutil's set_mode, as dialect doesn't provide a direct way to set mode
            # with a MAVLink_command_long_message
            self.drone.set_mode(mode_str)
            self.flight_mode = mode_enum
            logger.success(f"Flight mode set to {mode_str}")
            return True
        except Exception as e:
            logger.error(f"Failed to set flight mode: {str(e)}")
            return False

    def set_throttle(self, throttle_value):
        """
        Set the throttle value
        Args:
            throttle_value (int): Throttle percentage (0-100)
        Returns:
            bool: True if throttle set successfully, False otherwise
        """
        if not self.drone or not self.is_armed:
            logger.error("Drone not connected or not armed")
            return False

        if 0 <= throttle_value <= 100:
            pwm = 1000 + (throttle_value * 10)

            # Create RC channels override message
            # Note: This is not a command_long, but a different message type
            # We keep using the drone.mav.rc_channels_override_send method for this
            self.drone.mav.rc_channels_override_send(
                self.drone.target_system,
                self.drone.target_component,
                pwm,    # Throttle channel
                65535, 65535, 65535,  # Other channels (unused)
                65535, 65535, 65535, 65535
            )

            logger.success(f"Throttle set to {throttle_value}%")
            return True
        else:
            logger.error("Invalid throttle value (0-100)")
            return False

    def get_current_mode(self):
        """
        Get the current flight mode of the drone

        Returns:
            FlightMode: Current flight mode enum, or None if not connected
        """
        if not self.drone:
            return None

        try:
            # Create request message command
            request_message = dialect.MAVLink_command_long_message(
                target_system=self.drone.target_system,
                target_component=self.drone.target_component,
                command=dialect.MAV_CMD_REQUEST_MESSAGE,
                confirmation=0,
                param1=dialect.MAVLINK_MSG_ID_HEARTBEAT,  # Message ID for heartbeat
                param2=0,
                param3=0,
                param4=0,
                param5=0,
                param6=0,
                param7=0
            )

            # Send the request message
            self.drone.mav.send(request_message)

            # Wait for heartbeat message to get mode
            msg = self.drone.recv_match(type='HEARTBEAT', blocking=True, timeout=1.0)
            if msg:
                # Convert mode to string using MAVLink mode mapping
                custom_mode = msg.custom_mode
                flight_mode_str = mavutil.mode_mapping_acm.get(custom_mode)

                if flight_mode_str:
                    # Convert to FlightMode enum and update internal tracking
                    try:
                        flight_mode_enum = FlightMode.from_string(flight_mode_str)
                        self.flight_mode = flight_mode_enum
                        logger.info(f"Current flight mode: {flight_mode_str}")
                        return flight_mode_enum
                    except ValueError:
                        logger.warning(f"Unknown flight mode string: {flight_mode_str}")
                        # Still update the internal string representation
                        self.flight_mode = flight_mode_str
                        return flight_mode_str
                else:
                    logger.warning("Couldn't determine flight mode from heartbeat")
            else:
                logger.warning("Couldn't retrieve flight mode - no heartbeat received")

            # Return last known mode if available
            return self.flight_mode

        except Exception as e:
            logger.error(f"Error getting flight mode: {str(e)}")
            return self.flight_mode  # Return last known mode on error

    def get_drone_status(self):
        """
        Get comprehensive drone status information

        Returns:
            dict: Dictionary containing current drone status values
        """
        if not self.drone:
            return {
                'connected': False
            }

        # Get current mode if we don't have it
        if not self.flight_mode:
            self.get_current_mode()

        # Convert enum to string for display if needed
        if isinstance(self.flight_mode, FlightMode):
            mode_display = self.flight_mode.value
        elif isinstance(self.flight_mode, str):
            mode_display = self.flight_mode
        else:
            mode_display = "Unknown"

        # Compile status information
        status = {
            'connected': True,
            'armed': self.is_armed,
            'mode': mode_display,
            'altitude': self.altitude,
        }

        # Add extended status if available
        if hasattr(self, 'current_status'):
            for key, value in self.current_status.items():
                status[key] = value

        return status

    def cleanup(self):
        """
        Cleanup method to be called before program exit
        """
        self.stop_status_tracking()
        if self.drone:
            self.drone.close()
            logger.info("Drone connection closed")

# for test
if __name__ == "__main__":
    q = CommandQueue()
    q.put("connect")
    q.put('arm')
    print(q.get())
    print(q.get())

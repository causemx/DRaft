from typing import Tuple


class Calculator:
    @staticmethod
    def calculate_distance_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> Tuple[float, float]:
        """
        Calculate distance and bearing between two GPS points
        
        Returns:
            (distance_meters, bearing_degrees): Distance and bearing to target
        """
        import math
        
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        # Calculate distance using Haversine formula
        a = (math.sin(delta_lat/2)**2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = 6378137.0 * c  # Earth radius in meters
        
        # Calculate bearing
        y = math.sin(delta_lon) * math.cos(lat2_rad)
        x = (math.cos(lat1_rad) * math.sin(lat2_rad) - 
             math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon))
        bearing = math.atan2(y, x)
        bearing = math.degrees(bearing)
        bearing = (bearing + 360) % 360  # Normalize to 0-360
        
        return distance, bearing
    
    @staticmethod
    def relative_to_gps(base_lat: float, base_lon: float, rel_x: float, rel_y: float) -> Tuple[float, float]:
        """
        Convert relative X,Y position (meters) to GPS coordinates
        
        Args:
            base_lat, base_lon: Base GPS position (leader's position)
            rel_x, rel_y: Relative position in meters (X=East, Y=North)
        
        Returns:
            (target_lat, target_lon): Target GPS coordinates
        """
        import math
        
        # Earth radius in meters
        earth_radius = 6378137.0
        
        # Convert relative position to lat/lon offset
        lat_offset = rel_y / earth_radius * (180.0 / math.pi)
        lon_offset = rel_x / (earth_radius * math.cos(math.radians(base_lat))) * (180.0 / math.pi)
        
        target_lat = base_lat + lat_offset
        target_lon = base_lon + lon_offset
        
        return target_lat, target_lon
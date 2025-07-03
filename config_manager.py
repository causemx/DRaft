#!/usr/bin/env python3
"""
Configuration Manager for Raft-Drone System
Handles loading and managing drone connections and node settings from config.ini
"""

import configparser
import os
import logging
from typing import List, Tuple

class ConfigManager:
    """Configuration manager for drone and node settings"""
    
    def __init__(self, config_file='config.ini'):
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.load_config()
    
    def load_config(self):
        """Load configuration from ini file"""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found")
        
        self.config.read(self.config_file)
        logging.info(f"Configuration loaded from {self.config_file}")
    
    def get_drone_connection(self, drone_index: int) -> str:
        """Get drone connection string by index"""
        section = f'drone{drone_index}'
        if section not in self.config:
            raise ValueError(f"Drone {drone_index} not found in configuration")
        
        return self.config[section]['connect_string']
    
    def get_node_port(self, drone_index: int) -> int:
        """Get node port by drone index"""
        section = f'drone{drone_index}'
        if section not in self.config:
            raise ValueError(f"Drone {drone_index} not found in configuration")
        
        return int(self.config[section]['node_port'])
    
    def get_all_nodes(self) -> List[Tuple[str, int, str]]:
        """Get all node configurations: (node_id, port, connection_string)"""
        nodes = []
        for section in self.config.sections():
            if section.startswith('drone'):
                drone_index = int(section.replace('drone', ''))
                node_id = f"drone{drone_index + 1}"  # drone1, drone2, etc.
                port = int(self.config[section]['node_port'])
                connection = self.config[section]['connect_string']
                nodes.append((node_id, port, connection))
        
        return sorted(nodes, key=lambda x: x[1])  # Sort by port
    
    def get_all_ports(self) -> List[int]:
        """Get all node ports"""
        ports = []
        for section in self.config.sections():
            if section.startswith('drone'):
                ports.append(int(self.config[section]['node_port']))
        return sorted(ports)
    
    def get_drone_index_by_port(self, port: int) -> int:
        """Get drone index by port"""
        for section in self.config.sections():
            if section.startswith('drone'):
                if int(self.config[section]['node_port']) == port:
                    return int(section.replace('drone', ''))
        raise ValueError(f"Port {port} not found in configuration")
    
    def get_node_id_by_port(self, port: int) -> str:
        """Get node ID by port"""
        drone_index = self.get_drone_index_by_port(port)
        return f"drone{drone_index + 1}"
    
    def get_drone_count(self) -> int:
        """Get total number of drones in configuration"""
        count = 0
        for section in self.config.sections():
            if section.startswith('drone'):
                count += 1
        return count
    
    def validate_config(self) -> bool:
        """Validate configuration integrity"""
        try:
            # Check if we have at least one drone
            if self.get_drone_count() == 0:
                logging.error("No drones found in configuration")
                return False
            
            # Check all required fields
            for section in self.config.sections():
                if section.startswith('drone'):
                    if 'connect_string' not in self.config[section]:
                        logging.error(f"Missing connect_string in {section}")
                        return False
                    if 'node_port' not in self.config[section]:
                        logging.error(f"Missing node_port in {section}")
                        return False
                    
                    # Validate port is a number
                    try:
                        int(self.config[section]['node_port'])
                    except ValueError:
                        logging.error(f"Invalid node_port in {section}")
                        return False
            
            # Check for port conflicts
            ports = self.get_all_ports()
            if len(ports) != len(set(ports)):
                logging.error("Duplicate ports found in configuration")
                return False
            
            logging.info("Configuration validation passed")
            return True
            
        except Exception as e:
            logging.error(f"Configuration validation failed: {e}")
            return False
    
    def reload_config(self):
        """Reload configuration from file"""
        self.load_config()
    
    def get_config_summary(self) -> dict:
        """Get a summary of the current configuration"""
        summary = {
            'config_file': self.config_file,
            'drone_count': self.get_drone_count(),
            'nodes': self.get_all_nodes(),
            'ports': self.get_all_ports()
        }
        return summary
    
    def __str__(self) -> str:
        """String representation of configuration"""
        summary = self.get_config_summary()
        return f"ConfigManager({summary['config_file']}, {summary['drone_count']} drones)"
    
    def __repr__(self) -> str:
        """Detailed string representation"""
        return self.__str__()

# Global instance for easy access
_config_manager = None

def get_config_manager(config_file='config.ini') -> ConfigManager:
    """Get the global configuration manager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(config_file)
    return _config_manager

def reload_config():
    """Reload the global configuration"""
    global _config_manager
    if _config_manager:
        _config_manager.reload_config()

if __name__ == "__main__":
    # Test the configuration manager
    try:
        config = ConfigManager()
        print("Configuration Manager Test")
        print("=" * 40)
        print(f"Config file: {config.config_file}")
        print(f"Drone count: {config.get_drone_count()}")
        print(f"Validation: {'PASSED' if config.validate_config() else 'FAILED'}")
        
        print("\nNodes:")
        for node_id, port, connection in config.get_all_nodes():
            print(f"  {node_id}: {connection} on port {port}")
        
    except Exception as e:
        print(f"Error: {e}")
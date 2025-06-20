import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import socket
import json
import threading
import time
from datetime import datetime
from typing import Dict, Optional
import subprocess
import psutil

class RaftMonitorGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Raft Cluster Monitor")
        self.root.geometry("1200x800")
        self.root.configure(bg='#2b2b2b')
        
        # Cluster configuration
        self.cluster_ports = [5566, 5567, 5568, 5569]
        self.monitor_port = 15600
        self.nodes_status = {}
        self.node_processes = {}  # Track started processes
        
        # Monitoring state
        self.monitoring = False
        self.monitor_thread = None
        
        self.setup_gui()
        self.start_monitoring()
        
    def setup_gui(self):
        """Setup the GUI layout"""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)
        
        # Title
        title_label = ttk.Label(main_frame, text="🚀 Raft Consensus Cluster Monitor", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 20))
        
        # Left panel - Node controls
        self.setup_node_controls(main_frame)
        
        # Right panel - Logs and command interface
        self.setup_log_panel(main_frame)
        
        # Bottom status bar
        self.setup_status_bar(main_frame)
        
    def setup_node_controls(self, parent):
        """Setup node control panel"""
        # Node controls frame
        controls_frame = ttk.LabelFrame(parent, text="Node Controls", padding="10")
        controls_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        
        # Cluster overview
        overview_frame = ttk.LabelFrame(controls_frame, text="Cluster Overview", padding="5")
        overview_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.cluster_status_label = ttk.Label(overview_frame, text="Cluster Status: Unknown", 
                                            font=('Arial', 10, 'bold'))
        self.cluster_status_label.pack()
        
        self.leader_label = ttk.Label(overview_frame, text="Leader: None")
        self.leader_label.pack()
        
        # Node status frames
        self.node_frames = {}
        for i, port in enumerate(self.cluster_ports):
            self.setup_node_frame(controls_frame, i, port)
        
        # Global controls
        global_frame = ttk.LabelFrame(controls_frame, text="Cluster Controls", padding="5")
        global_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(global_frame, text="🚀 Start All Nodes", 
                  command=self.start_all_nodes).pack(fill=tk.X, pady=2)
        ttk.Button(global_frame, text="🛑 Stop All Nodes", 
                  command=self.stop_all_nodes).pack(fill=tk.X, pady=2)
        ttk.Button(global_frame, text="🔄 Refresh Status", 
                  command=self.refresh_all_nodes).pack(fill=tk.X, pady=2)
        
    def setup_node_frame(self, parent, node_id, port):
        """Setup individual node control frame"""
        node_frame = ttk.LabelFrame(parent, text=f"Node {node_id} (:{port})", padding="5")
        node_frame.pack(fill=tk.X, pady=5)
        
        # Status display
        status_frame = ttk.Frame(node_frame)
        status_frame.pack(fill=tk.X)
        
        status_label = ttk.Label(status_frame, text="●", font=('Arial', 12), foreground='red')
        status_label.pack(side=tk.LEFT)
        
        info_label = ttk.Label(status_frame, text="Offline")
        info_label.pack(side=tk.LEFT, padx=(5, 0))
        
        # Control buttons
        button_frame = ttk.Frame(node_frame)
        button_frame.pack(fill=tk.X, pady=(5, 0))
        
        start_btn = ttk.Button(button_frame, text="Start", 
                              command=lambda: self.start_node(node_id, port))
        start_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        stop_btn = ttk.Button(button_frame, text="Stop", 
                             command=lambda: self.stop_node(node_id, port))
        stop_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        kill_btn = ttk.Button(button_frame, text="Kill", 
                             command=lambda: self.kill_node(node_id, port))
        kill_btn.pack(side=tk.LEFT)
        
        # Store references
        self.node_frames[node_id] = {
            'frame': node_frame,
            'status_label': status_label,
            'info_label': info_label,
            'start_btn': start_btn,
            'stop_btn': stop_btn,
            'kill_btn': kill_btn
        }
    
    def setup_log_panel(self, parent):
        """Setup log and command panel"""
        right_frame = ttk.Frame(parent)
        right_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(0, weight=1)
        
        # Command interface
        cmd_frame = ttk.LabelFrame(right_frame, text="Command Interface", padding="5")
        cmd_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Leader command section
        leader_cmd_frame = ttk.Frame(cmd_frame)
        leader_cmd_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(leader_cmd_frame, text="Send to Leader:").pack(side=tk.LEFT)
        self.command_entry = ttk.Entry(leader_cmd_frame, width=30)
        self.command_entry.pack(side=tk.LEFT, padx=(5, 0), fill=tk.X, expand=True)
        self.command_entry.bind('<Return>', lambda e: self.send_command())
        
        ttk.Button(leader_cmd_frame, text="Send", 
                  command=self.send_command).pack(side=tk.RIGHT, padx=(5, 0))
        
        # Node selection for commands
        node_cmd_frame = ttk.Frame(cmd_frame)
        node_cmd_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(node_cmd_frame, text="Send to Node:").pack(side=tk.LEFT)
        self.node_var = tk.StringVar(value="0")
        node_combo = ttk.Combobox(node_cmd_frame, textvariable=self.node_var, 
                                 values=[str(i) for i in range(len(self.cluster_ports))], 
                                 width=10, state="readonly")
        node_combo.pack(side=tk.LEFT, padx=(5, 0))
        
        self.node_command_entry = ttk.Entry(node_cmd_frame, width=25)
        self.node_command_entry.pack(side=tk.LEFT, padx=(5, 0), fill=tk.X, expand=True)
        self.node_command_entry.bind('<Return>', lambda e: self.send_node_command())
        
        ttk.Button(node_cmd_frame, text="Send", 
                  command=self.send_node_command).pack(side=tk.RIGHT, padx=(5, 0))
        
        # Quick commands
        quick_frame = ttk.Frame(cmd_frame)
        quick_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(quick_frame, text="Quick Commands:").pack(side=tk.LEFT)
        ttk.Button(quick_frame, text="Show Status", 
                  command=lambda: self.send_command("status")).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, text="Show Log", 
                  command=lambda: self.send_command("log")).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, text="Add Sample Data", 
                  command=self.add_sample_data).pack(side=tk.LEFT, padx=2)
        
        # Log display
        log_frame = ttk.LabelFrame(right_frame, text="System Logs", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=20, 
                                                 bg='#1e1e1e', fg='#ffffff',
                                                 font=('Consolas', 9))
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Log control buttons
        log_btn_frame = ttk.Frame(log_frame)
        log_btn_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(5, 0))
        
        ttk.Button(log_btn_frame, text="Clear Logs", 
                  command=self.clear_logs).pack(side=tk.LEFT)
        ttk.Button(log_btn_frame, text="Save Logs", 
                  command=self.save_logs).pack(side=tk.LEFT, padx=(5, 0))
        
        # Auto-scroll checkbox
        self.auto_scroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(log_btn_frame, text="Auto-scroll", 
                       variable=self.auto_scroll_var).pack(side=tk.RIGHT)
    
    def setup_status_bar(self, parent):
        """Setup status bar"""
        status_frame = ttk.Frame(parent)
        status_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        
        self.status_label = ttk.Label(status_frame, text="Monitor starting...")
        self.status_label.pack(side=tk.LEFT)
        
        self.time_label = ttk.Label(status_frame, text="")
        self.time_label.pack(side=tk.RIGHT)
        
        # Update time
        self.update_time()
    
    def update_time(self):
        """Update the time display"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.config(text=current_time)
        self.root.after(1000, self.update_time)
    
    def log_message(self, message, level="INFO"):
        """Add message to log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        colored_message = f"[{timestamp}] {level}: {message}\n"
        
        self.log_text.insert(tk.END, colored_message)
        
        if self.auto_scroll_var.get():
            self.log_text.see(tk.END)
    
    def start_monitoring(self):
        """Start the monitoring thread"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self.monitor_loop, daemon=True)
        self.monitor_thread.start()
        self.log_message("Monitoring started")
    
    def stop_monitoring(self):
        """Stop the monitoring thread"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
    
    def monitor_loop(self):
        """Main monitoring loop"""
        while self.monitoring:
            try:
                self.check_all_nodes()
                self.update_cluster_status()
                time.sleep(2)  # Check every 2 seconds
            except Exception as e:
                self.log_message(f"Monitor error: {e}", "ERROR")
    
    def check_all_nodes(self):
        """Check status of all nodes"""
        for i, port in enumerate(self.cluster_ports):
            status = self.get_node_status(port)
            self.nodes_status[i] = status
            self.root.after(0, lambda i=i, status=status: self.update_node_display(i, status))
    
    def get_node_status(self, port) -> Optional[Dict]:
        """Get status from a specific node"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(("localhost", port))
            
            # Send status request (simulate client request)
            request = {
                'type': 'client_request',
                'command': 'status'
            }
            sock.send(json.dumps(request).encode('utf-8'))
            response = sock.recv(4096).decode('utf-8')
            sock.close()
            
            if response:
                return json.loads(response)
            return None
        except:
            return None
    
    def update_node_display(self, node_id, status):
        """Update node display based on status"""
        if node_id not in self.node_frames:
            return
            
        frame_refs = self.node_frames[node_id]
        
        if status is None:
            # Node is offline
            frame_refs['status_label'].config(foreground='red')
            frame_refs['info_label'].config(text="Offline")
            frame_refs['start_btn'].config(state='normal')
            frame_refs['stop_btn'].config(state='disabled')
            frame_refs['kill_btn'].config(state='disabled')
        else:
            # Node is online, check if it responded with actual status
            if 'success' in status and not status.get('success'):
                # Node responded but with error (likely not leader for status command)
                state = status.get('current_state', 'unknown')
                frame_refs['status_label'].config(foreground='orange')
                frame_refs['info_label'].config(text=f"Online ({state})")
            else:
                # Assume node is running normally
                frame_refs['status_label'].config(foreground='green')
                frame_refs['info_label'].config(text="Online")
            
            frame_refs['start_btn'].config(state='disabled')
            frame_refs['stop_btn'].config(state='normal')
            frame_refs['kill_btn'].config(state='normal')
    
    def update_cluster_status(self):
        """Update overall cluster status"""
        online_nodes = sum(1 for status in self.nodes_status.values() if status is not None)
        total_nodes = len(self.cluster_ports)
        
        # Try to find leader by checking which node accepts commands
        leader_node = None
        for i, port in enumerate(self.cluster_ports):
            if self.nodes_status.get(i) is not None:
                # Try to send a status command to see if it's the leader
                leader_status = self.send_command_to_node(port, "status")
                if leader_status and leader_status.get('success'):
                    leader_node = i
                    break
        
        # Update UI
        def update_ui():
            status_text = f"Cluster Status: {online_nodes}/{total_nodes} nodes online"
            if online_nodes > total_nodes // 2:
                status_text += " (Healthy)"
                self.cluster_status_label.config(foreground='green')
            else:
                status_text += " (Degraded)"
                self.cluster_status_label.config(foreground='red')
            
            self.cluster_status_label.config(text=status_text)
            
            if leader_node is not None:
                self.leader_label.config(text=f"Leader: Node {leader_node}")
            else:
                self.leader_label.config(text="Leader: None")
            
            self.status_label.config(text=f"Monitoring: {online_nodes}/{total_nodes} nodes")
        
        self.root.after(0, update_ui)
    
    def start_node(self, node_id, port):
        """Start a specific node"""
        try:
            if node_id in self.node_processes:
                self.log_message(f"Node {node_id} is already running", "WARNING")
                return
            
            # Start the node process
            cmd = ["python", "raft_implementation.py", str(port)]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.node_processes[node_id] = process
            
            self.log_message(f"Started Node {node_id} on port {port}")
            
        except Exception as e:
            self.log_message(f"Failed to start Node {node_id}: {e}", "ERROR")
    
    def stop_node(self, node_id, port):
        """Gracefully stop a specific node"""
        try:
            if node_id in self.node_processes:
                process = self.node_processes[node_id]
                process.terminate()
                process.wait(timeout=5)
                del self.node_processes[node_id]
                self.log_message(f"Stopped Node {node_id}")
            else:
                # Try to find and stop by port
                self.kill_process_by_port(port)
                self.log_message(f"Stopped Node {node_id} (by port)")
                
        except Exception as e:
            self.log_message(f"Failed to stop Node {node_id}: {e}", "ERROR")
    
    def kill_node(self, node_id, port):
        """Forcefully kill a specific node"""
        try:
            if node_id in self.node_processes:
                process = self.node_processes[node_id]
                process.kill()
                del self.node_processes[node_id]
                self.log_message(f"Killed Node {node_id}")
            else:
                # Try to find and kill by port
                killed = self.kill_process_by_port(port)
                if killed:
                    self.log_message(f"Killed Node {node_id} (by port)")
                else:
                    self.log_message(f"Node {node_id} not found", "WARNING")
                    
        except Exception as e:
            self.log_message(f"Failed to kill Node {node_id}: {e}", "ERROR")
    
    def kill_process_by_port(self, port):
        """Kill process listening on specific port"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'connections']):
                try:
                    connections = proc.info['connections']
                    for conn in connections:
                        if conn.laddr.port == port:
                            proc.kill()
                            return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return False
        except:
            return False
    
    def start_all_nodes(self):
        """Start all nodes"""
        for i, port in enumerate(self.cluster_ports):
            if self.nodes_status.get(i) is None:  # Only start if offline
                self.start_node(i, port)
                time.sleep(0.5)  # Stagger startup
    
    def stop_all_nodes(self):
        """Stop all nodes"""
        for i, port in enumerate(self.cluster_ports):
            if self.nodes_status.get(i) is not None:  # Only stop if online
                self.stop_node(i, port)
    
    def refresh_all_nodes(self):
        """Manually refresh all node statuses"""
        self.log_message("Refreshing node statuses...")
        self.check_all_nodes()
    
    def send_command(self, command=None):
        """Send command to the leader"""
        if command is None:
            command = self.command_entry.get().strip()
        
        if not command:
            return
        
        # Find the leader and send command
        for i, port in enumerate(self.cluster_ports):
            if self.nodes_status.get(i) is not None:
                response = self.send_command_to_node(port, command)
                if response and response.get('success'):
                    self.log_message(f"Command sent to leader (Node {i}): {command}")
                    self.log_message(f"Response: {response}")
                    self.command_entry.delete(0, tk.END)
                    return
        
        self.log_message("No leader found or command failed", "ERROR")
    
    def send_node_command(self):
        """Send command to specific node"""
        node_id = int(self.node_var.get())
        command = self.node_command_entry.get().strip()
        
        if not command:
            return
        
        port = self.cluster_ports[node_id]
        response = self.send_command_to_node(port, command)
        
        if response:
            self.log_message(f"Command sent to Node {node_id}: {command}")
            self.log_message(f"Response: {response}")
        else:
            self.log_message(f"Failed to send command to Node {node_id}", "ERROR")
        
        self.node_command_entry.delete(0, tk.END)
    
    def send_command_to_node(self, port, command):
        """Send command to specific node"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect(("localhost", port))
            
            request = {
                'type': 'client_request',
                'command': command
            }
            sock.send(json.dumps(request).encode('utf-8'))
            response = sock.recv(4096).decode('utf-8')
            sock.close()
            
            return json.loads(response) if response else None
        except Exception as e:
            return None
    
    def add_sample_data(self):
        """Add sample data through the leader"""
        commands = [
            "SET name=Alice",
            "SET age=30", 
            "SET city=NewYork",
            "GET name",
            "SET counter=1",
            "INCREMENT counter"
        ]
        
        for cmd in commands:
            self.send_command(cmd)
            time.sleep(0.1)
    
    def clear_logs(self):
        """Clear the log display"""
        self.log_text.delete(1.0, tk.END)
    
    def save_logs(self):
        """Save logs to file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"raft_monitor_logs_{timestamp}.txt"
            
            with open(filename, 'w') as f:
                logs = self.log_text.get(1.0, tk.END)
                f.write(logs)
            
            self.log_message(f"Logs saved to {filename}")
            messagebox.showinfo("Success", f"Logs saved to {filename}")
            
        except Exception as e:
            self.log_message(f"Failed to save logs: {e}", "ERROR")
            messagebox.showerror("Error", f"Failed to save logs: {e}")
    
    def run(self):
        """Run the GUI"""
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self.log_message("Shutting down monitor...")
        finally:
            self.stop_monitoring()
            # Clean up any processes we started
            for process in self.node_processes.values():
                try:
                    process.terminate()
                except:
                    pass

def main():
    """Main function"""
    print("🖥️  Starting Raft Cluster GUI Monitor on port 15600...")
    print("This will provide a visual interface to monitor and control Raft nodes.")
    print("Press Ctrl+C to exit.")
 
    app = RaftMonitorGUI()
    app.run()

if __name__ == "__main__":
    main()
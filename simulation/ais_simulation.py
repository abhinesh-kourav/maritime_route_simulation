import pandas as pd
import numpy as np
import json
import time
import math
import random
import datetime
import asyncio
import websockets
import pyais
from geopy.distance import geodesic
from shapely.geometry import LineString, Point
import threading
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('simulation')

# Constants
EARTH_RADIUS = 6371.0  # Earth radius in km
KNOTS_TO_KM_PER_HOUR = 1.852  # 1 knot = 1.852 km/h

class Vessel:
    def __init__(self, mmsi, route_coordinates, speed_knots=12.0):
        """
        Initialize a vessel with route coordinates and speed.
        
        Args:
            mmsi: Maritime Mobile Service Identity number
            route_coordinates: List of [lon, lat] coordinates defining the route
            speed_knots: Vessel speed in knots (nautical miles per hour)
        """
        self.mmsi = mmsi
        # Store original [lon, lat] format
        self.route_coordinates = route_coordinates.copy()
        # Convert to [lat, lon] for geodesic calculations and LineString
        self.route_coords_lat_lon = [[coord[1], coord[0]] for coord in route_coordinates]
        self.speed_knots = speed_knots
        self.speed_km_h = speed_knots * KNOTS_TO_KM_PER_HOUR
        
        # Create a LineString from the route for interpolation
        # LineString uses x, y which correspond to lon, lat in geospatial data
        self.route_line = LineString(self.route_coordinates)
        self.total_distance_km = self._calculate_total_distance()
        
        # Initial position is at the start of the route (in [lon, lat] format)
        self.current_position = self.route_coordinates[0].copy()
        self.current_distance_traveled = 0.0
        self.heading = self._calculate_initial_heading()
        
    def _calculate_total_distance(self):
        """Calculate the total distance of the route in kilometers."""
        total_distance = 0.0
        for i in range(len(self.route_coords_lat_lon) - 1):
            point1 = self.route_coords_lat_lon[i]
            point2 = self.route_coords_lat_lon[i + 1]
            total_distance += geodesic(point1, point2).kilometers
        return total_distance
    
    def _calculate_initial_heading(self):
        """Calculate initial heading based on first two points of the route."""
        if len(self.route_coords_lat_lon) < 2:
            return 0.0
        
        start = self.route_coords_lat_lon[0]
        next_point = self.route_coords_lat_lon[1]
        
        # Calculate bearing between the points
        lat1, lon1 = math.radians(start[0]), math.radians(start[1])
        lat2, lon2 = math.radians(next_point[0]), math.radians(next_point[1])
        
        dlon = lon2 - lon1
        
        y = math.sin(dlon) * math.cos(lat2)
        x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
        bearing = math.degrees(math.atan2(y, x))
        
        # Convert to 0-360 range
        return (bearing + 360) % 360
    
    def _calculate_heading(self, point1, point2):
        """Calculate heading between two points (points in [lat, lon] format)."""
        lat1, lon1 = math.radians(point1[0]), math.radians(point1[1])
        lat2, lon2 = math.radians(point2[0]), math.radians(point2[1])
        
        dlon = lon2 - lon1
        
        y = math.sin(dlon) * math.cos(lat2)
        x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
        bearing = math.degrees(math.atan2(y, x))
        
        # Convert to 0-360 range
        return (bearing + 360) % 360
    
    def update_position(self, time_interval_minutes):
        """
        Update vessel position based on time interval.

        Args:
            time_interval_minutes: Time interval in minutes since the last update

        Returns:
            Dictionary with current position information
        """
        # Calculate distance traveled in this interval
        hours = time_interval_minutes / 60.0
        distance_km = self.speed_km_h * hours

        # Update total distance traveled
        self.current_distance_traveled += distance_km

        # If we've reached the end of the route, stay at the last position
        if self.current_distance_traveled >= self.total_distance_km:
            self.current_position = self.route_coordinates[-1].copy()
            return {
                "position": self.current_position,
                "heading": self.heading,
                "speed": self.speed_knots,
                "complete": True
            }

        # Calculate new position along the route
        fraction = self.current_distance_traveled / self.total_distance_km
        point = self.route_line.interpolate(fraction, normalized=True)
        # Store in [lon, lat] format
        self.current_position = [point.x, point.y]

        # Find the segment to calculate heading
        current_lon, current_lat = self.current_position
        segment_found = False

        for i in range(len(self.route_coordinates) - 1):
            segment_start_lon, segment_start_lat = self.route_coordinates[i]
            segment_end_lon, segment_end_lat = self.route_coordinates[i+1]
            
            # Check if current position falls between segment start and end points
            lon_in_range = False
            lat_in_range = False
            
            if segment_start_lon <= segment_end_lon:
                lon_in_range = segment_start_lon <= current_lon <= segment_end_lon
            else:
                lon_in_range = segment_end_lon <= current_lon <= segment_start_lon
                
            if segment_start_lat <= segment_end_lat:
                lat_in_range = segment_start_lat <= current_lat <= segment_end_lat
            else:
                lat_in_range = segment_end_lat <= current_lat <= segment_start_lat
            
            # If current position falls within the bounding box of the segment
            if lon_in_range and lat_in_range:
                segment_found = True
                # Use lat, lon format for heading calculation
                segment_start = [segment_start_lat, segment_start_lon]
                segment_end = [segment_end_lat, segment_end_lon]
                self.heading = self._calculate_heading(segment_start, segment_end)
                break

        # If no segment found, use the heading from previous calculation
        if not segment_found:
            logger.debug(f"No segment found for vessel {self.mmsi} at position {self.current_position}")

        self.speed_knots = self.speed_knots + (random.random() * 2 - 1) * 2.0
        # Enforce the lower bound
        if self.speed_knots < 5:
            self.speed_knots = 5

        # Enforce the upper bound
        elif self.speed_knots > 20:
            self.speed_knots = 20

        self.speed_km_h = self.speed_knots * KNOTS_TO_KM_PER_HOUR

        return {
            "position": self.current_position,
            "heading": self.heading,
            "speed": self.speed_knots,
            "complete": False
        }
    
    def generate_ais_message(self):
        """Generate an AIS position report (message type 1) for the current position."""
        # Get current position in the right format - extracting lat, lon from lon, lat
        lon, lat = self.current_position
        
        # Create AIS position report using pyais
        try:
            # Create a position report (message type 1)
            msg = pyais.encode_dict({
                'type': 1,  # Position report
                'mmsi': self.mmsi,
                'lat': lat,
                'lon': lon,
                'course': self.heading,  # COG (Course over Ground)
                'heading': round(self.heading),  # True heading
                'speed': round(self.speed_knots,1),  # Speed knots
                'status': 0,  # Underway using engine
                'turn': 0,  # Not turning
                'acc': 0,  # Low accuracy (<10m)
                'maneuver': 0,  # Not special maneuver
                'raim': 0,  # RAIM not in use
                'radio': 0  # Default radio status
            })
            
            return msg
        except Exception as e:
            logger.error(f"Error generating AIS message: {e}")
            return None

class AISSimulator:
    def __init__(self):
        """Initialize the AIS simulator."""
        self.vessels = {}
        self.simulation_running = False
        self.simulation_thread = None
        self.simulation_speed_factor = 1.0
        self.websocket_connections = set()
        
    def add_vessel(self, vessel):
        """Add a vessel to the simulation."""
        self.vessels[vessel.mmsi] = vessel
        logger.info(f"Added vessel with MMSI {vessel.mmsi} to simulation")
    
    def start_simulation(self, interval_minutes=5.0, speed_factor=1.0):
        """
        Start the AIS simulation.
        
        Args:
            interval_minutes: Time interval between AIS messages in minutes
            speed_factor: Simulation speed factor (1.0 = real time)
        """
        if self.simulation_running:
            logger.info("Simulation already running")
            return
        
        self.simulation_running = True
        self.simulation_speed_factor = speed_factor
        
        # Start simulation in a separate thread
        self.simulation_thread = threading.Thread(
            target=self._run_simulation,
            args=(interval_minutes,),
            daemon=True
        )
        self.simulation_thread.start()
        logger.info(f"Started simulation with interval {interval_minutes} minutes and speed factor {speed_factor}")
    
    def stop_simulation(self):
        """Stop the AIS simulation."""
        self.simulation_running = False
        if self.simulation_thread:
            self.simulation_thread.join(timeout=2.0)
            self.simulation_thread = None
        logger.info("Stopped simulation")
    
    def _run_simulation(self, interval_minutes):
        """
        Run the simulation, generating AIS messages at specified intervals.
        
        Args:
            interval_minutes: Time interval between AIS messages in minutes
        """
        start_time = datetime.now()
        simulation_time = start_time
        
        while self.simulation_running:
            active_vessels = 0
            
            # For each vessel, update position and generate AIS message
            for mmsi, vessel in self.vessels.items():
                position_info = vessel.update_position(interval_minutes)
                
                if not position_info["complete"]:
                    active_vessels += 1
                
                # Generate and broadcast AIS message
                ais_message = vessel.generate_ais_message()
                if ais_message:
                    message_obj = {
                        "message": "AIVDM",
                        "mmsi": vessel.mmsi,
                        "timestamp": simulation_time.isoformat(),
                        "payload": ais_message
                    }
                    
                    # Send to all connected WebSocket clients
                    asyncio.run(self._broadcast_message(message_obj))
                    
                    # Debug output
                    logger.debug(f"[{simulation_time}] Generated AIS message for vessel {mmsi} at {position_info['position']}")
            
            # Stop simulation if all vessels have completed their routes
            if active_vessels == 0:
                logger.info("All vessels have completed their routes. Stopping simulation.")
                self.simulation_running = False
                break
            
            # Calculate sleep time based on simulation speed factor
            if self.simulation_speed_factor > 0:
                sleep_seconds = (interval_minutes * 60) / self.simulation_speed_factor
                time.sleep(sleep_seconds)
            else:
                # Speed factor -1: send all messages immediately
                pass
            
            # Update simulation time
            simulation_time += timedelta(minutes=interval_minutes)
    
    async def _broadcast_message(self, message):
        """Broadcast a message to all connected WebSocket clients."""
        if not self.websocket_connections:
            # If no connections, just log the message
            logger.debug(f"No WebSocket connections. Message: {message}")
            return
        
        # Convert message to JSON
        message_json = json.dumps(message)
        
        # Send to all connected clients
        disconnected = set()
        for websocket in self.websocket_connections:
            try:
                await websocket.send(message_json)
            except websockets.exceptions.ConnectionClosed:
                disconnected.add(websocket)
        
        # Remove disconnected clients
        self.websocket_connections -= disconnected
    
    async def handle_websocket_connection(self, websocket):
        """Handle a WebSocket connection."""
        # Register the connection
        self.websocket_connections.add(websocket)
        logger.info(f"New WebSocket connection established. Total connections: {len(self.websocket_connections)}")
        
        try:
            # Keep the connection open until client disconnects
            async for message in websocket:
                # Handle any client messages (like simulation control commands)
                try:
                    data = json.loads(message)
                    if 'command' in data:
                        if data['command'] == 'start':
                            interval = data.get('interval', 5.0)
                            speed_factor = data.get('speed_factor', 1.0)
                            self.start_simulation(interval, speed_factor)
                            await websocket.send(json.dumps({"status": "simulation_started"}))
                        
                        elif data['command'] == 'stop':
                            self.stop_simulation()
                            await websocket.send(json.dumps({"status": "simulation_stopped"}))
                        
                        elif data['command'] == 'set_speed_factor':
                            speed_factor = data.get('speed_factor', 1.0)
                            self.simulation_speed_factor = speed_factor
                            await websocket.send(json.dumps({"status": "speed_factor_updated"}))
                except json.JSONDecodeError:
                    logger.warning(f"Received invalid JSON: {message}")
        
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
        finally:
            # Remove the connection
            self.websocket_connections.remove(websocket)


def create_vessels_from_routes(routes, base_speed=12.0, speed_variation=2.0):
    """
    Create vessel objects from route data.
    
    Args:
        routes: List of route dictionaries from the route generation step
        base_speed: Base vessel speed in knots
        speed_variation: Maximum variation in speed (+/-)
    
    Returns:
        Dictionary of vessel objects keyed by MMSI
    """
    vessels = {}
    
    for i, route in enumerate(routes):
        if 'route_coordinates' not in route or not route['route_coordinates']:
            logger.warning(f"Skipping route {i} - no valid coordinates")
            continue
        
        # Generate a unique MMSI for this vessel (9 digits starting with 2)
        mmsi = 200000000 + random.randint(0, 99999999)
        
        # Assign a random speed around the base speed
        speed = base_speed + (random.random() * 2 - 1) * speed_variation
        
        # Create the vessel object
        vessel = Vessel(mmsi, route['route_coordinates'], speed)
        vessels[mmsi] = vessel
        
        logger.info(f"Created vessel with MMSI {mmsi} following route from {route['start_port']['name']} to {route['end_port']['name']}")
    
    return vessels

# Function to start the WebSocket server
async def start_websocket_server(simulator, host='localhost', port=8765):
    """Start the WebSocket server."""
    server = await websockets.serve(
        simulator.handle_websocket_connection,
        host, port
    )
    logger.info(f"WebSocket server started at ws://{host}:{port}")
    return server

# Main function to tie everything together
async def main_simulation(routes=None, num_vessels=1, interval_minutes=5.0, speed_factor=1.0):
    """Main async function to run the AIS simulation with WebSocket server."""
    # If no routes provided, generate them
    if not routes:
        # Import route generation code
        import simulation.route_generation as mrg
        
        # Generate routes
        generated_routes, _, _ = mrg.main(num_vessels=num_vessels)
        routes = generated_routes
    
    # Create vessel objects from routes
    vessels = create_vessels_from_routes(routes)
    
    # Create simulator
    simulator = AISSimulator()
    
    # Add vessels to simulator
    for mmsi, vessel in vessels.items():
        simulator.add_vessel(vessel)
    
    # Start WebSocket server
    server = await start_websocket_server(simulator)
    
    # Start simulation
    simulator.start_simulation(interval_minutes, speed_factor)
    
    # Keep the server running
    try:
        logger.info("Server running. Press Ctrl+C to exit.")
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        simulator.stop_simulation()
        server.close()
        await server.wait_closed()

# Simple WebSocket client for testing
async def test_client():
    """Test WebSocket client to verify server functionality."""
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as websocket:
        # Start simulation
        await websocket.send(json.dumps({
            "command": "start",
            "interval": 1.0,  # 1 minute interval for testing
            "speed_factor": 10.0  # 10x speed for testing
        }))
        
        # Listen for messages
        for _ in range(10):  # Listen for 10 messages
            message = await websocket.recv()
            logger.info(f"Received: {message}")
        
        # Stop simulation
        await websocket.send(json.dumps({"command": "stop"}))
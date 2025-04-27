import pandas as pd
import numpy as np
import random
import matplotlib.pyplot as plt
from searoute import searoute
import folium
import logging

logger = logging.getLogger("simulation")

# Step 1: Load the port data from CSV
# Using the actual column names from your provided data
def load_port_data(filename):
    """Load port data from CSV file."""
    try:
        ports_df = pd.read_csv(filename)
        
        # Keep only necessary columns and rename for easier use
        ports_df = ports_df[['Main Port Name', 'Latitude', 'Longitude', 'Country Code']]
        ports_df.rename(columns={
            'Main Port Name': 'port_name',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Country Code': 'country'
        }, inplace=True)
        
        # Filter out ports with missing coordinates
        ports_df = ports_df.dropna(subset=['latitude', 'longitude'])
        
        logger.info(f"Loaded {len(ports_df)} ports with valid coordinates")
        return ports_df
    
    except Exception as e:
        logger.error(f"Error loading port data: {e}")
        return None

# Step 2: Select random ports for route generation
def generate_random_routes(ports_dataframe, num_vessels=1):
    """Generate random routes by selecting start and end ports."""
    routes = []
    
    for vessel_id in range(1, num_vessels + 1):
        # Randomly select two different ports
        selected_ports = ports_dataframe.sample(n=2).reset_index(drop=True)
        
        start_port = {
            'name': selected_ports.loc[0, 'port_name'],
            'country': selected_ports.loc[0, 'country'] if 'country' in selected_ports.columns else '',
            'lat': selected_ports.loc[0, 'latitude'],
            'lon': selected_ports.loc[0, 'longitude']
        }
        
        end_port = {
            'name': selected_ports.loc[1, 'port_name'],
            'country': selected_ports.loc[1, 'country'] if 'country' in selected_ports.columns else '',
            'lat': selected_ports.loc[1, 'latitude'],
            'lon': selected_ports.loc[1, 'longitude']
        }
        
        routes.append({
            'vessel_id': f"Vessel_{vessel_id}",
            'start_port': start_port,
            'end_port': end_port
        })
        
        logger.info(f"Generated route for Vessel_{vessel_id}: {start_port['name']} to {end_port['name']}")
    
    return routes

# Step 3: Generate realistic routes using searoute-py
def generate_searoutes(routes):
    """Generate realistic sea routes using searoute-py."""
    for route in routes:
        start = (float(route['start_port']['lon']), float(route['start_port']['lat']))
        end = (float(route['end_port']['lon']), float(route['end_port']['lat']))
        
        try:
            # Generate route using searoute
            logger.info(f"Generating route from {start} to {end}...")
            route_geometry = searoute(start, end)
            
            # Extract coordinates from the route geometry
            if route_geometry and 'geometry' in route_geometry:
                route['route_coordinates'] = route_geometry['geometry']['coordinates']
                logger.info(f"Route generated for {route['vessel_id']} from {route['start_port']['name']} to {route['end_port']['name']} with {len(route['route_coordinates'])} points")
            else:
                logger.warning(f"Failed to generate route for {route['vessel_id']} - Invalid geometry returned")
                route['route_coordinates'] = []
        except Exception as e:
            logger.error(f"Error generating route: {e}")
            route['route_coordinates'] = []
    
    return routes

# Step 4: Visualize the routes using Folium
def visualize_routes(routes, output_file="artifacts/vessel_routes.html"):
    """Create an interactive map visualization of the routes."""
    # Create a map centered at a middle point
    map_center = [0, 0]  # Default center
    
    if routes and len(routes) > 0 and 'route_coordinates' in routes[0]:
        # Find a better center if we have routes
        lats = []
        lons = []
        for route in routes:
            if route['route_coordinates']:
                for coord in route['route_coordinates']:
                    lons.append(coord[0])
                    lats.append(coord[1])
        
        if lats and lons:
            map_center = [sum(lats)/len(lats), sum(lons)/len(lons)]
    
    # Create the map
    m = folium.Map(location=map_center, zoom_start=2)
    
    # Add routes to the map
    for route in routes:
        # Add start port marker
        start_popup = f"Start: {route['start_port']['name']}"
        if route['start_port'].get('country'):
            start_popup += f" ({route['start_port']['country']})"
            
        folium.Marker(
            [route['start_port']['lat'], route['start_port']['lon']],
            popup=start_popup,
            icon=folium.Icon(color='green')
        ).add_to(m)
        
        # Add end port marker
        end_popup = f"End: {route['end_port']['name']}"
        if route['end_port'].get('country'):
            end_popup += f" ({route['end_port']['country']})"
            
        folium.Marker(
            [route['end_port']['lat'], route['end_port']['lon']],
            popup=end_popup,
            icon=folium.Icon(color='red')
        ).add_to(m)
        
        # Add route line if we have coordinates
        if route.get('route_coordinates'):
            # Convert route coordinates from (lon, lat) to (lat, lon) for folium
            route_points = [(coord[1], coord[0]) for coord in route['route_coordinates']]
            
            # Generate a random color for the route
            route_color = f"#{random.randint(0, 0xFFFFFF):06x}"
            
            folium.PolyLine(
                route_points,
                color=route_color,
                weight=2.5,
                opacity=0.8,
                popup=f"{route['vessel_id']}: {route['start_port']['name']} to {route['end_port']['name']}"
            ).add_to(m)
    
    # Save the map
    m.save(output_file)
    logger.info(f"Map saved to {output_file}")
    return output_file

# Step 5: Save route data to CSV
def save_route_data(routes, output_file="artifacts/vessel_routes.csv"):
    """Save route information to a CSV file."""
    routes_df = pd.DataFrame([
        {
            'vessel_id': route['vessel_id'],
            'start_port': route['start_port']['name'],
            'start_country': route['start_port'].get('country', ''),
            'start_lat': route['start_port']['lat'],
            'start_lon': route['start_port']['lon'],
            'end_port': route['end_port']['name'],
            'end_country': route['end_port'].get('country', ''),
            'end_lat': route['end_port']['lat'],
            'end_lon': route['end_port']['lon'],
            'route_points': len(route.get('route_coordinates', [])),
        }
        for route in routes
    ])
    
    routes_df.to_csv(output_file, index=False)
    logger.info(f"Route data saved to {output_file}")
    return output_file

# Main function to execute the entire route generation process
def main(port_file="data/UpdatedPub150.csv", num_vessels=1):
    """Main function to execute the entire route generation process."""
    # Load port data
    logger.info(f"Loading port data from {port_file}...")
    ports_df = load_port_data(port_file)
    
    if ports_df is None or len(ports_df) < 2:
        logger.error("Error: Not enough valid ports in the data file.")
        return None, None
    
    # Generate random routes
    logger.info(f"Generating routes for {num_vessels} vessels...")
    vessel_routes = generate_random_routes(ports_df, num_vessels)
    
    # Generate realistic sea routes
    logger.info("Calculating realistic sea routes...")
    vessel_routes = generate_searoutes(vessel_routes)
    
    # Visualize the routes
    logger.info("Creating visualization...")
    map_file = visualize_routes(vessel_routes)
    
    # Save route data
    data_file = save_route_data(vessel_routes)
    
    return vessel_routes, map_file, data_file
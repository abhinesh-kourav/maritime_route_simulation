import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime
import numpy as np
from math import radians, cos, sin, asin, sqrt
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# Load environment variables from .env file (if present)
load_dotenv()

# Configuration setup
st.set_page_config(
    page_title="AIS Vessel Tracking Dashboard",
    page_icon="🚢",
    layout="wide",
)

# Database connection function
@st.cache_resource
def get_db_engine():
    """Create and return a SQLAlchemy engine"""
    try:
        # Create a connection string
        conn_str = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres')}@" \
                  f"{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'ais_data')}"
        
        # Create engine with connection pooling
        engine = create_engine(
            conn_str, 
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
        return engine
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

# Function to calculate distance between two points using Haversine formula
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on earth (specified in decimal degrees)"""
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

# Data retrieval functions
@st.cache_data(ttl=300)  # Cache data for 5 minutes
def get_vessel_list():
    """Get list of all vessels (MMSIs) in the database"""
    engine = get_db_engine()
    if not engine:
        return []
    
    try:
        query = """
            SELECT v.mmsi, 
                   v.first_seen, 
                   v.last_seen, 
                   v.message_count
            FROM vessels v
            ORDER BY v.last_seen DESC
        """
        return pd.read_sql(query, engine).to_dict('records')
    except Exception as e:
        st.error(f"Error fetching vessel list: {e}")
        return []

@st.cache_data
def get_vessel_track(mmsi, start_time=None, end_time=None):
    """Get track for a specific vessel within optional time window"""
    engine = get_db_engine()
    if not engine:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT 
                mmsi,
                timestamp,
                latitude,
                longitude,
                speed,
                course,
                heading
            FROM ais_messages
            WHERE mmsi = :mmsi AND is_valid = TRUE
        """
        params = {"mmsi": mmsi}
        
        if start_time:
            query += " AND timestamp >= :start_time"
            params["start_time"] = start_time
        
        if end_time:
            query += " AND timestamp <= :end_time"
            params["end_time"] = end_time
        
        query += " ORDER BY timestamp ASC"
        
        return pd.read_sql(text(query), engine, params=params)
    except Exception as e:
        st.error(f"Error fetching vessel track: {e}")
        return pd.DataFrame()

@st.cache_data
def get_vessel_statistics(mmsi, start_time=None, end_time=None):
    """Calculate statistics for a specific vessel within optional time window"""
    track_df = get_vessel_track(mmsi, start_time, end_time)
    
    if track_df.empty:
        return {
            "total_messages": 0,
            "total_distance_km": 0,
            "average_speed_knots": 0,
            "max_speed_knots": 0,
            "duration_hours": 0,
            "start_time": None,
            "end_time": None
        }
    
    # Calculate total distance
    total_distance = 0
    
    if len(track_df) > 1:
        # Calculate distance between consecutive points
        for i in range(len(track_df) - 1):
            lat1, lon1 = track_df.iloc[i]['latitude'], track_df.iloc[i]['longitude']
            lat2, lon2 = track_df.iloc[i+1]['latitude'], track_df.iloc[i+1]['longitude']
            
            # Filter out invalid coordinates
            if (abs(lat1) <= 90 and abs(lon1) <= 180 and 
                abs(lat2) <= 90 and abs(lon2) <= 180):
                distance = haversine_distance(lat1, lon1, lat2, lon2)
                total_distance += distance
    
    # Calculate time statistics
    start_time = track_df['timestamp'].min()
    end_time = track_df['timestamp'].max()
    duration = (end_time - start_time).total_seconds() / 3600  # in hours
    
    # Speed statistics
    avg_speed = track_df['speed'].mean() if 'speed' in track_df else 0
    max_speed = track_df['speed'].max() if 'speed' in track_df else 0
    
    return {
        "total_messages": len(track_df),
        "total_distance_km": round(total_distance, 2),
        "average_speed_knots": round(avg_speed, 2),
        "max_speed_knots": round(max_speed, 2),
        "duration_hours": round(duration, 2),
        "start_time": start_time,
        "end_time": end_time
    }

def get_recent_vessel_positions(limit=50):
    """Get most recent position for each vessel"""
    engine = get_db_engine()
    if not engine:
        return pd.DataFrame()
    
    try:
        query = """
        WITH ranked AS (
            SELECT 
                mmsi,
                timestamp,
                latitude,
                longitude,
                speed,
                course,
                ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) as rn
            FROM ais_messages
            WHERE is_valid = TRUE
        )
        SELECT mmsi, timestamp, latitude, longitude, speed, course
        FROM ranked
        WHERE rn = 1
        ORDER BY timestamp DESC
        LIMIT :limit
        """
        
        return pd.read_sql(text(query), engine, params={"limit": limit})
    except Exception as e:
        st.error(f"Error fetching recent vessel positions: {e}")
        return pd.DataFrame()

# Dashboard UI
def main():
    st.title("🚢 AIS Vessel Tracking Dashboard")
    
    # Dashboard tabs
    tab1, tab2, tab3 = st.tabs(["Vessel Map", "Vessel Tracking", "System Statistics"])
    
    # Tab 1: Map view of all vessels
    with tab1:
        st.header("Vessel Map View")
        
        # Fetch recent vessel positions
        recent_positions = get_recent_vessel_positions(limit=100)
        
        if not recent_positions.empty:
            # Create map with vessel positions
            fig = px.scatter_map(
                recent_positions,
                lat="latitude",
                lon="longitude",
                hover_name="mmsi",
                hover_data=["timestamp", "speed", "course"],
                color_discrete_sequence=["blue"],
                zoom=1,
                height=600
            )
            
            fig.update_layout(
                map_style="open-street-map",
                margin={"r": 0, "t": 0, "l": 0, "b": 0}
            )
            
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("###")
            # Display vessels table
            st.subheader("Recent Vessel Positions")
            st.dataframe(
                recent_positions[["mmsi", "timestamp", "latitude", "longitude", "speed", "course"]]
            )
        else:
            st.info("No vessel positions available. Please check your database connection or data.")
    
    # Tab 2: Vessel tracking and analysis
    with tab2:
        st.header("Vessel Track Analysis")
        
        # Get vessel list
        vessels = get_vessel_list()
        
        if vessels:
            vessel_options = [{"label": f"MMSI: {v['mmsi']} (msgs: {v['message_count']})", "value": v['mmsi']} for v in vessels]
            selected_mmsi = st.selectbox(
                "Select Vessel MMSI",
                options=[v["value"] for v in vessel_options],
                format_func=lambda x: next((v["label"] for v in vessel_options if v["value"] == x), x)
            )
            
            # Time filters
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", 
                                           value=datetime.datetime.now().date() - datetime.timedelta(days=7))
            with col2:
                end_date = st.date_input("End Date", 
                                         value=datetime.datetime.now().date() + datetime.timedelta(days=358))
            
            # Convert to datetime
            start_datetime = datetime.datetime.combine(start_date, datetime.time.min)
            end_datetime = datetime.datetime.combine(end_date, datetime.time.max)
            
            # Get vessel track
            track_df = get_vessel_track(selected_mmsi, start_datetime, end_datetime)
            
            if not track_df.empty:
                # Display statistics
                stats = get_vessel_statistics(selected_mmsi, start_datetime, end_datetime)
                
                st.subheader("Vessel Statistics")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Distance", f"{stats['total_distance_km']} km")
                col2.metric("Average Speed", f"{stats['average_speed_knots']} knots")
                col3.metric("Max Speed", f"{stats['max_speed_knots']} knots")
                
                col1, col2 = st.columns(2)
                col1.metric("Duration", f"{stats['duration_hours']} hours")
                col2.metric("Total Messages", stats['total_messages'])
                
                # Vessel route map
                st.subheader("Vessel Route")
                
                if len(track_df) > 1:
                    # Create map with vessel track
                    fig = px.line_map(
                        track_df,
                        lat="latitude",
                        lon="longitude",
                        hover_name="mmsi",
                        hover_data=["timestamp", "speed", "course"],
                        zoom=2,
                        height=500
                    )
                    
                    # Add points for start and end positions
                    fig.add_trace(
                        go.Scattermap(
                            lat=[track_df.iloc[0]['latitude'], track_df.iloc[-1]['latitude']],
                            lon=[track_df.iloc[0]['longitude'], track_df.iloc[-1]['longitude']],
                            mode="markers",
                            marker=dict(size=[12, 12], color=["green", "red"]),
                            text=["Start", "End"],
                            hoverinfo="text",
                            showlegend=False
                        )
                    )
                    
                    fig.update_layout(
                        map_style="open-street-map",
                        margin={"r": 0, "t": 0, "l": 0, "b": 0}
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    st.markdown("###")
                    # Speed over time chart
                    if 'speed' in track_df.columns:
                        st.subheader("Speed Over Time")
                        speed_fig = px.line(
                            track_df, 
                            x="timestamp", 
                            y="speed",
                            labels={"timestamp": "Time", "speed": "Speed (knots)"},
                            height=300
                        )
                        st.plotly_chart(speed_fig, use_container_width=True)
                    
                    # Raw data table
                    with st.expander("View Raw Data"):
                        st.dataframe(track_df)
                else:
                    st.warning("Not enough data points to create a track visualization")
            else:
                st.warning(f"No track data available for MMSI {selected_mmsi} in the selected time range")
        else:
            st.info("No vessels found in the database. Please check your database connection or data.")
    
    # Tab 3: System statistics
    with tab3:
        st.header("System Statistics")
        
        # Database connection info
        st.subheader("Database Information")
        engine = get_db_engine()
        
        if engine:
            try:
                # Get database statistics
                with engine.connect() as conn:
                    # Total messages query
                    result = conn.execute(text("SELECT COUNT(*) FROM ais_messages"))
                    total_messages = result.scalar()
                    
                    # Total vessels query
                    result = conn.execute(text("SELECT COUNT(*) FROM vessels"))
                    total_vessels = result.scalar()
                    
                    # Invalid messages query
                    result = conn.execute(text("SELECT COUNT(*) FROM ais_messages WHERE NOT is_valid"))
                    invalid_messages = result.scalar()
                    
                    # Time range query
                    result = conn.execute(text("""
                        SELECT 
                            MIN(timestamp) AS earliest_message,
                            MAX(timestamp) AS latest_message
                        FROM ais_messages
                    """))
                    time_range = result.fetchone()
                    
                    # Most active vessels query
                    active_vessels_df = pd.read_sql(text("""
                        SELECT 
                            mmsi,
                            message_count,
                            first_seen,
                            last_seen
                        FROM vessels
                        ORDER BY message_count DESC
                        LIMIT 10
                    """), engine)
                
                # Display metrics
                col1, col2 = st.columns(2)
                col1.metric("Total Messages", f"{total_messages:,}")
                col2.metric("Total Vessels", f"{total_vessels:,}")
                
                col1, col2 = st.columns(2)
                col1.metric("Valid Messages", f"{(total_messages - invalid_messages):,}")
                col2.metric("Invalid Messages", f"{invalid_messages:,}")
                
                st.subheader("Data Time Range")
                if time_range[0] and time_range[1]:
                    col1, col2 = st.columns(2)
                    col1.metric("Earliest Message", time_range[0].strftime("%Y-%m-%d %H:%M:%S"))
                    col2.metric("Latest Message", time_range[1].strftime("%Y-%m-%d %H:%M:%S"))
                    
                    # Calculate time span
                    time_span = time_range[1] - time_range[0]
                    days = time_span.days
                    hours = time_span.seconds // 3600
                    minutes = (time_span.seconds % 3600) // 60
                    
                    st.info(f"Data spans {days} days, {hours} hours, and {minutes} minutes")
                else:
                    st.info("No time range data available")
                
                # Most active vessels
                st.subheader("Most Active Vessels")
                if not active_vessels_df.empty:
                    st.dataframe(active_vessels_df)
                else:
                    st.info("No vessel activity data available")
                
            except Exception as e:
                st.error(f"Error fetching system statistics: {e}")
        else:
            st.error("Unable to connect to database to retrieve system statistics")

if __name__ == "__main__":
    main()
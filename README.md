# Maritime Vessel Route Simulation & Data Engineering Challenge

## Table of Contents
- [Background](#background)
- [Solution Approach](#solution-approach)
- [Setup & Running Instructions](#setup--running-instructions)

---

## Background
This project simulates maritime vessel movements and builds a data engineering pipeline to ingest, store, and analyze AIS (Automatic Identification System) messages.  
The challenge focuses on realistic route generation, AIS message simulation, robust data ingestion, efficient storage, and basic analytics dashoboard.

---

## Solution Approach

### Data Flow Architecture
```bash
Simulator → WebSocket → Data Receiver → PostgreSQL/PostGIS → Dashboard
Our solution follows a pipeline architecture where vessel position data flows from simulation to visualization.

### Core Components

1. **Vessel Simulator**
   - Models realistic vessel movement using geodesic calculations
   - Generates AIS messages with position, heading, and speed
   - Broadcasts data through a WebSocket server

2. **Data Receiver**
   - Connects to the WebSocket stream
   - Validates and processes incoming AIS messages
   - Implements buffered batch inserts for database efficiency
   - Tracks data quality metrics

3. **Spatial Database**
   - Uses PostgreSQL with PostGIS for geographic data
   - Optimized schema design with spatial indexing
   - Stores both message history and vessel metadata

4. **Interactive Dashboard**
   - Real-time map visualization
   - Historical track analysis
   - Performance metrics and statistics
   - Implements caching for responsive user experience

This approach enables a complete vessel tracking pipeline while maintaining flexibility for extension to real-world AIS data sources.

---

## Setup & Running Instructions

- Install PostgreSQL 14 with PostGIS extension. Keep user = "postgres", password="postgres"
- Create a virtual emvironment with Python 3.9
   ```bash
   conda create -n p39 python=3.9
- Activate the new virtual environment
   ```bash
   conda activate p39
- Install required packages using requirements.txt
   ```bash
   pip install -r requirements.txt
- Run the simulation and websocket server  
  ```bash
  python run_simulation.py
- Setup database and initiate data ingestion -> Open a new cmd terminal 
  ```bash
  conda activate p39
  python run_data_receiver.py
- Launch the dashboard on browser -> Open a new cmd terminal
  ```bash
  conda activate p39
  streamlit run dashboard.py

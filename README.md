# Maritime Vessel Route Simulation & Data Engineering Challenge

## Table of Contents
- [Background](#background)
- [Project Overview](#project-overview)
- [Solution Approach](#solution-approach)
- [Setup & Running Instructions](#setup--running-instructions)

---

## Background
This project simulates maritime vessel movements and builds a data engineering pipeline to ingest, store, and analyze AIS (Automatic Identification System) messages.  
The challenge focuses on realistic route generation, AIS message simulation, robust data ingestion, efficient storage, and basic analytics dashoboard.

---

## Project Overview

1. **Route Generation**  
   - Use a port dataset (e.g., World Port Index).
   - Randomly select two ports and generate a realistic vessel route using `searoute-py`.

2. **AIS Simulation**  
   - Simulate vessel movement along the route at fixed intervals (e.g., every 5 minutes).
   - Generate AIS messages using `pyais` and stream them over a WebSocket.
   - Support simulation speed factors (`1.0`, `2.0`, `-1`).

3. **Data Engineering & Storage**  
   - WebSocket client ingests AIS messages.
   - Parse and store into a PostgreSQL database.
   - Handle duplicate, out-of-order, or malformed messages.
   - Implemented indexing for efficient retrieval.

4. **Query & Analytics**  
   - Retrieve vessel trajectory.
   - Calculate total distance traveled and average speed within a time window.
   - Additional analytics features.

---

## Solution Approach

- **Language**: Python
- **Key Libraries**:
  - `searoute-py`: Realistic maritime route generation.
  - `pyais`: AIS message encoding and decoding.
  - `websockets`: WebSocket server and client.
  - `PostgreSQL` or `SQLite` (depending on scalability needs) for data storage.
- **Simulation**:
  - Vessel objects assigned unique MMSI.
  - AIS messages simulated at each waypoint interval.
- **Database Schema**:
  - Store timestamp, MMSI, latitude, longitude, and payload.
  - Indexed by MMSI and timestamp for fast queries.

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

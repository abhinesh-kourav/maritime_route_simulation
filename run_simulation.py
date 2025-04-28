import logging
import asyncio
from simulation.ais_simulation import main_simulation, test_client
import os

os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/simulation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("simulation")

##### INPUT VALUES #####
num_vessels = 3
interval_minutes = 5.0
speed_factor = 10
mode = "server" #"server" or "client"

if __name__ == "__main__":
    import sys
    
    # Default to running the server
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    
    if mode == "server":
        # Run the server
        asyncio.run(main_simulation(num_vessels=num_vessels, interval_minutes=interval_minutes, speed_factor=speed_factor))
    elif mode == "client":
        # Run the test client
        asyncio.run(test_client())
    else:
        logger.error(f"Unknown mode: {mode}. Use 'server' or 'client'.")
import logging
import asyncio
import sys
import argparse
from data_engineering.db_setup import create_database, setup_database
from data_engineering.ais_data_receiver import main_data_receiver

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_receiver.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_receiver")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AIS Data Receiver")
    parser.add_argument("--websocket-uri", default="ws://localhost:8765", 
                      help="WebSocket server URI")
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="ais_data", help="Database name")
    parser.add_argument("--db-user", default="postgres", help="Database user")
    parser.add_argument("--db-password", default="postgres", help="Database password")
    
    args = parser.parse_args()
    
    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "dbname": args.db_name,
        "user": args.db_user,
        "password": args.db_password
    }
    
    # Create database if it doesn't exist
    if not create_database(args.db_host, args.db_port, args.db_user, args.db_password, args.db_name):
        sys.exit(1)
    
    # Set up schema
    if not setup_database(args.db_host, args.db_port, args.db_user, args.db_password, args.db_name):
        sys.exit(1)
    
    logger.info("Database setup completed successfully")

    asyncio.run(main_data_receiver(args.websocket_uri, db_config))
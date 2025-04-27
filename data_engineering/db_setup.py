# db_setup.py
import argparse
import psycopg2
import logging
import sys

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[logging.StreamHandler()]
# )
logger = logging.getLogger("data_receiver")

def create_database(host, port, user, password, dbname):
    """Create the database if it doesn't exist"""
    try:
        # Connect to default database to create our database
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname="postgres"  # Connect to default database
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Check if database exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            if cursor.fetchone():
                logger.info(f"Database '{dbname}' already exists")
            else:
                cursor.execute(f"CREATE DATABASE {dbname}")
                logger.info(f"Database '{dbname}' created successfully")
        
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        return False

def setup_database(host, port, user, password, dbname):
    """Set up database schema and extensions"""
    try:
        # Connect to our database
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )
        
        with conn.cursor() as cursor:
            # Enable PostGIS extension
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            logger.info("PostGIS extension enabled")
            
            # Create AIS messages table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS ais_messages (
                id SERIAL PRIMARY KEY,
                message_id TEXT NOT NULL,
                mmsi INTEGER NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                payload TEXT NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                speed DOUBLE PRECISION,
                course DOUBLE PRECISION,
                heading INTEGER,
                navigation_status INTEGER,
                message_type INTEGER,
                is_valid BOOLEAN DEFAULT TRUE,
                validation_errors TEXT[],
                geom GEOGRAPHY(POINT, 4326),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """)
            
            # Create vessel table for vessel metadata
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS vessels (
                mmsi INTEGER PRIMARY KEY,
                first_seen TIMESTAMP WITH TIME ZONE,
                last_seen TIMESTAMP WITH TIME ZONE,
                message_count INTEGER DEFAULT 0
            );
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_mmsi ON ais_messages(mmsi);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_timestamp ON ais_messages(timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_geom ON ais_messages USING GIST(geom);")
            
            # Add unique constraint to prevent duplicates
            cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_message 
            ON ais_messages(mmsi, timestamp, latitude, longitude);
            """)
            
            # Create data quality table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                total_messages INTEGER NOT NULL,
                valid_messages INTEGER NOT NULL,
                invalid_messages INTEGER NOT NULL,
                duplicate_messages INTEGER NOT NULL,
                malformed_messages INTEGER NOT NULL
            );
            """)
            
            conn.commit()
            logger.info("Database schema created successfully")
            
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error setting up database schema: {e}")
        return False
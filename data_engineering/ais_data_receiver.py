import json
import asyncio
import websockets
import logging
import datetime
import pyais
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import argparse
from typing import Dict, List, Any, Optional, Tuple
import os
from dataclasses import dataclass, asdict

logger = logging.getLogger("data_receiver")

@dataclass
class AISMessage:
    """Data class to store validated AIS message data"""
    message_id: str
    mmsi: int
    timestamp: datetime.datetime
    payload: str
    latitude: float
    longitude: float
    speed: Optional[float] = None
    course: Optional[float] = None
    heading: Optional[float] = None
    navigation_status: Optional[int] = None
    message_type: Optional[int] = None
    is_valid: bool = True
    validation_errors: List[str] = None
    
    def __post_init__(self):
        if self.validation_errors is None:
            self.validation_errors = []


@dataclass
class AISMessage:
    """Data class to store validated AIS message data"""
    message_id: str
    mmsi: int
    timestamp: datetime.datetime
    payload: str
    latitude: float
    longitude: float
    speed: Optional[float] = None
    course: Optional[float] = None
    heading: Optional[float] = None
    navigation_status: Optional[int] = None
    message_type: Optional[int] = None
    is_valid: bool = True
    validation_errors: List[str] = None
    
    def __post_init__(self):
        if self.validation_errors is None:
            self.validation_errors = []

class DataQualityMonitor:
    """Track data quality metrics for the ingestion pipeline"""
    def __init__(self):
        self.total_messages = 0
        self.valid_messages = 0
        self.invalid_messages = 0
        self.duplicate_messages = 0
        self.malformed_messages = 0
        self.last_report_time = datetime.datetime.now()
        self.report_interval = datetime.timedelta(minutes=5)
        
    def record_message(self, is_valid: bool, is_duplicate: bool = False, is_malformed: bool = False):
        """Record a processed message and its quality status"""
        self.total_messages += 1
        
        if is_valid:
            self.valid_messages += 1
        else:
            self.invalid_messages += 1
            
        if is_duplicate:
            self.duplicate_messages += 1
            
        if is_malformed:
            self.malformed_messages += 1
            
        # Generate periodic report
        now = datetime.datetime.now()
        if now - self.last_report_time > self.report_interval:
            self._generate_report()
            self.last_report_time = now
    
    def _generate_report(self):
        """Generate a data quality report"""
        if self.total_messages == 0:
            return
            
        valid_percent = (self.valid_messages / self.total_messages) * 100
        
        logger.info(f"=== DATA QUALITY REPORT ===")
        logger.info(f"Total messages processed: {self.total_messages}")
        logger.info(f"Valid messages: {self.valid_messages} ({valid_percent:.2f}%)")
        logger.info(f"Invalid messages: {self.invalid_messages}")
        logger.info(f"Duplicate messages: {self.duplicate_messages}")
        logger.info(f"Malformed messages: {self.malformed_messages}")
        logger.info(f"===========================")

class DatabaseManager:
    """Manage database connections and operations"""
    def __init__(self, host="localhost", port=5432, dbname="ais_data", 
                 user="postgres", password="postgres"):
        self.connection_params = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password
        }
        self.conn = None
        self.message_buffer = []
        self.buffer_size = 100  # Batch inserts for performance
        self.last_flush_time = datetime.datetime.now()
        self.flush_interval = datetime.timedelta(seconds=5)
        
    def connect(self):
        """Establish connection to the database"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("Connected to the database")
            return True
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
            
    def initialize_database(self):
        """Create tables, indexes, and extensions if they don't exist"""
        if not self.conn:
            if not self.connect():
                return False
                
        try:
            with self.conn.cursor() as cur:
                # Enable PostGIS extension
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                
                # Create AIS messages table
                cur.execute("""
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
                cur.execute("""
                CREATE TABLE IF NOT EXISTS vessels (
                    mmsi INTEGER PRIMARY KEY,
                    first_seen TIMESTAMP WITH TIME ZONE,
                    last_seen TIMESTAMP WITH TIME ZONE,
                    message_count INTEGER DEFAULT 0
                );
                """)
                
                # Create indexes
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_mmsi ON ais_messages(mmsi);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_timestamp ON ais_messages(timestamp);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ais_messages_geom ON ais_messages USING GIST(geom);")
                
                # Add unique constraint to prevent duplicates (same MMSI, timestamp, and location)
                cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_message 
                ON ais_messages(mmsi, timestamp, latitude, longitude);
                """)
                
                self.conn.commit()
                logger.info("Database initialized successfully")
                return True
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            self.conn.rollback()
            return False
            
    def store_message(self, message: AISMessage):
        """Store a single AIS message (adds to buffer)"""
        self.message_buffer.append(message)
        
        now = datetime.datetime.now()
        if len(self.message_buffer) >= self.buffer_size or \
           (now - self.last_flush_time) > self.flush_interval:
            self.flush_buffer()
            
    def flush_buffer(self):
        """Flush the message buffer to the database"""
        if not self.message_buffer:
            return
            
        if not self.conn:
            if not self.connect():
                logger.error("Cannot flush buffer: no database connection")
                return
                
        try:
            with self.conn.cursor() as cur:
                # Insert messages
                values = []
                for msg in self.message_buffer:
                    values.append((
                        msg.message_id,
                        msg.mmsi,
                        msg.timestamp,
                        msg.payload,
                        msg.latitude,
                        msg.longitude,
                        msg.speed,
                        msg.course,
                        msg.heading,
                        msg.navigation_status,
                        msg.message_type,
                        msg.is_valid,
                        msg.validation_errors if msg.validation_errors else None,
                        f"POINT({msg.longitude} {msg.latitude})"
                    ))
                
                execute_values(cur, """
                INSERT INTO ais_messages 
                (message_id, mmsi, timestamp, payload, latitude, longitude, 
                 speed, course, heading, navigation_status, message_type, 
                 is_valid, validation_errors, geom)
                VALUES %s
                ON CONFLICT (mmsi, timestamp, latitude, longitude) DO NOTHING
                """, values, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeographyFromText(%s))")
                
                # Update vessel stats
                mmsi_latest = {}
                for msg in self.message_buffer:
                    if msg.mmsi not in mmsi_latest:
                        mmsi_latest[msg.mmsi] = (msg.timestamp, msg.timestamp)
                    else:
                        first_seen, last_seen = mmsi_latest[msg.mmsi]
                        mmsi_latest[msg.mmsi] = (min(first_seen, msg.timestamp), max(last_seen, msg.timestamp))

                mmsi_values = [(mmsi, first_seen, last_seen) for mmsi, (first_seen, last_seen) in mmsi_latest.items()]

                execute_values(cur, """
                INSERT INTO vessels (mmsi, first_seen, last_seen, message_count)
                VALUES %s
                ON CONFLICT (mmsi) DO UPDATE SET
                first_seen = LEAST(vessels.first_seen, EXCLUDED.first_seen),
                last_seen = GREATEST(vessels.last_seen, EXCLUDED.last_seen),
                message_count = vessels.message_count + 1
                """, mmsi_values, template="(%s, %s, %s, 1)")
                                
                self.conn.commit()
                logger.info(f"Flushed {len(self.message_buffer)} messages to database")
                self.message_buffer = []
                self.last_flush_time = datetime.datetime.now()
        except Exception as e:
            logger.error(f"Error flushing message buffer: {e}")
            self.conn.rollback()
            
    def close(self):
        """Close database connection"""
        if self.conn:
            self.flush_buffer()  # Ensure all buffered messages are written
            self.conn.close()
            logger.info("Database connection closed")


class AISProcessor:
    """Process AIS messages from WebSocket and validate them"""
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.quality_monitor = DataQualityMonitor()
        
    def decode_ais_payload(self, payload: str) -> Optional[Dict[str, Any]]:
        """Decode AIS message payload using pyais"""
        try:
            # Don't strip '!'
            if ',' in payload:
                parts = payload.split(',')
                if len(parts) >= 6:  # Typical AIVDM/AIVDO message format
                    message = pyais.decode(payload)  # full message, with '!' at start
                    return message.asdict()

            return None
        except Exception as e:
            logger.error(f"Error decoding AIS payload: {e}")
            return None
        
    def validate_ais_message(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate AIS message and return (is_valid, error_list)"""
        errors = []
        
        # Check MMSI is valid
        if 'mmsi' not in data or not isinstance(data['mmsi'], int) or data['mmsi'] <= 0:
            errors.append("Invalid MMSI number")
            
        # Check coordinates are valid
        if 'lat' not in data or 'lon' not in data:
            errors.append("Missing coordinates")
        elif not (-90 <= data.get('lat', 0) <= 90):
            errors.append(f"Invalid latitude: {data.get('lat')}")
        elif not (-180 <= data.get('lon', 0) <= 180):
            errors.append(f"Invalid longitude: {data.get('lon')}")
            
        # Ensure minimum required fields are present
        for field in ['msg_type']:
            if field not in data:
                errors.append(f"Missing required field: {field}")
                
        return len(errors) == 0, errors
        
    async def process_message(self, raw_message: str):
        """Process a raw WebSocket message"""
        try:
            # Parse the WebSocket message
            message_data = json.loads(raw_message)
            
            # Get required fields
            mmsi = message_data.get('mmsi')
            timestamp_str = message_data.get('timestamp')
            payload = message_data.get('payload')
            
            if not all([mmsi, timestamp_str, payload]):
                logger.warning(f"Missing required fields in message: {raw_message}")
                self.quality_monitor.record_message(False, is_malformed=True)
                return
                
            # Convert timestamp
            try:
                timestamp = datetime.datetime.fromisoformat(timestamp_str)
            except ValueError:
                logger.warning(f"Invalid timestamp format: {timestamp_str}")
                timestamp = datetime.datetime.now()
            
            # Handle single payload or list of payloads
            if isinstance(payload, list):
                payload_str = payload[0]
            else:
                payload_str = payload
                
            # Decode AIS data
            decoded_data = self.decode_ais_payload(payload_str)
            
            if not decoded_data:
                logger.warning(f"Failed to decode AIS payload: {payload_str}")
                self.quality_monitor.record_message(False, is_malformed=True)
                return
                
            # Validate the decoded data
            is_valid, validation_errors = self.validate_ais_message(decoded_data)
            
            # Create AIS message object
            ais_message = AISMessage(
                message_id=f"{mmsi}_{timestamp.isoformat()}",
                mmsi=mmsi,
                timestamp=timestamp,
                payload=payload_str,
                latitude=decoded_data.get('lat', 0.0),
                longitude=decoded_data.get('lon', 0.0),
                speed=decoded_data.get('speed', None),
                course=decoded_data.get('course', None),
                heading=decoded_data.get('heading', None),
                navigation_status=decoded_data.get('status', None),
                message_type=decoded_data.get('msg_type', None),
                is_valid=is_valid,
                validation_errors=validation_errors
            )
            
            # Record quality metrics
            self.quality_monitor.record_message(is_valid)
            
            # Store in database
            self.db_manager.store_message(ais_message)
            
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in message: {raw_message}")
            self.quality_monitor.record_message(False, is_malformed=True)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.quality_monitor.record_message(False)



async def websocket_client(uri: str, processor: AISProcessor):
    """Connect to WebSocket server and process messages"""
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logger.info(f"Connected to {uri}")
                
                while True:
                    message = await websocket.recv()
                    logger.debug(f"Received message: {message}")
                    await processor.process_message(message)
        except websockets.ConnectionClosed:
            logger.warning("WebSocket connection closed, attempting to reconnect...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            await asyncio.sleep(5)

async def main_data_receiver(websocket_uri: str, db_config: Dict[str, Any]):
    """Main application entry point"""
    db_manager = DatabaseManager(
        host=db_config['host'],
        port=db_config['port'],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password']
    )
    
    if not db_manager.connect():
        logger.error("Failed to connect to database. Exiting.")
        return
        
    if not db_manager.initialize_database():
        logger.error("Failed to initialize database. Exiting.")
        return
    
    processor = AISProcessor(db_manager)
    
    try:
        await websocket_client(websocket_uri, processor)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        db_manager.close()

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
    
    asyncio.run(main_data_receiver(args.websocket_uri, db_config))
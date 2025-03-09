#!/usr/bin/env python3
# Worker service for the EmProps Redis system
import os
import sys
import json
import uuid
import time
import logging
import asyncio
import websockets
from typing import Dict, Any, Optional, List

# Configure logging
def setup_logging():
    """Set up basic logging configuration"""
    # Get the root logger
    root_logger = logging.getLogger()
    
    # Clear any existing handlers to avoid duplicate logs
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)
    
    # Default level from environment or DEBUG for development
    log_level_name = os.environ.get("LOG_LEVEL", "DEBUG")
    log_level = getattr(logging, log_level_name.upper(), logging.DEBUG)
    
    # Set root logger level
    root_logger.setLevel(log_level)
    
    # Create console handler with detailed formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Detailed format for development
    log_format = "%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
    formatter = logging.Formatter(log_format)
    console_handler.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)
    
    return root_logger

# Initialize logging
logger = setup_logging()

# Configuration from environment variables
REDIS_API_HOST = os.environ.get("REDIS_API_HOST", "localhost")
REDIS_API_PORT = int(os.environ.get("REDIS_API_PORT", "8001"))
WORKER_ID = os.environ.get("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "30"))

# WebSocket connection to Redis Hub
REDIS_HUB_WS_URL = f"ws://{REDIS_API_HOST}:{REDIS_API_PORT}/ws/worker/{WORKER_ID}"

# Worker capabilities
WORKER_CAPABILITIES = {
    "gpu": True,
    "cpu": True,
    "memory": "16GB",
    "version": "1.0.0"
}

async def connect_to_hub():
    """Connect to Redis Hub and handle messages"""
    logger.info(f"Connecting to Redis Hub at {REDIS_HUB_WS_URL}")
    
    while True:
        try:
            async with websockets.connect(REDIS_HUB_WS_URL) as websocket:
                logger.info("Connected to Redis Hub")
                
                # Send initial status message
                await websocket.send(json.dumps({
                    "type": "worker_status",
                    "worker_id": WORKER_ID,
                    "status": "idle",
                    "capabilities": WORKER_CAPABILITIES,
                    "timestamp": time.time()
                }))
                
                # Start heartbeat task
                heartbeat_task = asyncio.create_task(send_heartbeat(websocket))
                
                # Main message loop
                try:
                    while True:
                        message = await websocket.recv()
                        await handle_message(websocket, message)
                except Exception as e:
                    logger.error(f"Error in message loop: {str(e)}")
                finally:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass
        
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            logger.info(f"Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

async def send_heartbeat(websocket):
    """Send periodic heartbeat messages to the hub"""
    while True:
        try:
            await websocket.send(json.dumps({
                "type": "heartbeat",
                "worker_id": WORKER_ID,
                "timestamp": time.time()
            }))
            logger.debug(f"Heartbeat sent")
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except Exception as e:
            logger.error(f"Error sending heartbeat: {str(e)}")
            break

async def handle_message(websocket, message_json):
    """Handle incoming messages from the hub"""
    try:
        message = json.loads(message_json)
        message_type = message.get("type")
        
        logger.debug(f"Received message: {message_type}")
        
        if message_type == "connection_established":
            logger.info(f"Connection confirmed: {message.get('message')}")
        
        elif message_type == "job_assignment":
            await process_job(websocket, message)
        
        elif message_type == "heartbeat_response":
            logger.debug(f"Heartbeat acknowledged")
        
        else:
            logger.warning(f"Unknown message type: {message_type}")
    
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message_json}")
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}")

async def process_job(websocket, job_message):
    """Process a job assignment"""
    job_id = job_message.get("job_id")
    logger.info(f"Processing job: {job_id}")
    
    # Update status to busy
    await websocket.send(json.dumps({
        "type": "worker_status",
        "worker_id": WORKER_ID,
        "status": "busy",
        "job_id": job_id,
        "timestamp": time.time()
    }))
    
    try:
        # Simulate job processing
        logger.info(f"Starting job {job_id}")
        await asyncio.sleep(5)  # Simulate work
        logger.info(f"Completed job {job_id}")
        
        # Send job completion message
        await websocket.send(json.dumps({
            "type": "job_completed",
            "worker_id": WORKER_ID,
            "job_id": job_id,
            "result": {
                "status": "success",
                "output": f"Job {job_id} completed successfully"
            },
            "timestamp": time.time()
        }))
        
    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")
        
        # Send job failure message
        await websocket.send(json.dumps({
            "type": "job_failed",
            "worker_id": WORKER_ID,
            "job_id": job_id,
            "error": str(e),
            "timestamp": time.time()
        }))
    
    finally:
        # Update status back to idle
        await websocket.send(json.dumps({
            "type": "worker_status",
            "worker_id": WORKER_ID,
            "status": "idle",
            "timestamp": time.time()
        }))

if __name__ == "__main__":
    try:
        # Print startup banner
        print("""
        ╔════════════════════════════════════════════════════════════╗
        ║                                                            ║
        ║                EmProps Redis Worker                        ║
        ║                                                            ║
        ╚════════════════════════════════════════════════════════════╝
        """)
        
        logger.info(f"Starting worker {WORKER_ID}")
        logger.info(f"Connecting to Redis Hub at {REDIS_API_HOST}:{REDIS_API_PORT}")
        
        # Run the worker
        asyncio.run(connect_to_hub())
    except KeyboardInterrupt:
        logger.info("Worker shutting down")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1)

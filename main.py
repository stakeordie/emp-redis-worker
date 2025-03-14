#!/usr/bin/env python3
# Worker service for the EmProps Redis system
import os
import sys
import json
import uuid
import time
import asyncio
import websockets
from typing import Dict, Any, Optional, List

# Import message models from core module
from core.message_models import (
    MessageType,
    WorkerHeartbeatMessage,
    WorkerStatusMessage,
    CompleteJobMessage,
    BaseMessage
)
from core.utils.logger import logger

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
                
                # Send initial status message
                status_message = WorkerStatusMessage(
                    worker_id=WORKER_ID,
                    status="idle",
                    capabilities=WORKER_CAPABILITIES
                )
                await websocket.send(status_message.model_dump_json())
                
                # Start heartbeat task
                heartbeat_task = asyncio.create_task(send_heartbeat(websocket))
                
                # Main message loop
                try:
                    while True:
                        message = await websocket.recv()
                        await handle_message(websocket, message)
                except Exception as e:
                    print(f"\n\n==== ERROR PROCESSING MESSAGE: {str(e)} ===\n\n")
                finally:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass
        
        except Exception as e:
            await asyncio.sleep(5)

async def send_heartbeat(websocket):
    """Send periodic heartbeat messages to the hub"""
    while True:
        try:
            # Use the proper WorkerHeartbeatMessage model
            heartbeat_message = WorkerHeartbeatMessage(
                worker_id=WORKER_ID,
                status="idle",
                load=0.0
            )
            await websocket.send(heartbeat_message.model_dump_json())
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except Exception as e:
            print(f"\n\n==== ERROR SENDING HEARTBEAT: {str(e)} ===\n\n")
            break

async def handle_message(websocket, message_json):
    """Handle incoming messages from the hub"""
    try:
        message = json.loads(message_json)
        message_type = message.get("type")
        
        if message_type == MessageType.CONNECTION_ESTABLISHED:
            print(f"\n\n==== CONNECTION ESTABLISHED: {message.get('message')} ===\n\n")
        
        elif message_type == MessageType.JOB_ASSIGNED:  # Changed from "job_assignment" to match MessageType
            await process_job(websocket, message)
        
        elif message_type == MessageType.WORKER_HEARTBEAT:  # Changed from "heartbeat_response"
            print(f"\n\n==== HEARTBEAT ACKNOWLEDGED ===\n\n")
        else:
            print(f"\n\n==== UNKNOWN MESSAGE TYPE: {message_type} ===\n\n")
    
    except json.JSONDecodeError:
        print(f"\n\n==== INVALID JSON: {message_json} ===\n\n")
    except Exception as e:
        print(f"\n\n==== ERROR HANDLING MESSAGE: {str(e)} ===\n\n")

async def process_job(websocket, job_message):
    """Process a job assignment"""
    job_id = job_message.get("job_id")
    
    # Update status to busy
    busy_status = WorkerStatusMessage(
        worker_id=WORKER_ID,
        status="busy",
        capabilities={"job_id": job_id}
    )
    await websocket.send(busy_status.model_dump_json())
    
    try:
        # Simulate job processing
        await asyncio.sleep(5)  # Simulate work
        
        # Send job completion message
        complete_message = CompleteJobMessage(
            job_id=job_id,
            machine_id=WORKER_ID,
            gpu_id=0,  # Assuming default GPU ID
            result={
                "status": "success",
                "output": f"Job {job_id} completed successfully"
            }
        )
        await websocket.send(complete_message.model_dump_json())
        
    except Exception as e:
        print(f"\n\n==== ERROR PROCESSING JOB {job_id}: {str(e)} ===\n\n")
        
        # Send job failure message
        # Note: Using CompleteJobMessage with a failure result since there's no specific FailJobMessage
        fail_message = CompleteJobMessage(
            job_id=job_id,
            machine_id=WORKER_ID,
            gpu_id=0,  # Assuming default GPU ID
            result={
                "status": "failed",
                "error": str(e)
            }
        )
        await websocket.send(fail_message.model_dump_json())
    
    finally:
        # Update status back to idle
        idle_status = WorkerStatusMessage(
            worker_id=WORKER_ID,
            status="idle"
        )
        await websocket.send(idle_status.model_dump_json())

if __name__ == "__main__":
    try:
        # Run the worker
        asyncio.run(connect_to_hub())
    except KeyboardInterrupt:
        print(f"\n\n==== WORKER SHUTTING DOWN ===\n\n")
    except Exception as e:
        print(f"\n\n==== UNHANDLED EXCEPTION: {str(e)} ===\n\n")
        sys.exit(1)

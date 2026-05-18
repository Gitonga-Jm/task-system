import os
import sys
import json
import uuid
import asyncio
from datetime import datetime
from typing import Optional

# --- Critical: Print debug info to logs ---
print(f"Python version: {sys.version}")
print("Current working directory:", os.getcwd())
print("Files in directory:", os.listdir('.'))
# -----------------------------------------

try:
    from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, Response
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
    from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
    import redis
    print("✓ All core imports successful")
except Exception as e:
    print(f"✗ CRITICAL: Failed to import core modules: {e}")
    sys.exit(1)

try:
    from models import Task, TaskStatus
    print("✓ Models imported successfully")
except Exception as e:
    print(f"✗ CRITICAL: Failed to import models: {e}")
    sys.exit(1)

# --- App Initialization ---
app = FastAPI(title="Task Queue System")

# --- Redis Connection (with Render-compatible fallback) ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_TLS = os.getenv('REDIS_TLS', 'False').lower() == 'true'

print(f"Attempting Redis connection to {REDIS_HOST}:{REDIS_PORT} (TLS: {REDIS_TLS})")
try:
    if REDIS_PASSWORD:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=REDIS_TLS,
            ssl_cert_reqs=None,
            decode_responses=True,
            socket_connect_timeout=5
        )
    else:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5
        )
    redis_client.ping()
    print("✓ Redis connection successful")
except Exception as e:
    print(f"✗ Redis connection failed: {e}")
    # Don't exit - app can still run without Redis for basic routes
    redis_client = None

# --- Queue Names ---
QUEUE_NAME = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"

# --- Prometheus Metrics ---
tasks_created = Counter('tasks_created_total', 'Total tasks created')
tasks_processed = Counter('tasks_processed_total', 'Total tasks processed')
tasks_failed = Counter('tasks_failed_total', 'Total tasks failed')

# --- Static Files (Dashboard) ---
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    print(f"✓ Static files mounted from {static_dir}")
else:
    print(f"✗ Static directory not found: {static_dir}")

# --- Request/Response Models ---
class TaskCreate(BaseModel):
    type: str
    payload: dict
    max_retries: int = 3

# --- Basic Routes (for testing) ---
@app.get("/")
def root():
    return {"message": "Task Queue System", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/test")
def test():
    return {"test": "successful"}

# --- Main Entry Point ---
PORT = int(os.getenv('PORT', 8000))
if __name__ == "__main__":
    import uvicorn
    print(f"Starting server on port {PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=PORT)

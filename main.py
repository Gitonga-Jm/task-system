import sys
import traceback
print(f"Python version: {sys.version}")
print("Starting main.py import...")

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import redis
import uuid
import json
import os
import asyncio
from models import Task, TaskStatus
from datetime import datetime
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI(title="Task Queue System")

# Redis connection with Render.com support
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

if REDIS_PASSWORD:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
else:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# Queue names
QUEUE_NAME = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"

# Prometheus metrics
tasks_created = Counter('tasks_created_total', 'Total tasks created')
tasks_processed = Counter('tasks_processed_total', 'Total tasks processed')
tasks_failed = Counter('tasks_failed_total', 'Total tasks failed')

# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.task_updates = asyncio.Queue()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"[WebSocket] Client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        print(f"[WebSocket] Client disconnected. Total: {len(self.active_connections)}")

    async def broadcast_task_update(self, task_id: str, status: str, message: str = ""):
        update = {
            "type": "task_update",
            "task_id": task_id,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        await self.task_updates.put(update)

    async def broadcast_stats_update(self, stats: dict):
        update = {
            "type": "stats_update",
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        await self.task_updates.put(update)

    async def broadcast_dead_letter_update(self, dead_letters: list):
        update = {
            "type": "dead_letter_update",
            "dead_letters": dead_letters,
            "timestamp": datetime.now().isoformat()
        }
        await self.task_updates.put(update)

    async def process_updates(self):
        while True:
            update = await self.task_updates.get()
            for connection in self.active_connections[:]:
                try:
                    await connection.send_json(update)
                except:
                    if connection in self.active_connections:
                        self.active_connections.remove(connection)

manager = ConnectionManager()

# Start background task to process WebSocket updates
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(manager.process_updates())

# Serve static files (dashboard)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

class TaskCreate(BaseModel):
    type: str
    payload: dict
    max_retries: int = 3

class Notification(BaseModel):
    type: str
    data: dict

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    dashboard_path = os.path.join(static_dir, "dashboard.html")
    if os.path.exists(dashboard_path):
        with open(dashboard_path, "r") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard not found</h1>")

@app.post("/tasks", status_code=202)
def create_task(task_req: TaskCreate):
    task_id = str(uuid.uuid4())
    
    print(f"[API] Received task type: {task_req.type}")
    print(f"[API] Received payload: {task_req.payload}")
    
    task = Task(
        id=task_id,
        type=task_req.type,
        payload=task_req.payload,
        max_retries=task_req.max_retries,
        retries_remaining=task_req.max_retries,
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow()
    )
    
    # Store task metadata
    redis_client.hset(f"task:{task_id}", "id", task_id)
    redis_client.hset(f"task:{task_id}", "type", task.type)
    redis_client.hset(f"task:{task_id}", "status", task.status.value)
    redis_client.hset(f"task:{task_id}", "retries_remaining", task.retries_remaining)
    redis_client.hset(f"task:{task_id}", "created_at", task.created_at.isoformat())
    redis_client.hset(f"task:{task_id}", "max_retries", task.max_retries)
    
    # Push to queue
    task_json = task.to_json()
    redis_client.rpush(QUEUE_NAME, task_json)
    tasks_created.inc()
    
    # Broadcast WebSocket update
    asyncio.create_task(manager.broadcast_task_update(task_id, "queued", "Task submitted"))
    
    return {
        "task_id": task_id,
        "status": "accepted",
        "queue_position": redis_client.llen(QUEUE_NAME)
    }

@app.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    data = redis_client.hgetall(f"task:{task_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "id": data.get("id"),
        "type": data.get("type"),
        "status": data.get("status"),
        "retries_remaining": data.get("retries_remaining"),
        "last_error": data.get("last_error", None),
        "created_at": data.get("created_at"),
        "max_retries": data.get("max_retries")
    }

@app.get("/dead-letter")
def get_dead_letters():
    dead_letters = []
    queue_length = redis_client.llen(DEAD_LETTER_QUEUE)
    
    for i in range(queue_length):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letters.append(json.loads(item))
    
    return {
        "dead_letter_count": queue_length,
        "tasks": dead_letters
    }

@app.post("/dead-letter/retry/{task_id}")
def retry_dead_letter(task_id: str):
    queue_length = redis_client.llen(DEAD_LETTER_QUEUE)
    
    for i in range(queue_length):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letter = json.loads(item)
            if dead_letter.get("task_id") == task_id:
                redis_client.lrem(DEAD_LETTER_QUEUE, 1, item)
                
                new_task = Task(
                    id=task_id,
                    type=dead_letter["type"],
                    payload=dead_letter["payload"],
                    max_retries=dead_letter["max_retries"],
                    retries_remaining=dead_letter["max_retries"],
                    status=TaskStatus.PENDING,
                    created_at=datetime.utcnow()
                )
                
                redis_client.rpush(QUEUE_NAME, new_task.to_json())
                redis_client.hset(f"task:{task_id}", "status", "pending")
                redis_client.hset(f"task:{task_id}", "retries_remaining", new_task.retries_remaining)
                redis_client.hset(f"task:{task_id}", "last_error", "")
                
                tasks_created.inc()
                asyncio.create_task(manager.broadcast_task_update(task_id, "retried", "Retried from dead letter"))
                
                return {"message": f"Task {task_id} re-queued successfully"}
    
    raise HTTPException(status_code=404, detail="Task not found")

@app.delete("/dead-letter/{task_id}")
def delete_dead_letter(task_id: str):
    queue_length = redis_client.llen(DEAD_LETTER_QUEUE)
    
    for i in range(queue_length):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letter = json.loads(item)
            if dead_letter.get("task_id") == task_id:
                redis_client.lrem(DEAD_LETTER_QUEUE, 1, item)
                return {"message": f"Task {task_id} deleted"}
    
    raise HTTPException(status_code=404, detail="Task not found")

@app.delete("/dead-letter/clear")
def clear_dead_letter_queue():
    queue_length = redis_client.llen(DEAD_LETTER_QUEUE)
    redis_client.delete(DEAD_LETTER_QUEUE)
    return {"message": f"Cleared {queue_length} tasks"}

@app.get("/metrics")
def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/queue/length")
def queue_length():
    return {"queue_length": redis_client.llen(QUEUE_NAME)}

@app.get("/stats")
def get_stats():
    stats_data = {
        "queue_size": redis_client.llen(QUEUE_NAME),
        "dead_letter_size": redis_client.llen(DEAD_LETTER_QUEUE),
        "total_tasks_created": tasks_created._value.get(),
        "total_tasks_processed": tasks_processed._value.get(),
        "total_tasks_failed": tasks_failed._value.get()
    }
    asyncio.create_task(manager.broadcast_stats_update(stats_data))
    return stats_data

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial data
        stats = {
            "queue_size": redis_client.llen(QUEUE_NAME),
            "dead_letter_size": redis_client.llen(DEAD_LETTER_QUEUE),
            "total_tasks_created": tasks_created._value.get(),
            "total_tasks_processed": tasks_processed._value.get(),
            "total_tasks_failed": tasks_failed._value.get()
        }
        await websocket.send_json({"type": "stats_update", "stats": stats})
        
        dead_letters = []
        for i in range(redis_client.llen(DEAD_LETTER_QUEUE)):
            item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
            if item:
                dead_letters.append(json.loads(item))
        await websocket.send_json({"type": "dead_letter_update", "dead_letters": dead_letters})
        
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.post("/internal/notify")
async def internal_notify(notification: Notification):
    if notification.type == "task_completed":
        tasks_processed.inc()
        await manager.broadcast_task_update(notification.data.get("task_id"), "completed", "")
    elif notification.type == "task_failed":
        tasks_failed.inc()
        await manager.broadcast_task_update(notification.data.get("task_id"), "failed", notification.data.get("error", ""))
    
    stats = {
        "queue_size": redis_client.llen(QUEUE_NAME),
        "dead_letter_size": redis_client.llen(DEAD_LETTER_QUEUE),
        "total_tasks_created": tasks_created._value.get(),
        "total_tasks_processed": tasks_processed._value.get(),
        "total_tasks_failed": tasks_failed._value.get()
    }
    await manager.broadcast_stats_update(stats)
    
    dead_letters = []
    for i in range(redis_client.llen(DEAD_LETTER_QUEUE)):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letters.append(json.loads(item))
    await manager.broadcast_dead_letter_update(dead_letters)
    
    return {"status": "ok"}

@app.get("/")
def root():
    return {
        "message": "Task Queue System - Production Ready",
        "version": "3.0.0",
        "endpoints": {
            "dashboard": "/dashboard",
            "submit_task": "POST /tasks",
            "task_status": "GET /tasks/{task_id}",
            "dead_letter": "GET /dead-letter",
            "metrics": "GET /metrics",
            "stats": "GET /stats",
            "websocket": "WS /ws",
            "docs": "/docs"
        }
    }

PORT = int(os.getenv('PORT', 8000))
if __name__ == "__main__":
    try:
        import uvicorn
        PORT = int(os.getenv('PORT', 8000))
        print(f"Starting server on port {PORT}...")
        uvicorn.run(app, host="0.0.0.0", port=PORT)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)

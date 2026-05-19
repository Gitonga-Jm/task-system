cd ~/task-queue-system
cat > main.py << 'EOF'
import os
import sys
import json
import uuid
import asyncio
from datetime import datetime
from typing import Optional
import ssl

print("Starting Task Queue System...")

try:
    from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, Response
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
    from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
    import redis
    print("✓ Core imports OK")
except Exception as e:
    print(f"✗ Import error: {e}")
    sys.exit(1)

try:
    from models import Task, TaskStatus
    print("✓ Models import OK")
except Exception as e:
    print(f"✗ Models error: {e}")
    sys.exit(1)

app = FastAPI(title="Task Queue System")

# ============================================================
# DIRECT HARDCODED REDIS CONNECTION - NO ENV VARIABLES
# ============================================================
REDIS_HOST = "whole-possum-41731.upstash.io"
REDIS_PORT = 6379
REDIS_PASSWORD = "AaMDAAIgcDFhOTJlZTA4NGM4NTY0MmE5ODVlNTFjMmY2MTM2YzExNQ"

print(f"Connecting to Redis: {REDIS_HOST}:{REDIS_PORT}")

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True,
        ssl_cert_reqs=ssl.CERT_NONE,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10
    )
    redis_client.ping()
    print("✓ Redis connected successfully!")
except Exception as e:
    print(f"✗ Redis connection failed: {e}")
    redis_client = None

QUEUE_NAME = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"

tasks_created = Counter('tasks_created_total', 'Total tasks created')
tasks_processed = Counter('tasks_processed_total', 'Total tasks processed')
tasks_failed = Counter('tasks_failed_total', 'Total tasks failed')

static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
    print("✓ Static files mounted")

class TaskCreate(BaseModel):
    type: str
    payload: dict
    max_retries: int = 3

@app.get("/")
def root():
    return {"message": "Task Queue System", "status": "running"}

@app.get("/health")
def health():
    if redis_client:
        try:
            redis_client.ping()
            return {"status": "healthy", "redis": "connected"}
        except:
            return {"status": "healthy", "redis": "disconnected"}
    return {"status": "healthy", "redis": "not_configured"}

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    dashboard_path = os.path.join(static_dir, "dashboard.html")
    if os.path.exists(dashboard_path):
        with open(dashboard_path, "r") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard</h1><p>Static file not found</p>")

@app.post("/tasks", status_code=202)
def create_task(task_req: TaskCreate):
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    
    task_id = str(uuid.uuid4())
    
    task = Task(
        id=task_id,
        type=task_req.type,
        payload=task_req.payload,
        max_retries=task_req.max_retries,
        retries_remaining=task_req.max_retries,
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow()
    )
    
    redis_client.hset(f"task:{task_id}", "id", task_id)
    redis_client.hset(f"task:{task_id}", "type", task.type)
    redis_client.hset(f"task:{task_id}", "status", task.status.value)
    redis_client.hset(f"task:{task_id}", "retries_remaining", task.retries_remaining)
    redis_client.hset(f"task:{task_id}", "created_at", task.created_at.isoformat())
    redis_client.hset(f"task:{task_id}", "max_retries", task.max_retries)
    
    redis_client.rpush(QUEUE_NAME, task.to_json())
    tasks_created.inc()
    
    return {
        "task_id": task_id,
        "status": "accepted",
        "queue_position": redis_client.llen(QUEUE_NAME)
    }

@app.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    
    data = redis_client.hgetall(f"task:{task_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "id": data.get("id"),
        "status": data.get("status"),
        "retries_remaining": data.get("retries_remaining"),
        "last_error": data.get("last_error", None),
        "created_at": data.get("created_at")
    }

@app.get("/dead-letter")
def get_dead_letters():
    if redis_client is None:
        return {"dead_letter_count": 0, "tasks": []}
    
    dead_letters = []
    for i in range(redis_client.llen(DEAD_LETTER_QUEUE)):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letters.append(json.loads(item))
    return {"dead_letter_count": len(dead_letters), "tasks": dead_letters}

@app.post("/dead-letter/retry/{task_id}")
def retry_dead_letter(task_id: str):
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    
    for i in range(redis_client.llen(DEAD_LETTER_QUEUE)):
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
                tasks_created.inc()
                return {"message": f"Task {task_id} re-queued"}
    
    raise HTTPException(status_code=404, detail="Task not found")

@app.delete("/dead-letter/{task_id}")
def delete_dead_letter(task_id: str):
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    
    for i in range(redis_client.llen(DEAD_LETTER_QUEUE)):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letter = json.loads(item)
            if dead_letter.get("task_id") == task_id:
                redis_client.lrem(DEAD_LETTER_QUEUE, 1, item)
                return {"message": f"Task {task_id} deleted"}
    
    raise HTTPException(status_code=404, detail="Task not found")

@app.delete("/dead-letter/clear")
def clear_dead_letter_queue():
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    
    count = redis_client.llen(DEAD_LETTER_QUEUE)
    redis_client.delete(DEAD_LETTER_QUEUE)
    return {"message": f"Cleared {count} tasks"}

@app.get("/metrics")
def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/queue/length")
def queue_length():
    if redis_client is None:
        return {"queue_length": 0}
    return {"queue_length": redis_client.llen(QUEUE_NAME)}

@app.get("/stats")
def get_stats():
    if redis_client is None:
        return {
            "queue_size": 0,
            "dead_letter_size": 0,
            "total_tasks_created": tasks_created._value.get(),
            "total_tasks_processed": tasks_processed._value.get(),
            "total_tasks_failed": tasks_failed._value.get()
        }
    
    return {
        "queue_size": redis_client.llen(QUEUE_NAME),
        "dead_letter_size": redis_client.llen(DEAD_LETTER_QUEUE),
        "total_tasks_created": tasks_created._value.get(),
        "total_tasks_processed": tasks_processed._value.get(),
        "total_tasks_failed": tasks_failed._value.get()
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('PORT', 8000))
    print(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
EOF

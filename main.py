import os
import sys
import json
import uuid
import asyncio
from datetime import datetime
from typing import Optional

print(f"Python version: {sys.version}")
print(f"Current directory: {os.getcwd()}")

try:
    from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, Response
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
    from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
    import redis
    print("Core imports OK")
except Exception as e:
    print(f"Import error: {e}")
    sys.exit(1)

try:
    from models import Task, TaskStatus
    print("Models import OK")
except Exception as e:
    print(f"Models error: {e}")
    sys.exit(1)

app = FastAPI(title="Task Queue System")

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_TLS = os.getenv('REDIS_TLS', 'False').lower() == 'true'

try:
    if REDIS_PASSWORD:
        if REDIS_TLS:
            redis_client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD,
                ssl=True, ssl_cert_reqs=None, decode_responses=True
            )
        else:
            redis_client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True
            )
    else:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
    print("Redis connected")
except Exception as e:
    print(f"Redis error: {e}")
    redis_client = None

QUEUE_NAME = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"

tasks_created = Counter('tasks_created_total', 'Total tasks created')
tasks_processed = Counter('tasks_processed_total', 'Total tasks processed')
tasks_failed = Counter('tasks_failed_total', 'Total tasks failed')

static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

class TaskCreate(BaseModel):
    type: str
    payload: dict
    max_retries: int = 3

@app.get("/")
def root():
    return {"message": "Task Queue System", "status": "running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    dashboard_path = os.path.join(static_dir, "dashboard.html")
    if os.path.exists(dashboard_path):
        with open(dashboard_path, "r") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard</h1><p>Create static/dashboard.html</p>")

@app.post("/tasks", status_code=202)
def create_task(task_req: TaskCreate):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    task_id = str(uuid.uuid4())
    task = Task(
        id=task_id, type=task_req.type, payload=task_req.payload,
        max_retries=task_req.max_retries, retries_remaining=task_req.max_retries,
        status=TaskStatus.PENDING, created_at=datetime.utcnow()
    )
    redis_client.hset(f"task:{task_id}", "id", task_id)
    redis_client.hset(f"task:{task_id}", "type", task.type)
    redis_client.hset(f"task:{task_id}", "status", task.status.value)
    redis_client.rpush(QUEUE_NAME, task.to_json())
    tasks_created.inc()
    return {"task_id": task_id, "status": "accepted"}

@app.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    data = redis_client.hgetall(f"task:{task_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"id": data.get("id"), "status": data.get("status")}

@app.get("/dead-letter")
def get_dead_letters():
    if not redis_client:
        return {"dead_letter_count": 0, "tasks": []}
    dead_letters = []
    for i in range(redis_client.llen(DEAD_LETTER_QUEUE)):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letters.append(json.loads(item))
    return {"dead_letter_count": len(dead_letters), "tasks": dead_letters}

@app.post("/dead-letter/retry/{task_id}")
def retry_dead_letter(task_id: str):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    for i in range(redis_client.llen(DEAD_LETTER_QUEUE)):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letter = json.loads(item)
            if dead_letter.get("task_id") == task_id:
                redis_client.lrem(DEAD_LETTER_QUEUE, 1, item)
                new_task = Task(
                    id=task_id, type=dead_letter["type"], payload=dead_letter["payload"],
                    max_retries=dead_letter["max_retries"], retries_remaining=dead_letter["max_retries"],
                    status=TaskStatus.PENDING, created_at=datetime.utcnow()
                )
                redis_client.rpush(QUEUE_NAME, new_task.to_json())
                tasks_created.inc()
                return {"message": f"Task {task_id} re-queued"}
    raise HTTPException(status_code=404, detail="Task not found")

@app.delete("/dead-letter/{task_id}")
def delete_dead_letter(task_id: str):
    if not redis_client:
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
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    redis_client.delete(DEAD_LETTER_QUEUE)
    return {"message": "Dead letter queue cleared"}

@app.get("/metrics")
def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/queue/length")
def queue_length():
    if not redis_client:
        return {"queue_length": 0}
    return {"queue_length": redis_client.llen(QUEUE_NAME)}

@app.get("/stats")
def get_stats():
    if not redis_client:
        return {"queue_size": 0, "dead_letter_size": 0, "total_tasks_created": 0, "total_tasks_processed": 0, "total_tasks_failed": 0}
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

PORT = int(os.getenv('PORT', 8000))
if __name__ == "__main__":
    import uvicorn
    print(f"Starting server on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)

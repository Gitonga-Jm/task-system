from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import redis
import uuid
import json
import os
from models import Task, TaskStatus
from datetime import datetime
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI(title="Task Queue System")

# Redis connection
import os
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# Queue names
QUEUE_NAME = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"

# Prometheus metrics
tasks_created = Counter('tasks_created_total', 'Total tasks created')
tasks_processed = Counter('tasks_processed_total', 'Total tasks processed')
tasks_failed = Counter('tasks_failed_total', 'Total tasks failed')

# Serve static files (dashboard)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

class TaskCreate(BaseModel):
    type: str
    payload: dict
    max_retries: int = 3

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    dashboard_path = os.path.join(static_dir, "dashboard.html")
    if os.path.exists(dashboard_path):
        with open(dashboard_path, "r") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard not found. Run: mkdir static && create dashboard.html</h1>")

@app.post("/tasks", status_code=202)
def create_task(task_req: TaskCreate):
    task_id = str(uuid.uuid4())
    
    print(f"[API] Received task type: {task_req.type}")
    print(f"[API] Received payload: {task_req.payload}")
    print(f"[API] Max retries: {task_req.max_retries}")
    
    task = Task(
        id=task_id,
        type=task_req.type,
        payload=task_req.payload,
        max_retries=task_req.max_retries,
        retries_remaining=task_req.max_retries,
        status=TaskStatus.PENDING,
        created_at=datetime.utcnow()
    )
    
    print(f"[API] Task created with payload: {task.payload}")
    
    # Store task metadata in Redis hash
    redis_client.hset(f"task:{task_id}", "id", task_id)
    redis_client.hset(f"task:{task_id}", "type", task.type)
    redis_client.hset(f"task:{task_id}", "status", task.status.value)
    redis_client.hset(f"task:{task_id}", "retries_remaining", task.retries_remaining)
    redis_client.hset(f"task:{task_id}", "created_at", task.created_at.isoformat())
    redis_client.hset(f"task:{task_id}", "max_retries", task.max_retries)
    
    # Convert task to JSON and push to queue
    task_json = task.to_json()
    print(f"[API] JSON being queued: {task_json}")
    
    redis_client.rpush(QUEUE_NAME, task_json)
    tasks_created.inc()
    
    queue_position = redis_client.llen(QUEUE_NAME)
    print(f"[API] Task {task_id} queued at position {queue_position}")
    
    return {
        "task_id": task_id,
        "status": "accepted",
        "queue_position": queue_position
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
                
                return {"message": f"Task {task_id} re-queued successfully"}
    
    raise HTTPException(status_code=404, detail="Task not found in dead letter queue")

@app.delete("/dead-letter/{task_id}")
def delete_dead_letter(task_id: str):
    queue_length = redis_client.llen(DEAD_LETTER_QUEUE)
    
    for i in range(queue_length):
        item = redis_client.lindex(DEAD_LETTER_QUEUE, i)
        if item:
            dead_letter = json.loads(item)
            if dead_letter.get("task_id") == task_id:
                redis_client.lrem(DEAD_LETTER_QUEUE, 1, item)
                return {"message": f"Task {task_id} deleted from dead letter queue"}
    
    raise HTTPException(status_code=404, detail="Task not found in dead letter queue")

@app.delete("/dead-letter/clear")
def clear_dead_letter_queue():
    queue_length = redis_client.llen(DEAD_LETTER_QUEUE)
    redis_client.delete(DEAD_LETTER_QUEUE)
    return {"message": f"Cleared {queue_length} tasks from dead letter queue"}

@app.get("/metrics")
def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/queue/length")
def queue_length():
    return {"queue_length": redis_client.llen(QUEUE_NAME)}

@app.get("/stats")
def get_stats():
    return {
        "queue_size": redis_client.llen(QUEUE_NAME),
        "dead_letter_size": redis_client.llen(DEAD_LETTER_QUEUE),
        "total_tasks_created": tasks_created._value.get(),
        "total_tasks_processed": tasks_processed._value.get(),
        "total_tasks_failed": tasks_failed._value.get()
    }

@app.get("/")
def root():
    return {
        "message": "Task Queue System - Production Ready",
        "version": "2.0.0",
        "endpoints": {
            "submit_task": "POST /tasks",
            "task_status": "GET /tasks/{task_id}",
            "queue_length": "GET /queue/length",
            "dead_letter": "GET /dead-letter",
            "retry_failed": "POST /dead-letter/retry/{task_id}",
            "delete_failed": "DELETE /dead-letter/{task_id}",
            "clear_dead_letter": "DELETE /dead-letter/clear",
            "metrics": "GET /metrics",
            "stats": "GET /stats",
            "dashboard": "GET /dashboard",
            "api_docs": "/docs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

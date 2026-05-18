cd ~/task-queue-system
cat > worker.py << 'EOF'
import redis
import json
import time
import signal
import os
import requests
import threading
from models import Task, TaskStatus
from PIL import Image

# WebSocket notification via API
def notify_websocket(event_type: str, data: dict):
    try:
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        api_url = f"http://{redis_host if redis_host != 'localhost' else 'localhost'}:8000/internal/notify"
        threading.Thread(target=lambda: requests.post(api_url, json={"type": event_type, "data": data}, timeout=2), daemon=True).start()
    except Exception as e:
        print(f"[Worker] Failed to send notification: {e}")

# Redis connection with Render.com support
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

if REDIS_PASSWORD:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
else:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

QUEUE_NAME = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"
RUNNING = True

def signal_handler(sig, frame):
    global RUNNING
    print("\n[WORKER] Shutting down gracefully...")
    RUNNING = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def process_task(task: Task):
    print(f"[WORKER] Processing task {task.id} (type: {task.type})")
    print(f"[WORKER] Full payload: {task.payload}")
    
    if task.type == "fail_me":
        raise Exception("Task was instructed to fail")
    
    elif task.type == "email":
        to_email = task.payload.get('to')
        subject = task.payload.get('subject', 'No subject')
        body = task.payload.get('body', '')
        print(f"[WORKER] Sending email to: {to_email}")
        print(f"[WORKER] Subject: {subject}")
        time.sleep(1)
        print(f"[WORKER] Email sent successfully to {to_email}")
        notify_websocket("task_completed", {"task_id": task.id, "type": "email", "to": to_email})
    
    elif task.type == "resize_image":
        input_path = task.payload.get('input_path')
        output_path = task.payload.get('output_path')
        width = task.payload.get('width', 800)
        height = task.payload.get('height', 600)
        
        print(f"[WORKER] Input path: {input_path}")
        print(f"[WORKER] Output path: {output_path}")
        
        if not input_path:
            raise Exception("Missing input_path in payload")
        
        if not os.path.exists(input_path):
            raise Exception(f"Input image not found: {input_path}")
        
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        with Image.open(input_path) as img:
            original_size = img.size
            print(f"[WORKER] Original size: {original_size[0]}x{original_size[1]}")
            resized = img.resize((width, height), Image.Resampling.LANCZOS)
            resized.save(output_path)
        
        print(f"[WORKER] Image resized and saved to {output_path}")
        notify_websocket("task_completed", {"task_id": task.id, "type": "image", "output": output_path})
    
    else:
        print(f"[WORKER] Unknown task type: {task.type}")

def main():
    print(f"[WORKER] Started. Listening on queue: {QUEUE_NAME}")
    print(f"[WORKER] Dead letter queue: {DEAD_LETTER_QUEUE}")
    print(f"[WORKER] Redis host: {REDIS_HOST}")
    
    while RUNNING:
        try:
            result = redis_client.brpop(QUEUE_NAME, timeout=1)
            
            if result is None:
                continue
            
            _, task_json = result
            task = Task.from_json(task_json)
            print(f"[WORKER] Got task {task.id}, retries left: {task.retries_remaining}")
            
            task.status = TaskStatus.PROCESSING
            redis_client.hset(f"task:{task.id}", "status", task.status.value)
            
            try:
                process_task(task)
                task.status = TaskStatus.COMPLETED
                print(f"[WORKER] Task {task.id} completed successfully")
                redis_client.hset(f"task:{task.id}", "status", task.status.value)
                
            except Exception as e:
                task.retries_remaining -= 1
                task.last_error = str(e)
                
                if task.retries_remaining > 0:
                    task.status = TaskStatus.PENDING
                    print(f"[WORKER] Task {task.id} failed. Retries left: {task.retries_remaining}. Re-queueing.")
                    redis_client.lpush(QUEUE_NAME, task.to_json())
                    redis_client.hset(f"task:{task.id}", "status", "pending")
                    redis_client.hset(f"task:{task.id}", "retries_remaining", task.retries_remaining)
                    redis_client.hset(f"task:{task.id}", "last_error", str(e))
                else:
                    task.status = TaskStatus.FAILED
                    print(f"[WORKER] Task {task.id} failed permanently. Sending to dead letter queue.")
                    
                    dead_letter_entry = {
                        "task_id": task.id,
                        "type": task.type,
                        "payload": task.payload,
                        "last_error": str(e),
                        "failed_at": time.time(),
                        "max_retries": task.max_retries
                    }
                    redis_client.lpush(DEAD_LETTER_QUEUE, json.dumps(dead_letter_entry))
                    redis_client.hset(f"task:{task.id}", "status", "failed")
                    redis_client.hset(f"task:{task.id}", "last_error", str(e))
                    notify_websocket("task_failed", {"task_id": task.id, "error": str(e), "type": task.type})
                    
        except Exception as outer_e:
            print(f"[WORKER] Unexpected error: {outer_e}")
            time.sleep(1)
    
    print("[WORKER] Exited")

if __name__ == "__main__":
    main()
EOF

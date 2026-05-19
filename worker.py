cd ~/task-queue-system
cat > worker.py << 'EOF'
import redis
import json
import time
import signal
import os
from models import Task, TaskStatus
from PIL import Image

print("Starting worker on Render...")

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

print(f"Worker connecting to Redis: {REDIS_HOST}:{REDIS_PORT}")

try:
    if REDIS_PASSWORD:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10
        )
    else:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10
        )
    redis_client.ping()
    print("✓ Worker Redis connected successfully")
except Exception as e:
    print(f"✗ Worker Redis connection failed: {e}")
    redis_client = None

QUEUE_NAME = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"
RUNNING = True

def signal_handler(sig, frame):
    global RUNNING
    print("\n[WORKER] Shutting down...")
    RUNNING = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def process_task(task: Task):
    print(f"[WORKER] Processing {task.id} ({task.type})")
    
    if task.type == "fail_me":
        raise Exception("Task was instructed to fail")
    
    elif task.type == "email":
        to_email = task.payload.get('to')
        subject = task.payload.get('subject', 'No subject')
        print(f"[WORKER] Sending email to {to_email}: {subject}")
        time.sleep(1)
        print(f"[WORKER] Email sent")
    
    elif task.type == "resize_image":
        input_path = task.payload.get('input_path')
        output_path = task.payload.get('output_path')
        width = task.payload.get('width', 800)
        height = task.payload.get('height', 600)
        
        if not input_path:
            raise Exception("Missing input_path")
        
        if not os.path.exists(input_path):
            os.makedirs(os.path.dirname(input_path), exist_ok=True)
            from PIL import Image, ImageDraw
            img = Image.new('RGB', (1920, 1080), color='blue')
            draw = ImageDraw.Draw(img)
            draw.text((700, 520), "TEST", fill='white')
            img.save(input_path)
        
        with Image.open(input_path) as img:
            resized = img.resize((width, height), Image.Resampling.LANCZOS)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            resized.save(output_path)
        print(f"[WORKER] Image resized to {width}x{height}")

def main():
    if redis_client is None:
        print("[WORKER] FATAL: Cannot connect to Redis")
        return
    
    print(f"[WORKER] Listening on queue: {QUEUE_NAME}")
    
    while RUNNING:
        try:
            result = redis_client.brpop(QUEUE_NAME, timeout=1)
            if result is None:
                continue
            
            _, task_json = result
            task = Task.from_json(task_json)
            print(f"[WORKER] Got task {task.id}, retries left: {task.retries_remaining}")
            
            redis_client.hset(f"task:{task.id}", "status", "processing")
            
            try:
                process_task(task)
                redis_client.hset(f"task:{task.id}", "status", "completed")
                print(f"[WORKER] Task {task.id} completed")
                
            except Exception as e:
                task.retries_remaining -= 1
                task.last_error = str(e)
                
                if task.retries_remaining > 0:
                    print(f"[WORKER] Task failed, {task.retries_remaining} retries left")
                    redis_client.lpush(QUEUE_NAME, task.to_json())
                    redis_client.hset(f"task:{task.id}", "status", "pending")
                    redis_client.hset(f"task:{task.id}", "retries_remaining", task.retries_remaining)
                    redis_client.hset(f"task:{task.id}", "last_error", str(e))
                else:
                    print(f"[WORKER] Task failed permanently, moving to DLQ")
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
                    
        except Exception as e:
            print(f"[WORKER] Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()
EOF

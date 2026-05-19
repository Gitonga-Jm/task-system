import redis
import json
import time
import signal
import os
import sys
from datetime import datetime
from typing import Optional
import ssl

print("Starting Task Queue Worker...")

# ============================================================
# REDIS CONNECTION
# ============================================================
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_TLS = os.getenv('REDIS_TLS', 'False').lower() == 'true'

print(f"Connecting to Redis: {REDIS_HOST}:{REDIS_PORT}")

def get_redis_client():
    try:
        if REDIS_PASSWORD:
            if REDIS_TLS:
                client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    password=REDIS_PASSWORD,
                    ssl=True,
                    ssl_cert_reqs=ssl.CERT_NONE,
                    decode_responses=True,
                    socket_timeout=10,
                    socket_connect_timeout=10
                )
            else:
                client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    password=REDIS_PASSWORD,
                    decode_responses=True,
                    socket_timeout=10,
                    socket_connect_timeout=10
                )
        else:
            client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_timeout=10,
                socket_connect_timeout=10
            )
        client.ping()
        print("✓ Worker Redis connected successfully")
        return client
    except Exception as e:
        print(f"✗ Worker Redis connection failed: {e}")
        return None

redis_client = get_redis_client()

if not redis_client:
    print("FATAL: Cannot connect to Redis. Exiting.")
    sys.exit(1)

# ============================================================
# QUEUE CONFIGURATION
# ============================================================
QUEUE_NAME = "task_queue"
DEAD_LETTER_QUEUE = "dead_letter_queue"
RUNNING = True

# Worker ID for this instance
WORKER_ID = os.getenv('WORKER_ID', f"worker_{os.getpid()}")

# ============================================================
# SIGNAL HANDLING
# ============================================================
def signal_handler(sig, frame):
    global RUNNING
    print(f"\n[WORKER {WORKER_ID}] Shutting down gracefully...")
    RUNNING = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================
# TASK PROCESSING
# ============================================================
def process_task(task_data: dict):
    """Process a task based on its type"""
    task_id = task_data.get('id')
    task_type = task_data.get('type')
    payload = task_data.get('payload', {})
    
    print(f"[WORKER {WORKER_ID}] Processing task {task_id} (type: {task_type})")
    
    if task_type == "fail_me":
        raise Exception("Task was instructed to fail")
    
    elif task_type == "email":
        to_email = payload.get('to')
        subject = payload.get('subject', 'No subject')
        body = payload.get('body', '')
        print(f"[WORKER {WORKER_ID}] Sending email to: {to_email}")
        print(f"[WORKER {WORKER_ID}] Subject: {subject}")
        if body:
            print(f"[WORKER {WORKER_ID}] Body: {body[:100]}...")
        time.sleep(2)  # Simulate email sending
        print(f"[WORKER {WORKER_ID}] Email sent successfully")
    
    elif task_type == "resize_image":
        input_path = payload.get('input_path')
        output_path = payload.get('output_path')
        width = payload.get('width', 800)
        height = payload.get('height', 600)
        
        print(f"[WORKER {WORKER_ID}] Resizing image: {input_path}")
        print(f"[WORKER {WORKER_ID}] Output: {output_path} ({width}x{height})")
        
        if not input_path:
            raise Exception("Missing input_path in payload")
        
        try:
            from PIL import Image
            import os.path
            
            if not os.path.exists(input_path):
                # Create a test image if it doesn't exist
                os.makedirs(os.path.dirname(input_path), exist_ok=True)
                img = Image.new('RGB', (1920, 1080), color='blue')
                from PIL import ImageDraw
                draw = ImageDraw.Draw(img)
                draw.text((700, 520), "TEST IMAGE", fill='white')
                img.save(input_path)
                print(f"[WORKER {WORKER_ID}] Created test image at {input_path}")
            
            with Image.open(input_path) as img:
                original_size = img.size
                print(f"[WORKER {WORKER_ID}] Original size: {original_size[0]}x{original_size[1]}")
                resized = img.resize((width, height), Image.Resampling.LANCZOS)
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                resized.save(output_path)
            
            print(f"[WORKER {WORKER_ID}] Image resized successfully")
        except ImportError:
            print(f"[WORKER {WORKER_ID}] PIL not installed, simulating image resize")
            time.sleep(1)
    
    else:
        print(f"[WORKER {WORKER_ID}] Unknown task type: {task_type}")
        raise Exception(f"Unknown task type: {task_type}")

def update_task_status(task_id: str, status: str, error: Optional[str] = None):
    """Update task status in Redis"""
    try:
        redis_client.hset(f"task:{task_id}", "status", status)
        if error:
            redis_client.hset(f"task:{task_id}", "last_error", error)
        if status == "completed":
            redis_client.hset(f"task:{task_id}", "completed_at", datetime.now().isoformat())
    except Exception as e:
        print(f"[WORKER {WORKER_ID}] Failed to update task status: {e}")

# ============================================================
# MAIN WORKER LOOP
# ============================================================
def main():
    print(f"[WORKER {WORKER_ID}] Started. Listening on queue: {QUEUE_NAME}")
    print(f"[WORKER {WORKER_ID}] Dead letter queue: {DEAD_LETTER_QUEUE}")
    
    tasks_processed = 0
    tasks_failed = 0
    
    while RUNNING:
        try:
            # Blocking pop from Redis queue (waits for tasks)
            result = redis_client.brpop(QUEUE_NAME, timeout=5)
            
            if result is None:
                # No task received, continue waiting
                continue
            
            queue_name, task_json = result
            task_data = json.loads(task_json)
            task_id = task_data.get('id')
            
            print(f"[WORKER {WORKER_ID}] Got task {task_id}, retries left: {task_data.get('retries_remaining', 0)}")
            
            # Update status to processing
            update_task_status(task_id, "processing")
            
            try:
                # Process the task
                process_task(task_data)
                
                # Mark as completed
                update_task_status(task_id, "completed")
                tasks_processed += 1
                print(f"[WORKER {WORKER_ID}] Task {task_id} completed successfully")
                
            except Exception as e:
                error_msg = str(e)
                print(f"[WORKER {WORKER_ID}] Task {task_id} failed: {error_msg}")
                
                # Handle retries
                retries_remaining = task_data.get('retries_remaining', 0) - 1
                task_data['retries_remaining'] = retries_remaining
                task_data['last_error'] = error_msg
                
                if retries_remaining > 0:
                    # Re-queue the task
                    redis_client.lpush(QUEUE_NAME, json.dumps(task_data))
                    update_task_status(task_id, "pending", error_msg)
                    redis_client.hset(f"task:{task_id}", "retries_remaining", retries_remaining)
                    print(f"[WORKER {WORKER_ID}] Task {task_id} re-queued, {retries_remaining} retries left")
                else:
                    # Move to dead letter queue
                    tasks_failed += 1
                    dead_letter_entry = {
                        "task_id": task_id,
                        "type": task_data.get('type'),
                        "payload": task_data.get('payload'),
                        "last_error": error_msg,
                        "failed_at": datetime.now().isoformat(),
                        "max_retries": task_data.get('max_retries', 3),
                        "worker_id": WORKER_ID
                    }
                    redis_client.lpush(DEAD_LETTER_QUEUE, json.dumps(dead_letter_entry))
                    update_task_status(task_id, "failed", error_msg)
                    print(f"[WORKER {WORKER_ID}] Task {task_id} moved to dead letter queue")
            
            # Update worker stats in Redis
            redis_client.hset("worker_stats", WORKER_ID, json.dumps({
                "tasks_processed": tasks_processed,
                "tasks_failed": tasks_failed,
                "last_active": datetime.now().isoformat(),
                "status": "running"
            }))
            redis_client.expire("worker_stats", 60)
            
        except redis.exceptions.ConnectionError as e:
            print(f"[WORKER {WORKER_ID}] Redis connection error: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"[WORKER {WORKER_ID}] Unexpected error: {e}")
            time.sleep(1)
    
    print(f"[WORKER {WORKER_ID}] Stopped. Processed: {tasks_processed}, Failed: {tasks_failed}")

if __name__ == "__main__":
    main()

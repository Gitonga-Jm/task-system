import redis
import json
import time
import signal
import os
from models import Task, TaskStatus
from PIL import Image

import os
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
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
        print(f"[WORKER] Body: {body[:100] if body else 'Empty'}")
        time.sleep(1)
        print(f"[WORKER] Email sent successfully to {to_email}")
    
    elif task.type == "resize_image":
        input_path = task.payload.get('input_path')
        output_path = task.payload.get('output_path')
        width = task.payload.get('width', 800)
        height = task.payload.get('height', 600)
        
        print(f"[WORKER] Input path: {input_path}")
        print(f"[WORKER] Output path: {output_path}")
        print(f"[WORKER] Target dimensions: {width}x{height}")
        
        if not input_path:
            raise Exception("Missing input_path in payload")
        
        if not os.path.exists(input_path):
            raise Exception(f"Input image not found: {input_path}")
        
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            print(f"[WORKER] Created output directory: {output_dir}")
        
        # Actually resize the image
        with Image.open(input_path) as img:
            original_size = img.size
            print(f"[WORKER] Original size: {original_size[0]}x{original_size[1]}")
            resized = img.resize((width, height), Image.Resampling.LANCZOS)
            resized.save(output_path)
        
        print(f"[WORKER] Image resized and saved to {output_path}")
        print(f"[WORKER] New size: {width}x{height}")
    
    else:
        print(f"[WORKER] Unknown task type: {task.type}")

def main():
    print(f"[WORKER] Started. Listening on queue: {QUEUE_NAME}")
    print(f"[WORKER] Dead letter queue: {DEAD_LETTER_QUEUE}")
    
    while RUNNING:
        try:
            result = redis_client.brpop(QUEUE_NAME, timeout=1)
            
            if result is None:
                continue
            
            _, task_json = result
            print(f"[WORKER] Raw JSON from queue: {task_json}")
            
            task = Task.from_json(task_json)
            print(f"[WORKER] Parsed task - ID: {task.id}, Type: {task.type}, Payload: {task.payload}")
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
                    print(f"[WORKER] Error: {str(e)}")
                    redis_client.lpush(QUEUE_NAME, task.to_json())
                    redis_client.hset(f"task:{task.id}", "status", "pending")
                    redis_client.hset(f"task:{task.id}", "retries_remaining", task.retries_remaining)
                    redis_client.hset(f"task:{task.id}", "last_error", str(e))
                else:
                    task.status = TaskStatus.FAILED
                    print(f"[WORKER] Task {task.id} failed permanently. Sending to dead letter queue.")
                    print(f"[WORKER] Final error: {str(e)}")
                    
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
                    
        except Exception as outer_e:
            print(f"[WORKER] Unexpected error: {outer_e}")
            time.sleep(1)
    
    print("[WORKER] Exited")

if __name__ == "__main__":
    main()

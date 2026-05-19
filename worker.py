cd ~/task-queue-system
cat > worker.py << 'EOF'
import redis
import json
import time
import signal
import os
import ssl
from models import Task, TaskStatus
from PIL import Image

print("Starting worker...")

# ============================================================
# PERMANENT REDIS CONNECTION - READS ENVIRONMENT VARIABLES
# ============================================================
def get_redis_config():
    """Get Redis configuration from environment variables with fallbacks"""
    return {
        'host': os.getenv('REDIS_HOST', 'whole-possum-41731.upstash.io'),
        'port': int(os.getenv('REDIS_PORT', 6379)),
        'password': os.getenv('REDIS_PASSWORD', 'AaMDAAIgcDFhOTJlZTA4NGM4NTY0MmE5ODVlNTFjMmY2MTM2YzExNQ'),
        'tls': os.getenv('REDIS_TLS', 'True').lower() == 'true'
    }

def create_redis_client():
    """Create Redis client with proper TLS configuration"""
    config = get_redis_config()
    
    print(f"Worker connecting to Redis: {config['host']}:{config['port']} (TLS: {config['tls']})")
    
    try:
        if config['tls']:
            client = redis.Redis(
                host=config['host'],
                port=config['port'],
                password=config['password'],
                ssl=True,
                ssl_cert_reqs=ssl.CERT_NONE,
                decode_responses=True,
                socket_timeout=10,
                socket_connect_timeout=10,
                retry_on_timeout=True,
                health_check_interval=30
            )
        else:
            client = redis.Redis(
                host=config['host'],
                port=config['port'],
                password=config['password'] if config['password'] else None,
                decode_responses=True,
                socket_timeout=10,
                socket_connect_timeout=10,
                retry_on_timeout=True,
                health_check_interval=30
            )
        
        client.ping()
        print("✓ Worker Redis connected successfully")
        return client
    except Exception as e:
        print(f"✗ Worker Redis connection failed: {e}")
        return None

redis_client = create_redis_client()

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
        body = task.payload.get('body', '')
        print(f"[WORKER] Sending email to {to_email}: {subject}")
        if body:
            print(f"[WORKER] Body: {body[:100]}")
        time.sleep(1)
        print(f"[WORKER] Email sent successfully")
    
    elif task.type == "resize_image":
        input_path = task.payload.get('input_path')
        output_path = task.payload.get('output_path')
        width = task.payload.get('width', 800)
        height = task.payload.get('height', 600)
        
        print(f"[WORKER] Resizing image: {input_path} -> {output_path} ({width}x{height})")
        
        if not input_path:
            raise Exception("Missing input_path in payload")
        
        if not os.path.exists(input_path):
            # Create directories and a test image
            os.makedirs(os.path.dirname(input_path), exist_ok=True)
            from PIL import Image, ImageDraw
            img = Image.new('RGB', (1920, 1080), color='blue')
            draw = ImageDraw.Draw(img)
            draw.text((700, 520), "TEST IMAGE", fill='white')
            img.save(input_path)
            print(f"[WORKER] Created test image at {input_path}")
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with Image.open(input_path) as img:
            original_size = img.size
            print(f"[WORKER] Original size: {original_size[0]}x{original_size[1]}")
            resized = img.resize((width, height), Image.Resampling.LANCZOS)
            resized.save(output_path)
        
        print(f"[WORKER] Image resized and saved to {output_path}")
    
    else:
        print(f"[WORKER] Unknown task type: {task.type}")

def main():
    if redis_client is None:
        print("[WORKER] FATAL: Cannot connect to Redis")
        return
    
    print(f"[WORKER] Listening on queue: {QUEUE_NAME}")
    print(f"[WORKER] Dead letter queue: {DEAD_LETTER_QUEUE}")
    
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
                print(f"[WORKER] Task {task.id} completed successfully")
                
            except Exception as e:
                task.retries_remaining -= 1
                task.last_error = str(e)
                
                if task.retries_remaining > 0:
                    print(f"[WORKER] Task {task.id} failed. Retries left: {task.retries_remaining}")
                    redis_client.lpush(QUEUE_NAME, task.to_json())
                    redis_client.hset(f"task:{task.id}", "status", "pending")
                    redis_client.hset(f"task:{task.id}", "retries_remaining", task.retries_remaining)
                    redis_client.hset(f"task:{task.id}", "last_error", str(e))
                else:
                    print(f"[WORKER] Task {task.id} failed permanently. Moving to Dead Letter Queue")
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
            print(f"[WORKER] Unexpected error: {e}")
            time.sleep(1)
    
    print("[WORKER] Exited")

if __name__ == "__main__":
    main()
EOF

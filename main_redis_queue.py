# Add this to your main.py - the queue pushing functionality

def push_task_to_queue(task_id: str, task_data: dict):
    """Push task to Redis queue for workers to process"""
    if REDIS_AVAILABLE and redis_client:
        task_json = json.dumps(task_data)
        redis_client.rpush(QUEUE_NAME, task_json)
        print(f"Task {task_id} pushed to Redis queue")
    else:
        # Fallback: store in memory queue
        if not hasattr(app, "memory_queue"):
            app.memory_queue = []
        app.memory_queue.append(task_data)
        print(f"Task {task_id} stored in memory queue (Redis unavailable)")

# In your create_task endpoint, after creating the task:
# push_task_to_queue(task_id, task.dict())
